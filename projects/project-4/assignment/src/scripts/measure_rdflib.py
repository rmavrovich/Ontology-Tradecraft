from rdflib import Graph, Literal, RDF, RDFS, OWL, XSD, Namespace, URIRef, BNode
from pathlib import Path
import pandas as pd
import re
from rdflib.namespace import XSD, RDFS, OWL

script_dir = Path(__file__).parent
root_dir = script_dir.parent

CSV_FILE = root_dir / 'data' / 'readings_normalized.csv'
OUT_FILE = root_dir / 'measure_cco.ttl'

NS_EX   = Namespace("http://example.org/measurement/")
NS_CCO  = Namespace("https://www.commoncoreontologies.org/CommonCoreOntologiesMerged/")
NS_OWL  = Namespace("http://www.w3.org/2002/07/owl#")
NS_OBO  = Namespace("http://purl.obolibrary.org/obo/")
NS_RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
NS_XSD = Namespace("http://www.w3.org/2001/XMLSchema#")
NS_EXPROP = Namespace("http://example.org/props#")
NS_EXC = Namespace("http://example.org/classes#")

graph = Graph()
graph.bind("ex", NS_EX)
graph.bind("cco", NS_CCO) 
graph.bind("owl", NS_OWL)
graph.bind("obo", NS_OBO) 
graph.bind("rdf", NS_RDF) 
graph.bind("rdfs", RDFS) 
graph.bind("xsd", NS_XSD) 
graph.bind("exc", NS_EXC)
graph.bind("exprop", NS_EXPROP)

onto_uri = URIRef("http://example.org/ontology")
graph.add((onto_uri, NS_RDF.type, NS_OWL.Ontology))
graph.add((onto_uri, RDFS.label, Literal("cco conformant measurements", lang="en")))

# ... [All your existing label additions, definitions, caches, etc. remain unchanged] ...
# (The long section you already have with labels, CLASS_DEFS, OBJECT_PROPERTY_DEFS, etc.)

# Your existing helper functions (_slug, canonicalize_kind, resolve_unit_and_value, etc.)
# are already defined above — we will reuse them

# Define the core IRIs used in generate_triples
IRI_SDC   = URIRef("http://purl.obolibrary.org/obo/BFO_0000020")
IRI_ART   = URIRef("https://www.commoncoreontologies.org/ont00000995")
IRI_MICE  = URIRef("https://www.commoncoreontologies.org/ont00001163")
IRI_MU    = URIRef("https://www.commoncoreontologies.org/ont00000120")
IRI_BEARER_OF     = URIRef("http://purl.obolibrary.org/obo/BFO_0000196")
IRI_IS_MEASURE_OF = URIRef("https://www.commoncoreontologies.org/ont00001966")
IRI_USES_MU       = URIRef("https://www.commoncoreontologies.org/ont00001863")
IRI_HAS_VALUE     = URIRef("https://www.commoncoreontologies.org/ont00001769")
IRI_HAS_TIMESTAMP = URIRef("https://www.commoncoreontologies.org/ont00001767")

# Caches shared across the script
reading_cache = {}
quality_class_cache = {}

# ------------------------------------------------------------------
# New helper: exact same URI generation logic as your earlier block
# ------------------------------------------------------------------
def generate_uris_for_row(row):
    artifact_id_raw = str(row['artifact_id']).strip()
    sdc_kind_raw = str(row['sdc_kind']).strip()
    unit_raw = str(row['unit_label']).strip()
    timestamp_raw = str(row['timestamp']).strip()
    value = float(row['value'])

    # Exact same slugging and canonicalization
    artifact_label = artifact_id_raw.replace(" ", "-")
    artifact_slug = _slug(artifact_label)
    canon_kind_label, canon_kind_slug = canonicalize_kind(sdc_kind_raw)
    ts_slug = _slug(timestamp_raw)

    # Artifact URI
    artifact_uri = URIRef(NS_EX + artifact_slug)

    # Reading (SDC) URI — with caching exactly as before
    reading_key = (artifact_slug, canon_kind_slug, ts_slug)
    if reading_key in reading_cache:
        reading_uri = reading_cache[reading_key]
    else:
        reading_id = f"{artifact_slug}_{canon_kind_slug}_{ts_slug}"
        reading_uri = URIRef(NS_EX + reading_id)
        reading_cache[reading_key] = reading_uri

    # Unit URI (re-use your resolve function for consistency)
    unit_uri, _, _ = resolve_unit_and_value(unit_raw, value)

    # Measurement Value URI — we create a dedicated node for the value (as in your intent)
    mv_uri = URIRef(NS_EX + f"mv_{artifact_slug}_{canon_kind_slug}_{ts_slug}")

    # MICE URI
    mice_id = f"MICE_{artifact_slug}_{canon_kind_slug}_{ts_slug}"
    mice_uri = URIRef(NS_EX + mice_id)

    return artifact_uri, reading_uri, unit_uri, mv_uri, mice_uri

# ------------------------------------------------------------------
# Refactored generate_triples — now iterative, no recursion
# ------------------------------------------------------------------
def generate_triples(df, graph):
    seen_static_entities = set()

    for _, row in df.iterrows():
        artifact_uri, sdc_uri, mu_uri, mv_uri, mice_uri = generate_uris_for_row(row)

        # Artifact and its bearer_of relation (deduplicated)
        artifact_key = str(artifact_uri)
        sdc_key = str(sdc_uri)
        if artifact_key not in seen_static_entities:
            graph.add((artifact_uri, RDF.type, IRI_ART))
            graph.add((artifact_uri, IRI_BEARER_OF, sdc_uri))
            graph.add((sdc_uri, RDF.type, IRI_SDC))
            seen_static_entities.add(artifact_key)
            seen_static_entities.add(sdc_key)

        # Measurement Unit (deduplicated)
        mu_key = str(mu_uri)
        if mu_key not in seen_static_entities:
            graph.add((mu_uri, RDF.type, IRI_MU))
            seen_static_entities.add(mu_key)

        # Measurement Value node (deduplicated)
        mv_key = str(mv_uri)
        if mv_key not in seen_static_entities:
            # Note: the literal value is attached directly to MICE via has_decimal_value in the earlier block,
            # but here we follow your original intent of a separate mv node
            graph.add((mv_uri, RDF.type, IRI_HAS_VALUE))
            graph.add((mv_uri, IRI_HAS_VALUE, Literal(row['value'], datatype=XSD.decimal)))
            seen_static_entities.add(mv_key)

        # MICE instance and its connections
        graph.add((mice_uri, RDF.type, IRI_MICE))
        graph.add((mice_uri, IRI_IS_MEASURE_OF, sdc_uri))
        graph.add((mice_uri, IRI_USES_MU, mu_uri))
        graph.add((mice_uri, IRI_HAS_VALUE, mv_uri))
        graph.add((mice_uri, IRI_HAS_TIMESTAMP, Literal(row['timestamp'], datatype=XSD.dateTime)))

    return graph

# ------------------------------------------------------------------
# main() — unchanged call
# ------------------------------------------------------------------
def main():
    if not CSV_FILE.exists():
        print(f"Error: CSV file not found at {CSV_FILE.resolve()}. Ensure the ETL step ran successfully.")
        with open(OUT_FILE, 'w') as f:
            f.write("@prefix ex: <http://example.org/measurement/> .\n")
        return

    print(f"Loading data from {CSV_FILE}")
    df = pd.read_csv(CSV_FILE, dtype=str, keep_default_na=False)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])

    print(f"Generating {len(df)} instance triples...")
    generate_triples(df, graph)

    print(f"Serializing graph with {len(graph)} total triples to {OUT_FILE}")
    graph.serialize(destination=OUT_FILE, format='turtle')

    if OUT_FILE.exists():
        print("✅ TTL file saved successfully.")
    else:
        print("❌ TTL file was not saved!")

if __name__ == '__main__':
    main()

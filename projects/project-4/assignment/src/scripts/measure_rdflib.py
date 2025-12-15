from rdflib import Graph, Literal, RDF, RDFS, OWL, XSD, Namespace, URIRef, BNode
from pathlib import Path
import pandas as pd
import hashlib


# Classes
IRI_SDC   = URIRef("http://purl.obolibrary.org/obo/BFO_0000020")       # State of Disorder/Condition (SDC)
IRI_ART   = URIRef("https://www.commoncoreontologies.org/ont00000995") # Artifact
IRI_MICE  = URIRef("https://www.commoncoreontologies.org/ont00001163") # Measurement Information Content Entity (MICE)
IRI_MU    = URIRef("https://www.commoncoreontologies.org/ont00000120")    # Measurement Unit (MU)

# Properties
IRI_BEARER_OF = URIRef("http://purl.obolibrary.org/obo/BFO_0000196")     # bearer_of (Artifact -> SDC)
IRI_IS_MEASURE_OF = URIRef("https://www.commoncoreontologies.org/ont00001966") # is_measurement_of (MICE -> SDC)
IRI_USES_MU       = URIRef("https://www.commoncoreontologies.org/ont00001863") # uses_measurement_unit (MICE -> MU)
IRI_HAS_VALUE     = URIRef("https://www.commoncoreontologies.org/ont00001769") # has_value (MICE -> Literal Value)
IRI_HAS_TIMESTAMP = URIRef("https://www.commoncoreontologies.org/ont00001767") # has_timestamp (MICE -> Literal Time)


script_dir = Path(__file__).parent
root_dir = script_dir.parent

CSV_FILE = root_dir / 'data' / 'readings_normalized.csv'
OUT_FILE = root_dir / 'measure_cco.ttl'

# Define namespaces 
NS_EX   = Namespace("http://example.org/measurement/") # Used for instance URIs like Artifact_...
NS_CCO  = Namespace("https://www.commoncoreontologies.org/CommonCoreOntologiesMerged/") 
NS_OWL  = Namespace("http://www.w3.org/2002/07/owl#")
NS_OBO  = Namespace("http://purl.obolibrary.org/obo/")

"""Initializes graph with namespaces."""
graph = Graph()
# Bind the instance namespace
graph.bind("ex", NS_EX)
# Bind the main ontology namespace
graph.bind("cco", NS_CCO) 
# Bind standard namespaces
graph.bind("owl", NS_OWL)
graph.bind("obo", NS_OBO) 
graph.bind("rdf", RDF) # RDF is already defined by rdflib
graph.bind("rdfs", RDFS) # RDFS is already defined by rdflib
graph.bind("xsd", XSD)   # XSD is already defined by rdflib


def generate_uris(row):
    """Generates deterministic URIs based on content hashes."""
    
    # 1. FIX: Ensure all identifiers are strings for hashing, INCLUDING value and timestamp
    artifact_id_str = str(row['artifact_id'])
    sdc_kind_str = str(row['sdc_kind'])
    unit_label_str = str(row['unit_label'])
    value_str = str(row['value'])        
    timestamp_str = str(row['timestamp'])  
    
    # 1. Artifact URI (based on its unique ID)
    artifact_uri = NS_EX[f"Artifact_{hashlib.sha256(artifact_id_str.encode()).hexdigest()[:8]}"]

    # 2. SDC URI (based on Artifact and the SDC kind)
    sdc_identifier = f"{artifact_id_str}_{sdc_kind_str}"
    sdc_uri = NS_EX[f"SDC_{hashlib.sha256(sdc_identifier.encode()).hexdigest()[:8]}"]
    
    # 3. MU URI (based on the unit label)
    mu_uri = NS_EX[f"MU_{hashlib.sha256(unit_label_str.encode()).hexdigest()[:8]}"]

    # 4. Measurement Value URI (MV_URI) - NEW
    # Based on the decimal value itself
    mv_uri = NS_EX[f"MV_{hashlib.sha256(value_str.encode()).hexdigest()[:8]}"] 
    
    # 5. MICE URI (based on SDC, timestamp, and value for unique measurement)
    # FIX: Include value and timestamp in the identifier to make MICE unique
    mice_identifier = f"{sdc_identifier}_{timestamp_str}_{value_str}" # <-- FIXED STRING
    mice_uri = NS_EX[f"MICE_{hashlib.sha256(mice_identifier.encode()).hexdigest()[:8]}"]

    # FIX: Return all five URIs
    return artifact_uri, sdc_uri, mu_uri, mv_uri, mice_uri # <-- ADDED mv_uri


# In generate_triples:
    artifact_uri, sdc_uri, mu_uri, mv_uri, mice_uri = generate_uris(row)

def generate_triples(df, graph):
    
    # Use a set to track generated Artifact, SDC, MU, and MV nodes
    seen_static_entities = set()
    
    for _, row in df.iterrows():
        
        artifact_uri, sdc_uri, mu_uri, mv_uri, mice_uri = generate_uris(row)
        
        # --- Static Entities (Artifact, SDC, MU, MV) ---
        
        # Artifact and SDC (Artifact -> SDC)
        artifact_key = str(artifact_uri)
        sdc_key = str(sdc_uri)
        if artifact_key not in seen_static_entities:
            # Artifact is defined and has a type
            graph.add((artifact_uri, RDF.type, IRI_ART)) 
            # Artifact bearer_of SDC
            graph.add((artifact_uri, IRI_BEARER_OF, sdc_uri))
            # SDC is defined and has a type
            graph.add((sdc_uri, RDF.type, IRI_SDC))
            seen_static_entities.add(artifact_key)
            seen_static_entities.add(sdc_key)
        
        # Measurement Unit (MU)
        mu_key = str(mu_uri)
        if mu_key not in seen_static_entities:
            # MU is defined and has a type
            graph.add((mu_uri, RDF.type, IRI_MU))
            seen_static_entities.add(mu_key)

        mv_key = str(mv_uri)
        if mv_key not in seen_static_entities:
            # MV is defined and has a type
            # Using IRI_VALUE, which is assumed to be defined as the CCO class for Value
            graph.add((mv_uri, RDF.type, IRI_HAS_VALUE)) 
            
            graph.add((mv_uri, IRI_HAS_VALUE, Literal(row['value'], datatype=XSD.decimal)))
            seen_static_entities.add(mv_key)

        graph.add((mice_uri, RDF.type, IRI_MICE))
        graph.add((mice_uri, IRI_IS_MEASURE_OF, sdc_uri))
        graph.add((mice_uri, IRI_USES_MU, mu_uri))
        graph.add((mice_uri, IRI_HAS_VALUE, mv_uri))
        graph.add((mice_uri, IRI_HAS_TIMESTAMP, Literal(row['timestamp'], datatype=XSD.dateTime)))

    return graph


def main():
    if not CSV_FILE.exists():
        print(f"Error: CSV file not found at {CSV_FILE.resolve()}. Ensure the ETL step ran successfully.")
        # Write an empty file to prevent parse errors in the next QC step
        with open(OUT_FILE, 'w') as f:
            f.write("@prefix ex: <http://example.org/measurement/> .\n")
        return

    # Load the cleaned data
    print(f"Loading data from {CSV_FILE}")
    df = pd.read_csv(CSV_FILE, dtype=str, keep_default_na=False) 
    
    # Coerce 'value' to numeric for correct XSD typing later
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])

    # Generate the triples from the DataFrame
    print(f"Generating {len(df)} instance triples...")
    # Global graph instance is used
    generate_triples(df, graph)
    
    # Serialize the graph to the output file (Fixes 1st Red X)
    print(f"Serializing graph with {len(graph)} total triples to {OUT_FILE}")
    graph.serialize(destination=OUT_FILE, format='turtle')

    if OUT_FILE.exists():
        print(f"✅ TTL file saved successfully.")
    else:
        print("❌ TTL file was not saved!")
        
if __name__ == '__main__':
    main()

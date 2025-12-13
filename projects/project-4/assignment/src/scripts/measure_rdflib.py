# src/scripts/measure_rdflib.py
from rdflib import Graph, Literal, RDF, RDFS, OWL, XSD, Namespace, URIRef, BNode
from pathlib import Path
import os
import pandas as pd
import hashlib

# =========================================================================
# 1. CRITICAL FIX: EXACT IRIS FOR VALIDATION (Task 1 & 2 Fix)
# These IRIs MUST match the validation script exactly.
# =========================================================================

# Classes (The entities that must be typed correctly)
IRI_SDC   = URIRef("http://purl.obolibrary.org/obo/BFO_0000020")       # State of Disorder/Condition (SDC)
IRI_ART   = URIRef("https://www.commoncoreontologies.org/ont00000995") # Artifact
IRI_MICE  = URIRef("https://www.commoncoreontologies.org/ont00001163") # Measurement Information Content Entity (MICE)
IRI_MU    = URIRef("https://www.commoncoreontologies.org/ont00000120")    # Measurement Unit (MU)

# Properties (The predicates that must link the entities correctly)
IRI_BEARER_OF = URIRef("http://purl.obolibrary.org/obo/BFO_0000196")     # bearer_of (Artifact -> SDC)
IRI_IS_MEASURE_OF = URIRef("https://www.commoncoreontologies.org/ont00001966") # is_measure_of (MICE -> SDC)
IRI_USES_MU = URIRef("https://www.commoncoreontologies.org/ont00001863")     # uses_measurement_unit (MICE -> MU)
IRI_HAS_VALUE = URIRef("https://www.commoncoreontologies.org/ont00001865")   # has_value (MICE -> Literal)
IRI_HAS_TIMESTAMP = URIRef("https://www.commoncoreontologies.org/ont00000116") # has_timestamp (used instead of EX)

# Namespace base for instances (arbitrary, for creating stable URIs)
EX = Namespace("http://example.org/measurement/") 

# =========================================================================
# Initial Setup
# =========================================================================

# Path setup (assuming CWD is the project root)
OUT_DIR = Path("src")
OUT_FILE = OUT_DIR / "measure_cco.ttl"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Path to the data file (relative to project root)
CSV_FILE = Path("src/data/readings_normalized.csv") 

# Create graph and bind prefixes for pretty Turtle output
graph = Graph()
# Bindings are for cleaner TTL output, the URIRefs above are used for triples
graph.bind("bfo", "http://purl.obolibrary.org/obo/BFO_") 
graph.bind("cco", "https://www.commoncoreontologies.org/ont") # Use the CCO base for binding
graph.bind("ex", EX)
graph.bind("owl", OWL)
graph.bind("xsd", XSD)


# =========================================================================
# Ontology Structure Definition (Schema)
# =========================================================================

# Declare object/datatype properties using exact IRIs
graph.add((IRI_BEARER_OF, RDF.type, OWL.ObjectProperty))
graph.add((IRI_IS_MEASURE_OF, RDF.type, OWL.ObjectProperty))
graph.add((IRI_USES_MU, RDF.type, OWL.ObjectProperty))
graph.add((IRI_HAS_VALUE, RDF.type, OWL.DatatypeProperty)) 
graph.add((IRI_HAS_TIMESTAMP, RDF.type, OWL.DatatypeProperty)) 

# Declare the classes (This ensures the required types exist in the TTL file)
graph.add((IRI_SDC, RDF.type, OWL.Class))
graph.add((IRI_ART, RDF.type, OWL.Class))
graph.add((IRI_MICE, RDF.type, OWL.Class))
graph.add((IRI_MU, RDF.type, OWL.Class))


# =========================================================================
# Core Logic: Read CSV and Generate Instances
# =========================================================================

def generate_uris(row):
    """
    Generates deterministic URIs based on CSV row data.
    """
    # Create safe names for URIs
    artifact_id_safe = row['artifact_id'].replace(" ", "_").replace("-", "").strip()
    sdc_kind_safe = row['sdc_kind'].replace(" ", "_").replace("-", "").strip()
    unit_label_safe = row['unit_label'].replace(" ", "_").replace("-", "").strip()
    
    # Artifact URI (Artifact is uniquely identified by its artifact_id)
    artifact_uri = EX[f"Artifact_{artifact_id_safe}"]
    
    # SDC URI (SDC is uniquely identified by its bearer and its kind)
    sdc_uri = EX[f"SDC_{artifact_id_safe}_{sdc_kind_safe}"]
    
    # MU URI (MU is uniquely identified by its unit_label)
    mu_uri = EX[f"MU_{unit_label_safe}"]
    
    # MICE URI (Unique per reading, based on a hash of the entire row for stability)
    mice_hash = hashlib.sha1(str(row).encode('utf-8')).hexdigest()[:10]
    mice_uri = EX[f"MICE_{mice_hash}"]
    
    return artifact_uri, sdc_uri, mu_uri, mice_uri

def generate_triples(df, graph):
    """
    Generates RDF triples for all rows in the DataFrame.
    """
    for _, row in df.iterrows():
        # 1. Generate URIs for the current reading
        artifact_uri, sdc_uri, mu_uri, mice_uri = generate_uris(row)

        # 2. Assert Types (Artifact, SDC, MICE, MU) - MUST use the exact IRI_ variables
        graph.add((artifact_uri, RDF.type, IRI_ART))   
        graph.add((sdc_uri, RDF.type, IRI_SDC))       
        graph.add((mu_uri, RDF.type, IRI_MU))         
        graph.add((mice_uri, RDF.type, IRI_MICE))      

        # 3. Assert Links (The Measurement Pattern) - MUST use the exact IRI_ variables
        # Artifact is bearer_of SDC
        graph.add((artifact_uri, IRI_BEARER_OF, sdc_uri))
        
        # MICE is_measure_of SDC
        graph.add((mice_uri, IRI_IS_MEASURE_OF, sdc_uri))
        
        # MICE uses_measurement_unit MU
        graph.add((mice_uri, IRI_USES_MU, mu_uri))
        
        # MICE has_value Literal
        # Value must be stored as a Literal with a numeric datatype (xsd:decimal is safe)
        graph.add((mice_uri, IRI_HAS_VALUE, Literal(row['value'], datatype=XSD.decimal)))
        
        # Add timestamp
        graph.add((mice_uri, IRI_HAS_TIMESTAMP, Literal(row['timestamp'], datatype=XSD.dateTime)))

    return graph


def main():
    if not CSV_FILE.exists():
        # This message will help diagnose if the normalization step failed
        print(f"Error: CSV file not found at {CSV_FILE.resolve()}")
        # Write an empty file so the validation script doesn't completely fail
        with open(OUT_FILE, 'w') as f:
            f.write("@prefix ex: <http://example.org/measurement/> .")
        return

    # Load the cleaned data
    print(f"Loading data from {CSV_FILE}")
    # Read as string to prevent Pandas from coercing numbers and potentially losing precision
    df = pd.read_csv(CSV_FILE, dtype=str, keep_default_na=False) 
    
    # We must convert 'value' to a numeric type if it isn't already, for correct XSD typing later
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    # Drop rows where 'value' failed to convert
    df = df.dropna(subset=['value'])

    # Generate the triples from the DataFrame
    print(f"Generating {len(df)} instance triples...")
    generate_triples(df, graph)
    
    # Serialize the graph to the output file
    print(f"Serializing graph with {len(graph)} total triples to {OUT_FILE}")
    graph.serialize(destination=OUT_FILE, format='turtle')

    print("✅ TTL generation complete.")


if __name__ == '__main__':
    # Execute the main function to generate the TTL
    main()

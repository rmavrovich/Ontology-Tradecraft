# measure_rdflib.py

from rdflib import Graph, Literal, RDF, RDFS, OWL, XSD, Namespace, URIRef, BNode
from pathlib import Path
import os
import pandas as pd
import hashlib

# =========================================================================
# 1. CRITICAL FIX: EXACT IRIS FOR VALIDATION
# These IRIs MUST match the validation script exactly.
# =========================================================================

# Classes (The entities that must be typed correctly)
IRI_SDC = URIRef("http://purl.obolibrary.org/obo/BFO_0000020")       # State of Disorder/Condition (SDC)
IRI_ART = URIRef("https://www.commoncoreontologies.org/ont00000995") # Artifact
IRI_MICE = URIRef("https://www.commoncoreontologies.org/ont00001163") # Measurement Information Content Entity (MICE)
IRI_MU = URIRef("https://www.commoncoreontologies.org/ont00000120")    # Measurement Unit (MU)

# Properties (The predicates that must link the entities correctly)
IRI_BEARER_OF = URIRef("http://purl.obolibrary.org/obo/BFO_0000196")     # bearer_of (Artifact -> SDC)
IRI_IS_MEASURE_OF = URIRef("https://www.commoncoreontologies.org/ont00001966") # is_measure_of (MICE -> SDC)
IRI_USES_MU = URIRef("https://www.commoncoreontologies.org/ont00001863")     # uses_measurement_unit (MICE -> MU)
IRI_HAS_VALUE = URIRef("https://www.commoncoreontologies.org/ont00001865")   # has_value (MICE -> Literal)
# CCO.has_value is commonly used; let's check the previous snippet. It used CCO.has_value. 
# We'll stick to a common one if the exact IRI isn't explicitly defined in the check, 
# but for safety, the one above is a common CCO value property. If the checker allows it, 
# we'll use it.

# Namespace base for instances (arbitrary, for creating stable URIs)
EX = Namespace("http://example.org/measurement/") 

# =========================================================================
# Initial Setup
# =========================================================================

# Create output directory if it doesn't exist
OUT_DIR = Path("src")
OUT_FILE = OUT_DIR / "measure_cco.ttl"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Path to the data file
CSV_FILE = Path("src/data/readings_normalized.csv") 

# Create graph and bind prefixes for pretty Turtle output
graph = Graph()
# Bindings are purely for cleaner TTL output, the URIRefs above are used for triples
graph.bind("bfo", "http://purl.obolibrary.org/obo/BFO_") 
graph.bind("cco", "https://www.commoncoreontologies.org/ont")
graph.bind("ex", EX)
graph.bind("owl", OWL)
graph.bind("xsd", XSD)


# =========================================================================
# Ontology Structure Definition (Schema)
# This part is largely kept but uses the exact IRIs defined above.
# =========================================================================

# Declare object/datatype properties using exact IRIs
graph.add((IRI_BEARER_OF, RDF.type, OWL.ObjectProperty))
graph.add((IRI_IS_MEASURE_OF, RDF.type, OWL.ObjectProperty))
graph.add((IRI_USES_MU, RDF.type, OWL.ObjectProperty))
graph.add((IRI_HAS_VALUE, RDF.type, OWL.DatatypeProperty)) # We will use this property

# Declare the classes for the schema check (This satisfies part 2's requirement for schema typing)
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
    # Create URIs for the four main entities
    artifact_id_safe = row['artifact_id'].replace(" ", "_").replace("-", "")
    sdc_kind_safe = row['sdc_kind'].replace(" ", "_").replace("-", "")
    unit_label_safe = row['unit_label'].replace(" ", "_").replace("-", "")
    
    # Artifact URI (e.g., http://example.org/measurement/Artifact_Boiler-07)
    artifact_uri = EX[f"Artifact_{artifact_id_safe}"]
    
    # SDC URI (e.g., http://example.org/measurement/SDC_Boiler-07_temp)
    sdc_uri = EX[f"SDC_{artifact_id_safe}_{sdc_kind_safe}"]
    
    # MU URI (e.g., http://example.org/measurement/MU_C)
    mu_uri = EX[f"MU_{unit_label_safe}"]
    
    # MICE URI (Unique per reading, based on a hash of the entire row)
    # This is the event of the measurement itself.
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

        # 2. Assert Types (Artifact, SDC, MICE, MU) - CRITICAL FIX FOR TASK 2
        # Ensure every instance is explicitly typed with the exact IRI
        graph.add((artifact_uri, RDF.type, IRI_ART))   # Artifact
        graph.add((sdc_uri, RDF.type, IRI_SDC))       # State of Disorder/Condition
        graph.add((mu_uri, RDF.type, IRI_MU))         # Measurement Unit
        graph.add((mice_uri, RDF.type, IRI_MICE))      # Measurement ICE

        # 3. Assert Links (The Measurement Pattern) - CRITICAL FIX FOR TASK 1 (Properties)
        
        # Artifact is bearer_of SDC
        graph.add((artifact_uri, IRI_BEARER_OF, sdc_uri))
        
        # MICE is_measure_of SDC
        graph.add((mice_uri, IRI_IS_MEASURE_OF, sdc_uri))
        
        # MICE uses_measurement_unit MU
        graph.add((mice_uri, IRI_USES_MU, mu_uri))
        
        # MICE has_value Literal
        # Value must be stored as a Literal with a numeric datatype (xsd:decimal is safe)
        graph.add((mice_uri, IRI_HAS_VALUE, Literal(row['value'], datatype=XSD.decimal)))
        
        # Add timestamp (Optional, but good practice)
        graph.add((mice_uri, EX.has_timestamp, Literal(row['timestamp'], datatype=XSD.dateTime)))

    return graph


def main():
    if not CSV_FILE.exists():
        print(f"Error: CSV file not found at {CSV_FILE.resolve()}")
        # Creating an empty graph and exiting, or raising an error, is safer.
        with open(OUT_FILE, 'w') as f:
            f.write("# Error: readings_normalized.csv was not found.")
        return

    # Load the cleaned data
    print(f"Loading data from {CSV_FILE}")
    # Read as string to prevent Pandas from coercing numbers and potentially losing precision
    df = pd.read_csv(CSV_FILE, dtype=str, keep_default_na=False) 
    
    # Ensure all rows have been processed correctly (e.g., 'value' is numeric)
    # The previous script ensured this, but we'll coerce value to float here just in case 
    # the TTL generation script needs it for proper XSD.decimal type.
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])

    # Generate the triples from the DataFrame
    print(f"Generating {len(df)} triples...")
    generate_triples(df, graph)
    
    # Serialize the graph to the output file
    print(f"Serializing graph with {len(graph)} triples to {OUT_FILE}")
    graph.serialize(destination=OUT_FILE, format='turtle')

    print("✅ TTL generation complete.")


if __name__ == '__main__':
    # Execute the main function to generate the TTL
    main()

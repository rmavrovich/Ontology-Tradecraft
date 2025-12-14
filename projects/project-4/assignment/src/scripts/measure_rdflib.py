# src/scripts/measure_rdflib.py

from rdflib import Graph, Literal, RDF, RDFS, OWL, XSD, Namespace, URIRef, BNode
from pathlib import Path
import pandas as pd
import hashlib

# 1. CRITICAL FIX: EXACT IRIS FOR VALIDATION (MUST MATCH)
# =========================================================================

# Classes (The entities that must be typed correctly)
IRI_SDC   = URIRef("http://purl.obolibrary.org/obo/BFO_0000020")       # State of Disorder/Condition (SDC)
IRI_ART   = URIRef("https://www.commoncoreontologies.org/ont00000995") # Artifact
IRI_MICE  = URIRef("https://www.commoncoreontologies.org/ont00001163") # Measurement Information Content Entity (MICE)
IRI_MU    = URIRef("https://www.commoncoreontologies.org/ont00000120")    # Measurement Unit (MU)

# Properties (The predicates that must link the entities correctly)
IRI_BEARER_OF = URIRef("http://purl.obolibrary.org/obo/BFO_0000196")     # bearer_of (Artifact -> SDC)
IRI_IS_MEASURE_OF = URIRef("https://www.commoncoreontologies.org/ont00001966") # is_measurement_of (MICE -> SDC)
IRI_USES_MU       = URIRef("https://www.commoncoreontologies.org/ont00001863") # uses_measurement_unit (MICE -> MU)
IRI_HAS_VALUE     = URIRef("https://www.commoncoreontologies.org/ont00001769") # has_value (MICE -> Literal Value)
IRI_HAS_TIMESTAMP = URIRef("https://www.commoncoreontologies.org/ont00001767") # has_timestamp (MICE -> Literal Time)


# =========================================================================
# 2. FILE AND NAMESPACE SETUP
# =========================================================================

# The CSV file output by normalize_readings.py
CSV_FILE = Path("src/data/readings_normalized.csv") 
# The TTL file read by the QC script
OUT_FILE = Path("src/measure_cco.ttl")

# Define namespaces (You can rename NS_rdf to NS_EX to match your generate_uris function)
# NOTE: The prefixes used here must match the URIs used later in the generate_uris function
NS_EX   = Namespace("http://example.org/measurement/") # Used for instance URIs like Artifact_...
NS_CCO  = Namespace("https://www.commoncoreontologies.org/CommonCoreOntologiesMerged/") # Note the trailing slash
NS_OWL  = Namespace("http://www.w3.org/2002/07/owl#")
NS_OBO  = Namespace("http://purl.obolibrary.org/obo/")
# ... and so on for others if needed later

def setup_graph():
    """Initializes graph with namespaces."""
    g = Graph()
    # Bind the instance namespace (used in generate_uris)
    g.bind("ex", NS_EX)
    # Bind the main ontology namespace
    g.bind("cco", NS_CCO) 
    # Bind standard namespaces (many of these are automatically handled by rdflib, 
    # but explicitly binding them ensures correct serialization prefix)
    g.bind("owl", NS_OWL)
    g.bind("obo", NS_OBO) 
    g.bind("rdf", RDF) # RDF is already defined by rdflib
    g.bind("rdfs", RDFS) # RDFS is already defined by rdflib
    g.bind("xsd", XSD)   # XSD is already defined by rdflib
    
    return g
# Fix the global namespace variable used in generate_uris (was NS_EX in the original code, but defined as NS_rdf)
NS_EX = Namespace("http://example.org/measurement/") 

# Global graph instance
graph = setup_graph()
# =========================================================================
# 3. URI GENERATION LOGIC (UNCHANGED)
# =========================================================================

def generate_uris(row):
    """Generates deterministic URIs based on content hashes."""
    
    # 1. FIX: Ensure all identifiers are strings for hashing, INCLUDING value and timestamp
    artifact_id_str = str(row['artifact_id'])
    sdc_kind_str = str(row['sdc_kind'])
    unit_label_str = str(row['unit_label'])
    value_str = str(row['value'])        # <-- UNCOMMENTED
    timestamp_str = str(row['timestamp'])  # <-- UNCOMMENTED
    
    # 1. Artifact URI (based on its unique ID)
    artifact_uri = NS_EX[f"Artifact_{hashlib.sha256(artifact_id_str.encode()).hexdigest()[:8]}"]

    # 2. SDC URI (based on Artifact and the SDC kind)
    sdc_identifier = f"{artifact_id_str}_{sdc_kind_str}"
    sdc_uri = NS_EX[f"SDC_{hashlib.sha256(sdc_identifier.encode()).hexdigest()[:8]}"]
    
    # 3. MU URI (based on the unit label)
    mu_uri = NS_EX[f"MU_{hashlib.sha256(unit_label_str.encode()).hexdigest()[:8]}"]

    # 4. Measurement Value URI (MV_URI) - NEW
    # Based on the decimal value itself
    mv_uri = NS_EX[f"MV_{hashlib.sha256(value_str.encode()).hexdigest()[:8]}"] # <-- UNCOMMENTED
    
    # 5. MICE URI (based on SDC, timestamp, and value for unique measurement)
    # FIX: Include value and timestamp in the identifier to make MICE unique
    mice_identifier = f"{sdc_identifier}_{timestamp_str}_{value_str}" # <-- FIXED STRING
    mice_uri = NS_EX[f"MICE_{hashlib.sha256(mice_identifier.encode()).hexdigest()[:8]}"]

    # FIX: Return all five URIs
    return artifact_uri, sdc_uri, mu_uri, mv_uri, mice_uri # <-- ADDED mv_uri


## Required Fix in `generate_triples`

#Once you apply the fix above, you must also update the assignment line in `generate_triples` to expect 5 values:

```python
# In generate_triples:
# FIX: The assignment must now match the five expected URIs
artifact_uri, sdc_uri, mu_uri, mv_uri, mice_uri = generate_uris(row)

# =========================================================================
# 4. TRIPLE GENERATION LOGIC (FIXED)
# =========================================================================

def generate_triples(df, graph):
    """
    Generates RDF triples for all rows in the DataFrame, incorporating the new 
    Measurement Value (MV) entity.
    """
    # Use a set to track generated Artifact, SDC, MU, and MV nodes
    seen_static_entities = set()
    
    for _, row in df.iterrows():
        
        # 1. FIX: Update the call to generate_uris to include mv_uri
        # This now expects 5 URIs: (artifact, sdc, mu, mv, mice)
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

        # 2. FIX: Measurement Value (MV) - NEW STATIC ENTITY
        mv_key = str(mv_uri)
        if mv_key not in seen_static_entities:
            # MV is defined and has a type
            # Using IRI_VALUE, which is assumed to be defined as the CCO class for Value
            graph.add((mv_uri, RDF.type, IRI_VALUE)) 
            
            # MV has_value Literal (Value) - The MV node carries the literal value
            # This is the actual measurement result attached as a Literal
            graph.add((mv_uri, IRI_HAS_VALUE, Literal(row['value'], datatype=XSD.decimal)))
            seen_static_entities.add(mv_key)

        # --- Dynamic Entity (MICE) ---
        # MICE is generated for every reading

        # MICE is defined and has a type
        graph.add((mice_uri, RDF.type, IRI_MICE))
        
        # MICE is_measure_of SDC 
        graph.add((mice_uri, IRI_IS_MEASURE_OF, sdc_uri))
        
        # MICE uses_measurement_unit MU 
        graph.add((mice_uri, IRI_USES_MU, mu_uri))

        # 3. FIX: MICE has_measurement_value MV (NEW TRIPLE)
        # MICE links to the reusable MV node, which in turn holds the literal value.
        graph.add((mice_uri, IRI_HAS_VALUE, mv_uri))
        
        # 4. FIX: MICE has_timestamp Literal (Time)
        graph.add((mice_uri, IRI_HAS_TIMESTAMP, Literal(row['timestamp'], datatype=XSD.dateTime)))

    return graph

# =========================================================================
# 5. MAIN EXECUTION LOGIC (FIXED)
# =========================================================================

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
    # setup_graph is now called before main
    main()

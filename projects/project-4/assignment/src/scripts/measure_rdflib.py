# src/scripts/measure_rdflib.py

from rdflib import Graph, Literal, RDF, RDFS, OWL, XSD, Namespace, URIRef, BNode
from pathlib import Path
import pandas as pd
import hashlib

# =========================================================================
# 1. CRITICAL FIX: EXACT IRIS FOR VALIDATION (MUST MATCH)
# This solves the "Artifact=0, SDC=0, MICE=0, MU=0" error.
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
IRI_HAS_VALUE = URIRef("https://www.commoncoreontologies.org/ont00001865")   # has_value
IRI_HAS_TIMESTAMP = URIRef("https://www.commoncoreontologies.org/ont00000116") # has_timestamp

# Namespace base for instances (arbitrary, for creating stable URIs)
EX = Namespace("http://example.org/measurement/") 

# =========================================================================
# Initial Setup
# =========================================================================

# Path setup 
OUT_DIR = Path("src")
OUT_FILE = OUT_DIR / "measure_cco.ttl"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Path to the data file 
CSV_FILE = Path("src/data/readings_normalized.csv") 

# Create graph and bind prefixes for pretty Turtle output
graph = Graph()
# Bindings are for cleaner TTL output, the URIRefs above are used for triples
graph.bind("bfo", "http://purl.obolibrary.org/obo/BFO_") 
graph.bind("cco", "https://www.commoncoreontologies.org/ont") 
graph.bind("ex", EX)
graph.bind("owl", OWL)
graph.bind("xsd", XSD)


# =========================================================================
# Ontology Structure Definition (Schema)
# Ensures the URIs are defined as classes/properties in the TTL file.
# =========================================================================

# Declare object/datatype properties using exact IRIs
graph.add((IRI_BEARER_OF, RDF.type, OWL.ObjectProperty))
graph.add((IRI_IS_MEASURE_OF, RDF.type, OWL.ObjectProperty))
graph.add((IRI_USES_MU, RDF.type, OWL.ObjectProperty))
graph.add((IRI_HAS_VALUE, RDF.type, OWL.DatatypeProperty)) 
graph.add((IRI_HAS_TIMESTAMP, RDF.type, OWL.DatatypeProperty)) 

# Declare the classes 
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
    # Create safe names for URIs (clean up spaces and dashes)
    artifact_id_safe = row['artifact_id'].replace(" ", "_").replace("-", "").strip()
    sdc_kind_safe = row['sdc_kind'].replace(" ", "_").replace("-", "").strip()
    unit_label_safe = row['unit_label'].replace(" ", "_").replace("-", "").strip()
    
    # Instance URIs
    artifact_uri = EX[f"Artifact_{artifact_id_safe}"]
    sdc_uri = EX[f"SDC_{artifact_id_safe}_{sdc_kind_safe}"]
    mu_uri = EX[f"MU_{unit_label_safe}"]
    
    # MICE URI (Unique per reading)
    mice_hash = hashlib.sha1(str(row).encode('utf-8')).hexdigest()[:10]
    mice_uri = EX[f"MICE_{mice_hash}"]
    
    return artifact_uri, sdc_uri, mu_uri, mice_uri

def generate_triples(df, graph):
    """
    Generates RDF triples for all rows in the DataFrame.
    """
    for _, row in df.iterrows():
        # 1. Generate URIs 
        artifact_uri, sdc_uri, mu_uri, mice_uri = generate_uris(row)

        # 2. Assert Types (CRITICAL: Using the exact IRI_ variables for the type object)
        graph.add((artifact_uri, RDF.type, IRI_ART))   
        graph.add((sdc_uri, RDF.type, IRI_SDC))       
        graph.add((mu_uri, RDF

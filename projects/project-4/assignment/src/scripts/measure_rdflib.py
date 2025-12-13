# measure_rdflib.py

from rdflib import Graph, Literal, RDF, RDFS, OWL, XSD, Namespace, URIRef, BNode
import os

# Create output directory if it doesn't exist
os.makedirs("src", exist_ok=True)

# Define namespaces
CCO = Namespace("https://ontology.ihccglobal.org/CCO/")
BFO = Namespace("http://purl.obolibrary.org/obo/BFO_")
EX = Namespace("http://example.org/measurement/")

# Create graph and bind prefixes for pretty Turtle output
graph = Graph()
graph.bind("cco", CCO)
graph.bind("bfo", BFO)
graph.bind("ex", EX)
graph.bind("owl", OWL)
graph.bind("xsd", XSD)

# ========================
# Declare object/datatype properties
# ========================
graph.add((CCO.bearer_of, RDF.type, OWL.ObjectProperty))  # Note: may be from BFO, but using CCO here for consistency
graph.add((CCO.is_measure_of, RDF.type, OWL.ObjectProperty))
graph.add((CCO.has_value, RDF.type, OWL.DatatypeProperty))
graph.add((CCO.uses_measurement_unit, RDF.type, OWL.ObjectProperty))

# ========================
# Class-level Design Pattern (using OWL restrictions with BNodes)
# ========================

# 1. MICE 'is_measure_of' some SDC
mice_restr1 = BNode()
graph.add((mice_restr1, RDF.type, OWL.Restriction))
graph.add((mice_restr1, OWL.onProperty, CCO.is_measure_of))
graph.add((mice_restr1, OWL.someValuesFrom, BFO.SDC))
graph.add((CCO.MICE, RDFS.subClassOf, mice_restr1))

# 2. MICE 'has_value' exactly 1
mice_restr2 = BNode()
graph.add((mice_restr2, RDF.type, OWL.Restriction))
graph.add((mice_restr2, OWL.onProperty, CCO.has_value))
graph.add((mice_restr2, OWL.cardinality, Literal("1", datatype=XSD.nonNegativeInteger)))
graph.add((CCO.MICE, RDFS.subClassOf, mice_restr2))

# 3. MICE 'uses_measurement_unit' some MU
mice_restr3 = BNode()
graph.add((mice_restr3, RDF.type, OWL.Restriction))
graph.add((mice_restr3, OWL.onProperty, CCO.uses_measurement_unit))
graph.add((mice_restr3, OWL.someValuesFrom, CCO.MU))
graph.add((CCO.MICE, RDFS.subClassOf, mice_restr3))

# 4. Artifact 'bearer_of' some MICE
art_restr = BNode()
graph.add((art_restr, RDF.type, OWL.Restriction))
graph.add((art_restr, OWL.onProperty, BFO.bearer_of))  # Using BFO bearer_of as in the diagram
graph.add((art_restr, OWL.someValuesFrom, CCO.MICE))
graph.add((CCO.Artifact, RDFS.subClassOf, art_restr))


# ========================
# Serialize to Turtle file
# ========================
output_file = "src/measure_cco.ttl"
graph.serialize(destination=output_file, format="turtle")

print(f"RDF graph successfully written to {output_file}")
print(f"Total triples: {len(graph)}")
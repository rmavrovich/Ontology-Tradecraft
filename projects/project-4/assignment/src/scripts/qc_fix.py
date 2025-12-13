from rdflib import Graph, URIRef
from pathlib import Path

# --- Configuration ---
TTL = Path("src/measure_cco.ttl")
assert TTL.exists(), "❌ src/measure_cco.ttl not found"

# Exact IRIs to enforce (MUST match those used in measure_rdflib.py)
IRI_SDC   = URIRef("http://purl.obolibrary.org/obo/BFO_0000020")
IRI_ART   = URIRef("https://www.commoncoreontologies.org/ont00000995")
IRI_MU    = URIRef("https://www.commoncoreontologies.org/ont00000120")
IRI_MICE  = URIRef("https://www.commoncoreontologies.org/ont00001163")

IRI_BEARER_OF   = URIRef("http://purl.obolibrary.org/obo/BFO_0000196")
IRI_IS_MEASURE_OF = URIRef("https://www.commoncoreontologies.org/ont00001966")
IRI_USES_MU       = URIRef("https://www.commoncoreontologies.org/ont00001863")
# ---------------------


g = Graph(); g.parse(TTL, format="turtle")
print(f"[ttl] triples: {len(g)}")

# 1) Ensure at least one of each type appears using the EXACT class IRIs
q_types = f"""
SELECT
  (COUNT(DISTINCT ?a) AS ?A)
  (COUNT(DISTINCT ?s) AS ?S)
  (COUNT(DISTINCT ?m) AS ?M)
  (COUNT(DISTINCT ?u) AS ?U)
WHERE {{
  OPTIONAL {{ ?a a <{IRI_ART}> . }}
  OPTIONAL {{ ?s a <{IRI_SDC}> . }}
  OPTIONAL {{ ?m a <{IRI_MICE}> . }}
  OPTIONAL {{ ?u a <{IRI_MU}> . }}
}}
"""
A,S,M,U = [int(x) for x in list(g.query(q_types))[0]]
assert all(v>0 for v in (A,S,M,U)), f"❌ Missing required typed nodes: Artifact={A}, SDC={S}, MICE={M}, MU={U}"
print(f"✅ Types present with exact IRIs: Artifact={A}, SDC={S}, MICE={M}, MU={U}")

# 2) Ensure at least one complete pattern exists using ONLY the exact property IRIs
q_pattern_strict = f"""
ASK {{
  ?a a <{IRI_ART}> ;
     <{IRI_BEARER_OF}> ?sdc .
  ?sdc a <{IRI_SDC}> .

  ?m a <{IRI_MICE}> ;
     <{IRI_IS_MEASURE_OF}> ?sdc ;
     <{IRI_USES_MU}> ?u .

  ?u a <{IRI_MU}> .
}}
"""
assert bool(g.query(q_pattern_strict).askAnswer), "❌ No complete pattern found using the exact property IRIs."
print("✅ Complete pattern found with exact property IRIs.")

# 3) All MICE Check: Split into two queries to avoid the rdflib UNION bug
# This is the replacement for the problematic single UNION query.
q_missing_measure_of = f"""
SELECT DISTINCT ?m WHERE {{
  ?m a <{IRI_MICE}> .
  FILTER NOT EXISTS {{ ?m <{IRI_IS_MEASURE_OF}> ?sdc . ?sdc a <{IRI_SDC}> . }}
}}
"""
q_missing_unit = f"""
SELECT DISTINCT ?m WHERE {{
  ?m a <{IRI_MICE}> .
  FILTER NOT EXISTS {{ ?m <{IRI_USES_MU}> ?u . ?u a <{IRI_MU}> . }}
}}
"""

# Execute queries and combine results in Python
bad_mice = set()
bad_mice.update(list(g.query(q_missing_measure_of)))
bad_mice.update(list(g.query(q_missing_unit)))

n_bad = len(bad_mice)

assert n_bad == 0, f"❌ Some MICE are missing required links with the exact IRIs (count={n_bad})."
print("✅ All MICE use exact IRIs for 'is measure of' and 'uses measurement unit'.")

print("✅ RDF passes exact-IRI checks for the measurement design pattern.")

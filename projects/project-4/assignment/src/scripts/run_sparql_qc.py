from rdflib import Graph
from pathlib import Path
import sys
  
TTL = Path("src/measure_cco.ttl")

# Adjust path relative to the PROJECT_ROOT environment variable, assuming default setup
if not TTL.exists() and Path(__file__).resolve().parents[2].name == 'assignment':
    BASE_DIR = Path(__file__).resolve().parents[2]
    TTL = BASE_DIR / "src" / "measure_cco.ttl"
  
try:
    assert TTL.exists(), f"❌ src/measure_cco.ttl not found at {TTL.resolve()}"
    g = Graph(); g.parse(TTL, format="turtle")
    print(f"[ttl] triples: {len(g)}")
except AssertionError as e:
    print(e)
    sys.exit(2) 
except Exception as e:
    print(f"❌ Error loading TTL file: {e}")
    sys.exit(1)
  
# Exact IRIs to enforce (must match the IRIs used in measure_rdflib.py)
IRI_SDC   = "http://purl.obolibrary.org/obo/BFO_0000020"
IRI_ART   = "https://www.commoncoreontologies.org/ont00000995"
IRI_MU    = "https://www.commoncoreontologies.org/ont00000120"
IRI_MICE  = "https://www.commoncoreontologies.org/ont00001163"
  
IRI_BEARER_OF   = "http://purl.obolibrary.org/obo/BFO_0000196"
IRI_IS_MEASURE_OF = "https://www.commoncoreontologies.org/ont00001966"
IRI_USES_MU       = "https://www.commoncoreontologies.org/ont00001863"
  
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
A,S,M,U = [int(x) for x in list(g.query(q_types.strip()))[0]]
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
assert bool(g.query(q_pattern_strict.strip()).askAnswer), "❌ No complete pattern found using the exact property IRIs."
print("✅ Complete pattern found with exact property IRIs.")
  
# 3) Optional: verify every MICE uses the exact property IRIs (no alternative predicates)
# CRITICAL FIX: MANUALLY STRIP ALL WHITESPACE FROM THE UNION QUERY
q_raw = f"""
SELECT (COUNT(DISTINCT ?m) AS ?n_bad)
WHERE {{
{{
?m a <{IRI_MICE}> .
FILTER NOT EXISTS {{ ?m <{IRI_IS_MEASURE_OF}> ?sdc . ?sdc a <{IRI_SDC}> . }}
}}
UNION
{{
?m a <{IRI_MICE}> .
FILTER NOT EXISTS {{ ?m <{IRI_USES_MU}> ?u . ?u a <{IRI_MU}> . }}
}}
}}
"""
q_all_mice_ok = "\n".join([line.strip() for line in q_raw.splitlines()])

# CRITICAL FIX: Use .strip() to clean leading/trailing whitespace before execution
n_bad = int(list(g.query(q_all_mice_ok.strip()))[0][0])
assert n_bad == 0, f"❌ Some MICE are missing required links with the exact IRIs (count={n_bad})."
print("✅ All MICE use exact IRIs for 'is measure of' and 'uses measurement unit'.")
  
print("✅ RDF passes exact-IRI checks for the measurement design pattern.")

# Exit successfully if all checks pass
sys.exit(0)

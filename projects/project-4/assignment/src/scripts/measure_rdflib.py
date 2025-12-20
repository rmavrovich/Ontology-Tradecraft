from rdflib import Graph, Literal, RDF, RDFS, OWL, XSD, Namespace, URIRef, BNode
from pathlib import Path
import pandas as pd
import hashlib
import re
from collections import defaultdict

# --- Configuration & Paths ---
script_dir = Path(__file__).parent
root_dir = script_dir.parent

CSV_FILE = root_dir / 'data' / 'readings_normalized.csv'
OUT_FILE = root_dir / 'measure_cco.ttl'

# --- Namespaces ---
NS_EX      = Namespace("http://example.org/measurement/")
NS_CCO     = Namespace("https://www.commoncoreontologies.org/CommonCoreOntologiesMerged/")
NS_OWL     = Namespace("http://www.w3.org/2002/07/owl#")
NS_OBO     = Namespace("http://purl.obolibrary.org/obo/")
NS_RDF     = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
NS_XSD     = Namespace("http://www.w3.org/2001/XMLSchema#")
NS_EXPROP  = Namespace("http://example.org/props#")
NS_EXC     = Namespace("http://example.org/classes#")

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

# --- Definitions Mapping ---
DIFFERENTIA = {
    NS_CCO.ont00000995: "is intentionally produced to realize some function or purpose",  
    NS_CCO.ont00001163: "specifies a numeric measurement value together with its associated unit",  
    NS_CCO.ont00000120: "serves to standardize quantities for measurement information content entities and specifically dependent continuants",  
    NS_OBO.BFO_0000197: "relates a specifically dependent continuant to the independent continuant it inheres in",  
    NS_CCO.ont00001966: "links a measurement information content entity to the reading (specifically dependent continuant) that it specifies",
    NS_CCO.ont00001863: "links a measurement information content entity or a specifically dependent continuant to the unit that qualifies its value",
    NS_CCO.ont00001769: "associates a measurement information content entity with a numeric value literal",
    NS_CCO.ont00000441: "A Quality that inheres in a bearer in virtue of its thermal energy."
}

# --- Helper Functions ---

def _slug(text: str) -> str:
    t = re.sub(r"[^A-Za-z0-9_]+", "-", (str(text) or "")).strip("-")
    t = re.sub(r"-+", "-", t)
    return t or "na"

def article_for(s: str) -> str:
    return "An" if s[:1].lower() in ("a", "e", "i", "o", "u") else "A"

def label_or_localname(iri: URIRef) -> str:
    for _, _, lab in graph.triples((iri, RDFS.label, None)):
        if isinstance(lab, Literal) and (lab.language or "").lower().startswith("en"):
            return str(lab).strip()
    s = str(iri)
    local = s.rsplit("#", 1)[-1] if "#" in s else s.rsplit("/", 1)[-1]
    return local.replace("_", " ").replace("-", " ").strip()

def parent_of(term: URIRef):
    for _, _, parent in graph.triples((term, RDFS.subClassOf, None)):
        if isinstance(parent, URIRef):
            return parent
    return None

def has_english_definition(term_iri: URIRef) -> bool:
    for _, _, c in graph.triples((term_iri, RDFS.comment, None)):
        if isinstance(c, Literal) and (c.language or "en").lower().startswith("en"):
            return True
    return False

def _whole_word_replace(text: str, token: str, replacement: str) -> str:
    if not token: return text
    pattern = r'(?i)(^|[^A-Za-z0-9_])(' + re.escape(token) + r')([^A-Za-z0-9_]|$)'
    return re.sub(pattern, r'\1' + replacement + r'\3', text)

def make_non_self_referential(def_text: str, class_iri: URIRef) -> str:
    if not isinstance(def_text, str) or not def_text.strip():
        return def_text
    class_label = label_or_localname(class_iri).strip()
    iri_str = str(class_iri)
    local_token = iri_str.rsplit("#", 1)[-1] if "#" in iri_str else iri_str.rsplit("/", 1)[-1]
    
    lower_def = def_text.lower()
    split_needles = (" is a ", " is an ")
    split_idx = -1
    needle_used = ""
    for needle in split_needles:
        i = lower_def.find(needle)
        if i != -1:
            split_idx = i
            needle_used = def_text[i:i+len(needle)]
            break
    if split_idx == -1: return def_text

    head = def_text[:split_idx + len(needle_used)]
    body = def_text[split_idx + len(needle_used):]
    new_body = _whole_word_replace(body, class_label, "this entity")
    if local_token and local_token.lower() != class_label.lower():
        new_body = _whole_word_replace(new_body, local_token, "this class")
    return head + new_body

def ensure_clean_definition(term_iri: URIRef, definition_text: str):
    is_class = any(graph.triples((term_iri, RDF.type, OWL.Class)))
    clean_text = make_non_self_referential(definition_text, term_iri) if is_class else definition_text
    if not has_english_definition(term_iri):
        graph.add((term_iri, RDFS.comment, Literal(clean_text.strip(), lang="en")))

# --- Domain Logic ---

def canonicalize_kind(raw: str) -> tuple[str, str]:
    KIND_ALIASES = {
        "Temperature": {"temperature", "temp", "tmp", "t", "degc", "degf", "c", "f"},
        "Pressure": {"pressure", "press", "psi", "bar", "pa", "kpa"},
        "Voltage": {"voltage", "volt", "volts", "v"},
    }
    base = (raw or "").strip().lower()
    for canonical, forms in KIND_ALIASES.items():
        if base in forms:
            return canonical, _slug(canonical.lower())
    return base.title(), _slug(base)

def resolve_unit_and_value(unit_raw: str, value_in: float):
    canon = _slug(unit_raw)
    mapping = {
        "c": (NS_CCO.ont00001606, value_in),
        "f": (NS_CCO.ont00001724, value_in),
        "pa": (NS_CCO.ont00001559, value_in),
        "psi": (NS_CCO.ont00001694, value_in),
        "volt": (NS_CCO.ont00001450, value_in),
    }
    return mapping.get(canon, (URIRef(NS_EX + canon), value_in))

def generate_triples(df, graph):
    seen_static = set()
    for _, row in df.iterrows():
        art_id = str(row['artifact_id'])
        art_slug = _slug(art_id)
        kind_label, kind_slug = canonicalize_kind(row['sdc_kind'])
        ts_raw = str(row['timestamp'])
        ts_slug = _slug(ts_raw)
        
        art_uri = URIRef(NS_EX + art_slug)
        sdc_uri = URIRef(NS_EX + f"{art_slug}_{kind_slug}_{ts_slug}")
        mice_uri = URIRef(NS_EX + f"MICE_{art_slug}_{kind_slug}_{ts_slug}")
        unit_uri, val = resolve_unit_and_value(row['unit_label'], row['value'])

        # Static Artifact Info
        if art_slug not in seen_static:
            graph.add((art_uri, RDF.type, NS_CCO.ont00000995))
            graph.add((art_uri, RDFS.label, Literal(art_id, lang="en")))
            seen_static.add(art_slug)

        # Measurement Chain
        graph.add((sdc_uri, RDF.type, NS_OBO.BFO_0000020))
        graph.add((sdc_uri, NS_OBO.BFO_0000197, art_uri))
        graph.add((sdc_uri, RDFS.label, Literal(f"{art_id} {kind_label} reading", lang="en")))
        
        graph.add((mice_uri, RDF.type, NS_CCO.ont00001163))
        graph.add((mice_uri, NS_CCO.ont00001966, sdc_uri))
        graph.add((mice_uri, NS_CCO.ont00001769, Literal(val, datatype=XSD.decimal)))
        graph.add((mice_uri, NS_CCO.ont00001863, unit_uri))
        graph.add((mice_uri, NS_EXPROP.hasTimestamp, Literal(ts_raw, datatype=XSD.dateTime)))

# --- Main Runner ---

def main():
    if not CSV_FILE.exists():
        print(f"Error: {CSV_FILE} not found.")
        return

    print(f"Loading data from {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE, dtype=str, keep_default_na=False)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])

    # 1. Process instance data
    generate_triples(df, graph)

    # 2. Enrich with definitions for all terms used in the graph
    seen_terms = set(graph.subjects(RDF.type, None)) | set(graph.objects(RDF.type, None))
    for term in seen_terms:
        if isinstance(term, URIRef) and not has_english_definition(term):
            parent = parent_of(term)
            p_label = label_or_localname(parent) if parent else "parent class (unspecified)"
            
            d_start = f"{article_for(label_or_localname(term))} {label_or_localname(term)} is a "
            d_body = DIFFERENTIA.get(term, f"has not yet had its differentiating factor specified relative to {p_label}")
            
            ensure_clean_definition(term, d_start + d_body)

    # 3. Serialize
    print(f"Serializing {len(graph)} triples to {OUT_FILE}...")
    graph.serialize(destination=str(OUT_FILE), format="turtle")
    print("✅ Process complete.")

if __name__ == '__main__':
    main()

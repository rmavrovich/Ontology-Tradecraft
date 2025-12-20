from rdflib import Graph, Literal, RDF, RDFS, OWL, XSD, Namespace, URIRef, BNode
from pathlib import Path
import pandas as pd
import hashlib
from rdflib.namespace import XSD, RDFS, OWL
import re
from collections import defaultdict

"""
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
"""

script_dir = Path(__file__).parent
root_dir = script_dir.parent

CSV_FILE = root_dir / 'data' / 'readings_normalized.csv'
OUT_FILE = root_dir / 'measure_cco.ttl'

# Define namespaces 
NS_EX   = Namespace("http://example.org/measurement/")
NS_CCO  = Namespace("https://www.commoncoreontologies.org/CommonCoreOntologiesMerged/") 
NS_OWL  = Namespace("http://www.w3.org/2002/07/owl#")
NS_OBO  = Namespace("http://purl.obolibrary.org/obo/")
NS_RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
NS_XSD = Namespace("http://www.w3.org/2001/XMLSchema#")
NS_EXPROP = Namespace("http://example.org/props#")
NS_EXC = Namespace("http://example.org/classes#")

"""Initializes graph with namespaces."""
graph = Graph()
graph.bind("ex", NS_EX)
graph.bind("cco", NS_CCO) 
graph.bind("owl", NS_OWL)
graph.bind("obo", NS_OBO) 
graph.bind("rdf", NS_RDF) 
graph.bind("rdfs", RDFS) # RDFS is already defined by rdflib
graph.bind("xsd", NS_XSD) 

graph.bind("exc", NS_EXC)
graph.bind("exprop", NS_EXPROP)

onto_uri = URIRef("http://example.org/ontology")
graph.add((onto_uri, NS_RDF.type, NS_OWL.Ontology))
graph.add((onto_uri, RDFS.label, Literal("cco conformant measurements", lang="en")))


graph.add((NS_CCO.ont00000995, RDFS.label, Literal("Artifact", lang="en")))
graph.add((NS_OBO.BFO_0000197, RDFS.label, Literal("inheres in", lang="en")))
graph.add((NS_CCO.ont00001863, RDFS.label, Literal("uses measurement unit", lang="en")))
graph.add((NS_CCO.ont00001606, RDFS.label, Literal("Degree Celsius Measurement Unit", lang="en")))
graph.add((NS_CCO.ont00001724, RDFS.label, Literal("Degree Fahrenheit Measurement Unit", lang="en")))
graph.add((NS_CCO.ont00000120, RDFS.label, Literal("Measurement Unit", lang="en")))
graph.add((NS_CCO.ont00001904, RDFS.label, Literal("is measured by", lang="en")))
graph.add((NS_CCO.ont00001163, RDFS.label, Literal("Measurement Information Content Entity", lang="en")))
graph.add((NS_CCO.ont00001769, RDFS.label, Literal("has decimal value", lang="en")))
graph.add((NS_CCO.BFO_0000196, RDFS.label, Literal("bearer of", lang="en"))) 
graph.add((NS_CCO.ont00001961, RDFS.label, Literal("is measurement unit of", lang="en"))) 
graph.add((NS_CCO.ont00001966, RDFS.label, Literal("is a measurement of", lang="en")))
graph.add((NS_CCO.ont00001450, RDFS.label, Literal("Volt Measurement Unit", lang="en")))
graph.add((NS_CCO.ont00001559, RDFS.label, Literal("Pascal Measurement Unit", lang="en")))
graph.add((NS_CCO.ont00001694, RDFS.label, Literal("Pounds Per Square Inch Measurement Unit", lang="en")))

OBJECT_PROPERTY_DEFS = {
    NS_OBO.BFO_0000196: "b bearer of c =Def c inheres in b", 
    NS_CCO.ont00001904: "y is_measured_by x iff x is an instance of Information Content Entity and y is an instance of Entity, such that x describes some attribute of y relative to some scale or classification scheme.",
    NS_CCO.ont00001966: "x is_a_measurement_of y iff x is an instance of Measurement Information Content Entity and y is an instance of Specifically Dependent Continuant (a reading), such that x specifies a value describing some attribute of y relative to some scale or classification scheme.",
    NS_CCO.ont00001961: "x is_measurement_unit_of y iff x is an instance of Measurement Unit and y is an instance of Measurement Information Content Entity or Specifically Dependent Continuant, such that x describes or qualifies the magnitude of the measured physical quantity referenced in y.",
}
CLASS_DEFS = {
    NS_OBO.BFO_0000020: "A specifically dependent continuant is a continuant & there is some independent continuant c which is not a spatial region and which is such that b s-depends_on c at every time t during the course of b’s existence.",
    NS_OBO.BFO_0000031: "A generically dependent continuant is a continuant that g-depends_on one or more other entities.",
    NS_OBO.BFO_0000040: "A material entity is an independent continuant that has some portion of matter as proper or improper continuant part.",
}
DIFFERENTIA = {
    NS_CCO.ont00000995: "is intentionally produced to realize some function or purpose",  
    NS_CCO.ont00001163: "specifies a numeric measurement value together with its associated unit",  
    NS_CCO.ont00000120: "serves to standardize quantities for measurement information content entities and specifically dependent continuants",  
    NS_OBO.BFO_0000197: "relates a specifically dependent continuant to the independent continuant it inheres in",  
    NS_CCO.ont00001966: "links a measurement information content entity to the reading (specifically dependent continuant) that it specifies",
    NS_CCO.ont00001863: "links a measurement information content entity or a specifically dependent continuant to the unit that qualifies its value",
    NS_CCO.ont00001769: "associates a measurement information content entity with a numeric value literal",  
}

_defined_terms = set()

def article_for(s: str) -> str:
    return "An" if s[:1].lower() in ("a", "e", "i", "o", "u") else "A"

def label_or_localname(iri: URIRef) -> str:
    for _, _, lab in graph.triples((iri, RDFS.label, None)):
        if isinstance(lab, Literal) and (lab.language or "").lower().startswith("en") and str(lab).strip():
            return str(lab).strip()
    s = str(iri)
    local = s.rsplit("#", 1)[-1] if "#" in s else s.rsplit("/", 1)[-1]
    return local.replace("_", " ").replace("-", " ").strip()

def parent_of(term: URIRef):
    for _, _, parent in graph.triples((term, RDFS.subClassOf, None)):
        if isinstance(parent, URIRef):
            return parent
    return None

def ensure_definition(term_iri: URIRef, definition_text: str):
    if not definition_text or not str(definition_text).strip():
        definition_text = "Definition not provided."
    if term_iri not in _defined_terms and not has_english_definition(term_iri):
        graph.add((term_iri, RDFS.comment, Literal(definition_text.strip(), lang="en")))
        _defined_terms.add(term_iri)

def has_english_definition(term_iri: URIRef) -> bool:
    for _, _, c in graph.triples((term_iri, RDFS.comment, None)):
        if isinstance(c, Literal) and c.language and c.language.lower().startswith("en") and str(c).strip():
            return True
    return False

def make_non_self_referential(def_text: str, class_iri: URIRef) -> str:
    if not isinstance(def_text, str) or not def_text.strip():
        return def_text
    class_label = label_or_localname(class_iri).strip()
    iri_str = str(class_iri)
    local_token = iri_str.rsplit("#", 1)[-1] if "#" in iri_str else iri_str.rsplit("/", 1)[-1]
    local_token = local_token.strip()
    lower_def = def_text.lower()
    split_needles = (" is a ", " is an ")
    split_idx = -1
    needle_used = None
    for needle in split_needles:
        i = lower_def.find(needle)
        if i != -1:
            split_idx = i
            needle_used = def_text[i:i+len(needle)]  
            break
    if split_idx == -1:
        return def_text

    head = def_text[:split_idx + len(needle_used)]
    body = def_text[split_idx + len(needle_used):]

def _whole_word_replace(text: str, token: str, replacement: str) -> str:
        if not token:
            return text
        pattern = r'(?i)(^|[^A-Za-z0-9_])(' + re.escape(token) + r')([^A-Za-z0-9_]|$)'
        return re.sub(pattern, r'\1' + replacement + r'\3', text)

        new_body = _whole_word_replace(body, class_label, "this quality")
if local_token and local_token.lower() != class_label.lower():
        new_body = _whole_word_replace(new_body, local_token, "this class")
return head + new_body

def ensure_clean_definition(term_iri: URIRef, definition_text: str):
    
    is_class = any(graph.triples((term_iri, NS_RDF.type, NS_OWL.Class))) or any(graph.triples((term_iri, NS_RDF.type, RDFS.Class)))
    clean_text = make_non_self_referential(definition_text, term_iri) if is_class else definition_text
    ensure_definition(term_iri, clean_text)

def _slug(text: str) -> str:
    t = re.sub(r"[^A-Za-z0-9_]+", "-", (text or "")).strip("-")
    t = re.sub(r"-+", "-", t)
    return t or "na"
    
KIND_ALIASES = {
    "Temperature": {"temperature", "temp", "tmp", "t", "degc", "degf", "c", "f"},
    "Pressure": {"pressure", "press", "psi", "bar", "pa", "kpa"},
    "Humidity": {"humidity", "rh", "rel humidity", "relative humidity"},
    "Resistance": {"resistance", "res", "ohm", "ohms", "ω", "r"},
    "Voltage": {"voltage", "volt", "volts", "v"},
}
QUALITIES_FOR_CLASSING = {"Pressure", "Temperature", "Resistance", "Voltage"}

def canonicalize_kind(raw: str) -> tuple[str, str]:
    base = (raw or "").strip().lower()
    base = re.sub(r"[^a-z0-9]+", " ", base).strip()
    for canonical, forms in KIND_ALIASES.items():
        if base in forms:
            return canonical, _slug(canonical.lower())
    canon = base.title() if base else "Unknown"
    return canon, _slug(canon.lower())

UNIT_ALIASES = {
    "c": {"c", "degc", "celsius", "°c"},
    "f": {"f", "degf", "fahrenheit", "°f"},
    "pa": {"pa", "pascal"},
    "kpa": {"kpa", "kilopascal", "kilopascals"},
    "psi": {"psi"},
    "volt": {"v", "volt", "volts"},
    "ohm": {"ohm", "ohms", "ω", "omega"},
}

def _normalize_unit_token(u_raw: str) -> str:
    base = (u_raw or "").strip().lower()
    if base:
        for canon, forms in UNIT_ALIASES.items():
            if base in forms:
                return canon
    base2 = re.sub(r"[^a-z0-9]+", "", base)
    for canon, forms in UNIT_ALIASES.items():
        if base2 in {re.sub(r"[^a-z0-9]+", "", f) for f in forms}:
            return canon
    return base2 or "na"

def resolve_unit_and_value(unit_raw: str, value_in: float):
    canon = _normalize_unit_token(unit_raw)

    if canon == "c":
        return NS_cco.ont00001606, value_in, True
    if canon == "f":
        return NS_cco.ont00001724, value_in, True
    if canon == "kpa":
        return NS_cco.ont00001559, value_in * 1000.0, True
    if canon == "pa":
        return NS_cco.ont00001559, value_in, True
    if canon == "psi":
        return NS_cco.ont00001694, value_in, True
    if canon == "volt":
        return NS_cco.ont00001450, value_in, True
    if canon == "ohm":
        return URIRef(ex + "ohm"), value_in, False
    return URIRef(ex + _slug(unit_raw)), value_in, False

EXPLICIT_CLASSES = [
    NS_cco.ont00000441,
    NS_cco.ont00000995,
    NS_obo.BFO_0000020,
    NS_cco.ont00001163,
    NS_cco.ont00000120,
    NS_obo.BFO_0000040,
    NS_obo.BFO_0000031,
]
EXPLICIT_OBJECT_PROPS = [
    NS_obo.BFO_0000197, 
    NS_obo.BFO_0000196,
    NS_cco.ont00001966,
    NS_cco.ont00001904,
    NS_cco.ont00001863,
    NS_cco.ont00001961,
]
EXPLICIT_DATATYPE_PROPS = [
    NS_cco.ont00001769,
    exprop.hasTimestamp,
]
for k in EXPLICIT_CLASSES:
    graph.add((k, NS_RDF.type, NS_OWL.Class))
for p in EXPLICIT_OBJECT_PROPS:
    graph.add((p, NS_RDF.type, NS_OWL.ObjectProperty))
for p in EXPLICIT_DATATYPE_PROPS:
    graph.add((p, NS_RDF.type, NS_OWL.DatatypeProperty))

graph.add((exprop.hasTimestamp, RDFS.label, Literal("has timestamp", lang="en")))

for klass, desc in CLASS_DEFS.items():
    graph.add((klass, NS_RDF.type, NS_OWL.Class))
    ensure_definition(klass, desc)
for prop, desc in OBJECT_PROPERTY_DEFS.items():
    graph.add((prop, NS_RDF.type, NS_OWL.ObjectProperty))
    ensure_definition(prop, desc)
for prop, desc in DATATYPE_PROPERTY_DEFS.items():
    graph.add((prop, NS_RDF.type, NS_OWL.DatatypeProperty))
    ensure_definition(prop, desc)


graph.add((NS_obo.BFO_0000196, NS_OWL.inverseOf, NS_obo.BFO_0000197))
graph.add((NS_obo.BFO_0000197, NS_OWL.inverseOf, NS_obo.BFO_0000196))
graph.add((NS_cco.ont00001904, NS_OWL.inverseOf, NS_cco.ont00001966))
graph.add((NS_cco.ont00001966, NS_OWL.inverseOf, NS_cco.ont00001904))
graph.add((NS_cco.ont00001961, NS_OWL.inverseOf, NS_cco.ont00001863))
graph.add((NS_cco.ont00001863, NS_OWL.inverseOf, NS_cco.ont00001961))

reading_cache = {}
quality_class_cache = {}

df = pd.read_csv(CSV_PATH)
for _, row in df.iterrows():
    artifact_id_raw = str(row['artifact_id']).strip()
    sdc_kind_raw = str(row['sdc_kind']).strip()
    unit_raw = str(row['unit_label']).strip()
    timestamp_raw = str(row['timestamp']).strip()

    if not artifact_id_raw or not sdc_kind_raw or not unit_raw or not timestamp_raw:
        continue
    try:
        value = float(row['value'])
    except Exception:
        continue

    artifact_label = artifact_id_raw.replace(" ", "-")
    artifact_slug = _slug(artifact_label)
    canon_kind_label, canon_kind_slug = canonicalize_kind(sdc_kind_raw)
    ts_slug = _slug(timestamp_raw)
        
    artifact_uri = URIRef(ex + artifact_slug)
    graph.add((artifact_uri, NS_RDF.type, NS_cco.ont00000995))
    graph.add((artifact_uri, RDFS.label, Literal(artifact_label, lang="en")))

    if canon_kind_label in QUALITIES_FOR_CLASSING:
        if canon_kind_slug not in quality_class_cache:
            if canon_kind_label == "Temperature":
                qual_class_uri = NS_cco.ont00000441
                quality_class_cache[canon_kind_slug] = qual_class_uri
                graph.add((qual_class_uri, NS_RDF.type, NS_OWL.Class))
                graph.add((qual_class_uri, RDFS.label, Literal("Temperature", lang="en")))
            else:
                qual_class_uri = URIRef(exc + canon_kind_slug)
                quality_class_cache[canon_kind_slug] = qual_class_uri
                graph.add((qual_class_uri, NS_RDF.type, NS_OWL.Class))
                graph.add((qual_class_uri, RDFS.subClassOf, NS_obo.BFO_0000020))
                graph.add((qual_class_uri, RDFS.label, Literal(canon_kind_label, lang="en")))
                _qdef = (
                    f"{article_for(canon_kind_label)} {canon_kind_label} is a Specifically Dependent Continuant quality "
                    f"that can inhere in a material entity and is typically subject to measurement."
                )
                ensure_definition(qual_class_uri, _qdef)
        else:
            qual_class_uri = quality_class_cache[canon_kind_slug]
    else:
        qual_class_uri = None
        
    reading_key = (artifact_slug, canon_kind_slug, ts_slug)
    if reading_key in reading_cache:
        reading_uri = reading_cache[reading_key]
    else:
        reading_id = f"{artifact_slug}_{canon_kind_slug}_{ts_slug}"
        reading_uri = URIRef(ex + reading_id)
        reading_cache[reading_key] = reading_uri

    graph.add((reading_uri, NS_RDF.type, NS_obo.BFO_0000020))
    if qual_class_uri is not None:
        graph.add((reading_uri, NS_RDF.type, qual_class_uri))
    graph.add((reading_uri, RDFS.label, Literal(f"{artifact_label}_{canon_kind_label} @ {timestamp_raw}", lang="en")))
    graph.add((reading_uri, NS_obo.BFO_0000197, artifact_uri))
    unit_uri, value, is_external_unit = resolve_unit_and_value(unit_raw, value)
    graph.add((unit_uri, NS_RDF.type, NS_cco.ont00000120))
    if not is_external_unit:
        graph.add((unit_uri, RDFS.label, Literal(unit_raw, lang="en")))
    mice_id = f"MICE_{artifact_slug}_{canon_kind_slug}_{ts_slug}"
    mice_uri = URIRef(ex + mice_id)
    graph.add((mice_uri, NS_RDF.type, NS_cco.ont00001163))
    graph.add((mice_uri, RDFS.label, Literal(f"MICE for {artifact_label}_{canon_kind_label} @ {timestamp_raw}", lang="en")))
    graph.add((mice_uri, NS_cco.ont00001769, Literal(value, datatype=XSD.decimal)))
    graph.add((mice_uri, NS_cco.ont00001863, unit_uri))
    graph.add((reading_uri, NS_cco.ont00001863, unit_uri))
    graph.add((mice_uri, NS_cco.ont00001966, reading_uri))
    graph.add((reading_uri, NS_cco.ont00001904, mice_uri))
    graph.add((mice_uri, exprop.hasTimestamp, Literal(timestamp_raw, datatype=XSD.dateTime)))
ensure_clean_definition(
    NS_cco.ont00000441,
)
graph.add((NS_cco.ont00000441, RDFS.subClassOf, NS_obo.BFO_0000020))
graph.add((NS_cco.ont00000995, RDFS.subClassOf, NS_obo.BFO_0000040))
graph.add((NS_cco.ont00001163, RDFS.subClassOf, NS_obo.BFO_0000031))
graph.add((NS_cco.ont00000120, RDFS.subClassOf, NS_obo.BFO_0000031))
seen_classes = set()
for c in graph.subjects(NS_RDF.type, NS_OWL.Class):
    if isinstance(c, URIRef):
        seen_classes.add(c)
for _, _, class_iri in graph.triples((None, NS_RDF.type, None)):
    if isinstance(class_iri, URIRef):
        seen_classes.add(class_iri)
for c in seen_classes:
    if not has_english_definition(c):
        ensure_clean_definition(c, (
            (lambda term: (
                f"{article_for(label_or_localname(term))} {label_or_localname(term)} is a "
                f"{label_or_localname(parent_of(term)) if parent_of(term) else 'parent class (unspecified)'} that "
                f"{DIFFERENTIA.get(term), f'has not yet had its differentiating factor specified relative to {label_or_localname(parent_of(term)) if parent_of(term) else parent class (unspecified)'}')'}'.'}")
            )(c)
        ))

seen_object_properties = set()
for p in graph.subjects(NS_RDF.type, NS_OWL.ObjectProperty):
    if isinstance(p, URIRef):
        seen_object_properties.add(p)
for p in seen_object_properties:
    if not has_english_definition(p):
        a = label_or_localname(p)
        cmt = f"{article_for(a)} {a} is an object property that {DIFFERENTIA.get(p, 'has not yet had its differentiating factor specified')} ."
        ensure_definition(p, cmt)

seen_datatype_properties = set()
for p in graph.subjects(NS_RDF.type, NS_OWL.DatatypeProperty):
    if isinstance(p, URIRef):
        seen_datatype_properties.add(p)
for p in seen_datatype_properties:
    if not has_english_definition(p):
        a = label_or_localname(p)
        cmt = f"{article_for(a)} {a} is a datatype property that {DIFFERENTIA.get(p, 'has not yet had its differentiating factor specified')} ."
        ensure_definition(p, cmt)

graph.serialize(destination=OUT_FILE, format="turtle")


def generate_triples(df, graph):
    seen_static_entities = set()
    for _, row in df.iterrows():
        artifact_uri, sdc_uri, mu_uri, mv_uri, mice_uri = generate_uris(row)
        artifact_key = str(artifact_uri)
        sdc_key = str(sdc_uri)
        if artifact_key not in seen_static_entities:
            graph.add((artifact_uri, NS_RDF.type, IRI_ART)) 
            graph.add((artifact_uri, IRI_BEARER_OF, sdc_uri))
            graph.add((sdc_uri, NS_RDF.type, IRI_SDC))
            seen_static_entities.add(artifact_key)
            seen_static_entities.add(sdc_key)
        mu_key = str(mu_uri)
        if mu_key not in seen_static_entities:
            graph.add((mu_uri, NS_RDF.type, IRI_MU))
            seen_static_entities.add(mu_key)
        mv_key = str(mv_uri)
        if mv_key not in seen_static_entities:
            graph.add((mv_uri, NS_RDF.type, IRI_HAS_VALUE)) 
            
            graph.add((mv_uri, IRI_HAS_VALUE, Literal(row['value'], datatype=XSD.decimal)))
            seen_static_entities.add(mv_key)

        graph.add((mice_uri, NS_RDF.type, IRI_MICE))
        graph.add((mice_uri, IRI_IS_MEASURE_OF, sdc_uri))
        graph.add((mice_uri, IRI_USES_MU, mu_uri))
        graph.add((mice_uri, IRI_HAS_VALUE, mv_uri))
        graph.add((mice_uri, IRI_HAS_TIMESTAMP, Literal(row['timestamp'], datatype=XSD.dateTime)))

    return graph
    
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
        print(f"✅ TTL file saved successfully.")
    else:
        print("❌ TTL file was not saved!")
        
if __name__ == '__main__':
    main()

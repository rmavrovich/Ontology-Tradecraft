
   
import pandas as pd
import json
from dateutil import parser as dateparser
from pathlib import Path

IN_A = Path("src/data/sensor_A.csv")
IN_B = Path("src/data/sensor_B.json")
IN_C = Path("src/data/sensor_C.csv")  # THIS A THE NEW LINE
OUT  = Path("src/data/readings_normalized.csv")

def main():
    print("[paths] A:", IN_A)
    print("[paths] B:", IN_B)
    print("[paths] C:", IN_C)         # THIS A THE NEW LINE
    df_a = load_sensor_a(IN_A)
    df_b = load_sensor_b(IN_B)
    df_c = load_sensor_a(IN_C)        # THIS A THE NEW LINE

    print(f"[normalize_readings] Input A rows: {len(df_a)}")
    print(f"[normalize_readings] Input B rows: {len(df_b)}")
    print(f"[normalize_readings] Input C rows: {len(df_c)}")    # THIS A THE NEW LINE

    combined = pd.concat([df_a, df_b, df_c], ignore_index=True) # UPDATED LINE
    cleaned = normalize_and_clean(combined)
   
df_a = pd.read_csv(IN_A, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])
# Map columns to canonical names (EDIT to match the actual headers)
df_a = df_a.rename(columns={
    "asset_id": "artifact_id",
    "measure_type": "sdc_kind",
    "unit": "unit_label",
    "reading": "value",
    "time": "timestamp",
})
# Keep only canonical columns that exist
df_a = df_a[[c for c in ["artifact_id","sdc_kind","unit_label","value","timestamp"] if c in df_a.columns]]
   
raw_txt = Path(IN_B).read_text(encoding="utf-8").strip()
try:
    obj = json.loads(raw_txt)
    records = obj["records"] if isinstance(obj, dict) and "records" in obj else (obj if isinstance(obj, list) else [obj])
except json.JSONDecodeError:
    # NDJSON fallback
    records = [json.loads(line) for line in raw_txt.splitlines() if line.strip()]

df_b = pd.DataFrame([{
    "artifact_id": r.get("artifact") or r.get("asset") or r.get("artifact_id"),
    "sdc_kind":    r.get("sdc") or r.get("measure_type") or r.get("sdc_kind"),
    "unit_label":  r.get("uom") or r.get("unit") or r.get("unit_label"),
    "value":       r.get("val") or r.get("reading") or r.get("value"),
    "timestamp":   r.get("ts") or r.get("time") or r.get("timestamp"),
} for r in records])
   

   
df = pd.concat([df_a, df_b], ignore_index=True)
   

   
for col in ["artifact_id","sdc_kind","unit_label"]:
    df[col] = df[col].astype(str).str.strip()

# numeric
df["value"] = pd.to_numeric(df["value"], errors="coerce")
   
def to_iso8601(x):
    try:
        # auto-detect; if timezone missing, assume UTC
        dt = dateparser.parse(str(x))
        if dt.tzinfo is None:
            # You can choose a policy; here we treat naive as UTC
            import datetime, pytz
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        return None

df["timestamp"] = df["timestamp"].apply(to_iso8601)
   

   
UNIT_MAP = {
    "celsius": "C", "°c": "C", "C": "C",
    "kilogram": "kg", "KG": "kg", "kg": "kg",
    "meter": "m", "M": "m", "m": "m",
    "fahrenheit": "F", "f": "F", "°f":"F", "F":"F",
    "kilopascal": "kPa", "KPA": "kPa", "kpa":"kPa", "kPa": "kPa",
    "voltage": "V", "v": "V", "V": "V", 
    "ohm": "Ω", "omega":"Ω", "Ω":"Ω"


}
df["unit_label"] = df["unit_label"].str.lower().map(UNIT_MAP).fillna(df["unit_label"])
   


   
df = df.dropna(subset=["artifact_id","sdc_kind","unit_label","value","timestamp"])
   


   
df = df.sort_values(["artifact_id", "timestamp"]).reset_index(drop=True)
   


    
OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)
print(f"Wrote {OUT} with {len(df)} rows.")
  
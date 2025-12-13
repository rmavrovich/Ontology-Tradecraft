import pandas as pd
import json
from dateutil import parser as dateparser
from pathlib import Path
import datetime
import sys
import io

# --- INPUT/OUTPUT PATHS (Confirmed correct for your workflow) ---
IN_A = Path("src/data/sensor_A.csv") 
IN_B = Path("src/data/sensor_B.json")
IN_C = Path("src/data/sensor_C.csv")
OUT  = Path("src/data/readings_normalized.csv")
# ----------------------------------------------------------------------------------

def load_sensor_a(file_path):
    """
    Loads data from a CSV, renames columns to canonical names, and selects them.
    """
    df_a = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])
    
    # Map columns to canonical names
    df_a = df_a.rename(columns={
        "Device Name": "artifact_id",
        "Reading Type": "sdc_kind",
        "Units": "unit_label",
        "Reading Value": "value",
        "Time (Local)": "timestamp",
        # Fallback names
        "asset_id": "artifact_id", "measure_type": "sdc_kind", "unit": "unit_label", 
        "reading": "value", "time": "timestamp",
    })
    
    # Keep only canonical columns
    canonical_cols = ["artifact_id", "sdc_kind", "unit_label", "value", "timestamp"]
    df_a = df_a[[c for c in canonical_cols if c in df_a.columns]]
    return df_a

def load_sensor_b(file_path):
    """
    Loads data from a JSON/NDJSON file and maps keys to canonical names.
    (Fixed for nested structure parsing)
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_txt = f.read().strip()
    
    records = []
    try:
        obj = json.loads(raw_txt)
        
        # Handle the nested structure from your uploaded JSON file
        if isinstance(obj, dict) and "readings" in obj and isinstance(obj["readings"], list):
             for reading_group in obj["readings"]:
                 entity_id = reading_group.get("entity_id")
                 for data_record in reading_group.get("data", []):
                     records.append({
                         "artifact_id": entity_id, 
                         "sdc_kind": data_record.get("kind"),
                         "unit_label": data_record.get("unit"),
                         "value": data_record.get("value"),
                         "timestamp": data_record.get("time")
                     })
             return pd.DataFrame([r for r in records if r.get('artifact_id') is not None])

        # Fallback to original logic for standard/flat JSON structure
        records = obj.get("records", obj) if isinstance(obj, dict) else (obj if isinstance(obj, list) else [obj])
    except json.JSONDecodeError:
        # NDJSON fallback
        records = [json.loads(line) for line in raw_txt.splitlines() if line.strip()]

    if isinstance(records, dict): records = [records]
    elif not isinstance(records, list): records = [records]
        
    df_b = pd.DataFrame([{
        "artifact_id": r.get("artifact") or r.get("asset") or r.get("artifact_id"),
        "sdc_kind":    r.get("sdc") or r.get("measure_type") or r.get("sdc_kind"),
        "unit_label":  r.get("uom") or r.get("unit") or r.get("unit_label"),
        "value":       r.get("val") or r.get("reading") or r.get("value"),
        "timestamp":   r.get("ts") or r.get("time") or r.get("timestamp"),
    } for r in records if isinstance(r, dict)])

    return df_b

def to_iso8601(x):
    """
    Converts a timestamp to ISO8601 format, assuming UTC if timezone is missing.
    """
    try:
        dt = dateparser.parse(str(x))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        return None

def normalize_and_clean(df):
    """
    Performs data type conversion, standardization, unit mapping, and cleaning.
    """
    # String cleaning
    for col in ["artifact_id", "sdc_kind", "unit_label"]:
        df[col] = df[col].astype(str).str.strip()

    # Numeric conversion
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Timestamp standardization
    df["timestamp"] = df["timestamp"].apply(to_iso8601)

    # Unit mapping standardization
    UNIT_MAP = {
        "celsius": "C", "°c": "C", "C": "C",
        "kilogram": "kg", "KG": "kg", "kg": "kg",
        "meter": "m", "M": "m", "m": "m",
        "fahrenheit": "F", "f": "F", "°f":"F", 
        # FINAL FIX: Map all kPa and psi forms to the canonical unit 'Pa' (Pascal)
        "kilopascal": "Pa", "KPA": "Pa", "kpa": "Pa", "kPa": "Pa",
        "psi": "Pa", "PSI": "Pa", 
        "voltage": "V", "v": "V", "V": "V",
        "ohm": "Ω", "omega": "Ω", "Ω": "Ω",
        "volt": "V"
    }
    df["unit_label"] = df["unit_label"].astype(str).str.lower().map(UNIT_MAP).fillna(df["unit_label"])
    
    
    # ==========================================================
    # <<< DIAGNOSTIC STATEMENTS >>>
    # ==========================================================
    
    print(f"\n[DIAGNOSTICS] Total rows before dropping: {len(df)}")
    print("[DIAGNOSTICS] Null/Missing values per column:")
    print(df.isnull().sum())
    
    # ==========================================================
    
    # Drop rows with missing critical data
    df = df.dropna(subset=["artifact_id", "sdc_kind", "unit_label", "value", "timestamp"])
    
    # Sort and reset index
    df = df.sort_values(["artifact_id", "timestamp"]).reset_index(drop=True)
    
    return df

def main():
    print(f"[paths] A: {IN_A}")
    print(f"[paths] B: {IN_B}")
    print(f"[paths] C: {IN_C}")
    
    if not IN_A.exists() or not IN_B.exists() or not IN_C.exists():
        print("ERROR: One or more input files were not found at the expected path.")
        print(f"Please check that files exist at: {IN_A.parent.resolve()}")
        df_a, df_b, df_c = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    else:
        # Load data from the files
        df_a = load_sensor_a(IN_A)
        df_b = load_sensor_b(IN_B)
        df_c = load_sensor_a(IN_C)
    
    print(f"[normalize_readings] Input A rows loaded: {len(df_a)}")
    print(f"[normalize_readings] Input B rows loaded: {len(df_b)}")
    print(f"[normalize_readings] Input C rows loaded: {len(df_c)}")

    # Combine all DataFrames
    combined = pd.concat([df_a, df_b, df_c], ignore_index=True)

    # Normalize and clean the combined data
    cleaned = normalize_and_clean(combined)
   
    # Ensure the output directory exists
    OUT.parent.mkdir(parents=True, exist_ok=True) 
    
    # Write the final output
    print(f"\nWriting {OUT} with {len(cleaned)} rows.")
    cleaned.to_csv(OUT, index=False)
    
    return cleaned

if __name__ == '__main__':
    main()

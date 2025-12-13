import pandas as pd
import json
from dateutil import parser as dateparser
from pathlib import Path
import datetime
import sys
import io

# --- INPUT/OUTPUT PATHS (Use relative paths for GitHub Workflow) ---
# NOTE: These paths MUST exist relative to where your workflow executes the script
IN_A = Path("src/data/sensor_A.csv") 
IN_B = Path("src/data/sensor_B.json")
IN_C = Path("src/data/sensor_C.csv")
OUT  = Path("src/data/readings_normalized.csv")
# ------------------------------------------------------------------

def load_sensor_a(file_path):
    """
    Loads data from a CSV, renames columns based on common headers, and selects canonical ones.
    (Assumes column names like 'Device Name', 'Reading Value', etc., from your uploaded files)
    """
    df_a = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])
    
    # Map columns to canonical names
    df_a = df_a.rename(columns={
        "Device Name": "artifact_id",
        "Reading Type": "sdc_kind",
        "Units": "unit_label",
        "Reading Value": "value",
        "Time (Local)": "timestamp",
        # Original script's column names (kept as fallback, though unlikely for your data)
        "asset_id": "artifact_id",
        "measure_type": "sdc_kind",
        "unit": "unit_label",
        "reading": "value",
        "time": "timestamp",
    })
    
    # Keep only canonical columns
    canonical_cols = ["artifact_id", "sdc_kind", "unit_label", "value", "timestamp"]
    df_a = df_a[[c for c in canonical_cols if c in df_a.columns]]
    return df_a

def load_sensor_b(file_path):
    """
    Loads data from a JSON/NDJSON file (sensor B) and maps keys to canonical names.
    (Includes nested structure parsing based on your 'sensor_B copia.json' file)
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_txt = f.read().strip()
    
    records = []
    try:
        obj = json.loads(raw_txt)
        
        # FIX: Handle the nested structure of the uploaded 'sensor_B copia.json'
        if isinstance(obj, dict) and "readings" in obj and isinstance(obj["readings"], list):
             for reading_group in obj["readings"]:
                 entity_id = reading_group.get("entity_id")
                 for data_record in reading_group.get("data", []):
                     # Flatten the nested structure
                     records.append({
                         "artifact_id": entity_id, 
                         "sdc_kind": data_record.get("kind"),
                         "unit_label": data_record.get("unit"),
                         "value": data_record.get("value"),
                         "timestamp": data_record.get("time")
                     })
             return pd.DataFrame([r for r in records if r.get('artifact_id') is not None])

        # Fallback to original logic if nested parsing failed
        records = obj.get("records", obj) if isinstance(obj, dict) else (obj if isinstance(obj, list) else [obj])
    except json.JSONDecodeError:
        # NDJSON fallback
        records = [json.loads(line) for line in raw_txt.splitlines() if line.strip()]

    # Ensure records is a list for iteration
    if isinstance(records, dict): records = [records]
    elif not isinstance(records, list): records = [records]
        
    # Original key mapping logic (used only if nested parsing above didn't return)
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
            # Policy: treat naive as UTC
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
        "celsius": "C", "°c": "C", "C": "C", "kilogram": "kg", "KG": "kg", 
        "meter": "m", "M": "m", "m": "m", "fahrenheit": "F", "f": "F", "°f":"F", 
        "kilopascal": "kPa", "KPA": "kPa", "kpa":"kPa", "kPa": "kPa",
        "voltage": "V", "v": "V", "V": "V", "ohm": "Ω", "omega":"Ω", "Ω":"Ω",
        "volt": "V", "psi": "psi" # Added units from sample data
    }
    df["unit_label"] = df["unit_label"].astype(str).str.lower().map(UNIT_MAP).fillna(df["unit_label"])
    
    
    # ==========================================================
    # <<< START: DIAGNOSTIC STATEMENTS (FOR DEBUGGING) >>>
    # ==========================================================
    
    # 1. Print the total number of rows before dropping
    print(f"\n[DIAGNOSTICS] Total rows before dropping: {len(df)}")
    
    # 2. Print the count of missing (NaN) values for *every* column
    print("[DIAGNOSTICS] Null/Missing values per column:")
    print(df.isnull().sum())
    
    # ==========================================================
    # <<< END: DIAGNOSTIC STATEMENTS >>>
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
    
    # Check if input files exist (critical for workflow debugging)
    if not IN_A.exists() or not IN_B.exists() or not IN_C.exists():
        print("ERROR: One or more input files were not found at the expected path.")
        print("Please ensure your GitHub workflow checked out the data and the paths in the script match the paths in the workflow.")
        # Create empty DataFrames to prevent further errors if files are missing
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
    
    print("\nFirst 5 rows of the final normalized data:")
    print(cleaned.head().to_markdown(index=False, numalign="left", stralign="left"))
    
    return cleaned

if __name__ == '__main__':
    # Added a check to help diagnose missing file paths in a workflow
    if not IN_A.exists() or not IN_B.exists() or not IN_C.exists():
        print("NOTE: Script running locally or in an environment where file paths might be dynamic.")
        # When running in an environment like this, the explicit file loading needs
        # to be handled outside of this script's Path logic if files aren't physically present.
        # However, for a GitHub workflow, the Path logic is correct if files are checked out.
    
    main()  

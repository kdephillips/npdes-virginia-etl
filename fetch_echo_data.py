import requests
import pandas as pd
from datetime import datetime
import os
import time

# filters
STATE = "VA"
OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Step 1: Get facilities file
print("Fetching Virginia NPDES facilities...")

search_response = requests.get(
    "https://echodata.epa.gov/echo/cwa_rest_services.get_facilities",
    params={"p_st": STATE, "p_act": "Y", "p_ptype": "NPD", "output": "JSON"}
)

search_json = search_response.json()
query_id = search_json["Results"]["QueryID"]

detail_response = requests.get(
    "https://echodata.epa.gov/echo/cwa_rest_services.get_qid",
    params={"qid": query_id, "output": "JSON", "p_qpages": "1"}
)

facility_data = detail_response.json()["Results"]["Facilities"]
facility_df = pd.DataFrame(facility_data)
facility_df.to_csv(os.path.join(OUTPUT_DIR, "facilities.csv"), index=False)
print(f" {len(facility_df)} facilities saved")

# Step 2: Load Historical DMR and Limits files
print("\nLoading historical DMR data from manual downloads...")

historical_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith("npdes_dmr_fy")]

if historical_files:
    dfs = []
    for file in historical_files:
        df = pd.read_csv(os.path.join(OUTPUT_DIR, file), dtype=str, low_memory=False)
        dfs.append(df)
        print(f"  ✓ Loaded {file} — {len(df)} records")
    historical_dmr = pd.concat(dfs, ignore_index=True)
else:
    print("  ⚠ No historical files found — place Virginia DMR CSVs in data/raw/")
    historical_dmr = pd.DataFrame()

# Limits Section
print("\nLoading historical Limits data from manual downloads...")

limits_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith("npdes_limits_fy")]

if limits_files:
    limit_dfs = []
    for file in limits_files:
        df = pd.read_csv(os.path.join(OUTPUT_DIR, file), dtype=str, low_memory=False)
        limit_dfs.append(df)
        print(f"  ✓ Loaded {file} — {len(df)} records")
    historical_limits = pd.concat(limit_dfs, ignore_index=True)
    historical_limits.to_csv(os.path.join(OUTPUT_DIR, "limits_data.csv"), index=False)
    print(f"  ✓ Combined limits saved — {len(historical_limits)} total records")
else:
    print("  No limits files found in data/raw/")

# Step 3: API Refresh babyyy
print("\nFetching recent DMR records via API...")

# Filter to VPDES IPs
va00_permits = facility_df[
    facility_df["SourceID"].str.startswith("VA00", na=False)
]["SourceID"].dropna().head(10).tolist()  # started with 10, will expand later

new_records = []

for permit_id in va00_permits:
    params = {
        "p_id": permit_id.upper(),
        "output": "JSON",
        "p_start_date": "01/01/2025",   # only pull what's newer than bulk download
        "p_end_date": datetime.today().strftime("%m/%d/%Y")
    }

    try:
        r = requests.get(
            "https://echodata.epa.gov/echo/eff_rest_services.get_effluent_chart",
            params=params,
            timeout=30
        )
        data = r.json()
        records = data.get("Results", {}).get("EffluentCharts", [])

        if records:
            new_records.extend(records)
            print(f"  {permit_id}: {len(records)} new records")
        else:
            print(f" — {permit_id}: no recent records")

    except Exception as e:
        print(f"  {permit_id}: {e}")

    time.sleep(0.5)

# ── Step 4: Combine and Save ───────────────────────────────
print("\nCombining historical + new data...")

if new_records:
    new_df = pd.DataFrame(new_records)
    combined_dmr = pd.concat([historical_dmr, new_df], ignore_index=True)
else:
    combined_dmr = historical_dmr

# Save DMR data
combined_dmr.to_csv(os.path.join(OUTPUT_DIR, "dmr_data.csv"), index=False)
print(f"  ✓ Final DMR dataset: {len(combined_dmr)} total records")
print(f"  DMR Columns: {list(combined_dmr.columns[:8])}...")

# Save Limits data separately
if 'historical_limits' in dir() and not historical_limits.empty:
    historical_limits.to_csv(os.path.join(OUTPUT_DIR, "limits_data.csv"), index=False)
    print(f"  ✓ Final Limits dataset: {len(historical_limits)} total records")
    print(f"  Limits Columns: {list(historical_limits.columns[:8])}...")
else:
    print("  ⚠ No limits data to save — check Step 2")
# Step 5: Refresh the log
with open("data/refresh_log.txt", "a") as log:
    log.write(f"{datetime.now()} — Facilities: {len(facility_df)}, DMR records: {len(combined_dmr)}, Limit records: {len(historical_limits)}\n")

print("\nDone!")
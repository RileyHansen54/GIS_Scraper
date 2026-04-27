import pandas as pd
import requests
import time
import os

# Pointing to the EnerGov database where the real estate metrics live
ARCGIS_URL = "https://gis.beaufortcountysc.gov/server/rest/services/EnerGov/MapServer/1/query"

# All the useful fields available on this layer
OUT_FIELDS = ",".join([
    "GisFile_SitusAddre",
    "GisFile_Appraised",
    "GisFile_Owner1",
    "GisFile_Acres",
    "GisFile_ResSquareF",
    "GisFile_Bldgs",
    "GisFile_MinYrBuilt",
    "GisFile_MaxYrBuilt",
    "GisFile_Land",
    "GisFile_Improvemen",
    "GisFile_ClassCode",
    "GisFile_SaleDate",
    "GisFile_SalePrice",
    "GisFile_LegalDescr",
])

# Mapping from GIS field names to clean column names for the CSV
FIELD_MAP = {
    "GisFile_SitusAddre": "Official_Address",
    "GisFile_Appraised":  "Appraised_Value",
    "GisFile_Owner1":     "GIS_Owner",
    "GisFile_Acres":      "Acres",
    "GisFile_ResSquareF":  "Res_SqFt",
    "GisFile_Bldgs":      "Bldg_Count",
    "GisFile_MinYrBuilt": "Year_Built_Min",
    "GisFile_MaxYrBuilt": "Year_Built_Max",
    "GisFile_Land":       "Land_Value",
    "GisFile_Improvemen": "Improvement_Value",
    "GisFile_ClassCode":  "Property_Class",
    "GisFile_SaleDate":   "Last_Sale_Date",
    "GisFile_SalePrice":  "Last_Sale_Price",
    "GisFile_LegalDescr": "Legal_Desc",
}


def query_arcgis(pin):
    """Queries the REST API with built-in retry logic for slow government servers."""

    max_retries = 3

    for field in ["GisFile_PIN", "ParcelPIN"]:
        params = {
            "where": f"{field} = '{pin}'",
            "outFields": OUT_FIELDS,
            "f": "json",
        }

        for attempt in range(max_retries):
            try:
                response = requests.get(ARCGIS_URL, params=params, timeout=20)

                if response.status_code != 200:
                    print(f"⚠️ Server returned status {response.status_code}, retrying...")
                    time.sleep(3)
                    continue

                data = response.json()

                if data.get("features") and len(data["features"]) > 0:
                    return data["features"][0]["attributes"]

                break  # Successful but empty — try next field name

            except requests.exceptions.ReadTimeout:
                wait_time = 3 * (attempt + 1)
                print(f"⏳ Server timed out. Retrying in {wait_time}s (Attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait_time)
            except Exception as e:
                print(f"❌ Connection error for {pin}: {e}")
                break

    return None


def _clean_value(val):
    """Return a cleaned value — convert 'None'/null-like strings to empty."""
    if val is None:
        return ""
    s = str(val).strip()
    if s.lower() in ("null", "none", "nan"):
        return ""
    return s


def enrich_data(input_csv="auction_list.csv", output_csv="enriched_list.csv"):
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"❌ {input_csv} not found. Run ingest.py first!")
        return

    if os.path.exists(output_csv):
        os.remove(output_csv)

    print(f"🚀 Starting GIS Enrichment for {len(df)} properties...")
    print(f"   Pulling {len(FIELD_MAP)} fields per parcel\n")

    start_time = time.time()
    successful_hits = 0
    skipped = 0

    for index, row in df.iterrows():
        pin = str(row["pin"]).strip()

        # Skip Mobile Homes
        if pin.startswith("M"):
            skipped += 1
            continue

        print(f"[{index + 1}/{len(df)}] 🔍 PIN: {pin}...", end=" ")

        geo_data = query_arcgis(pin)

        if geo_data:
            has_address = False
            for gis_key, col_name in FIELD_MAP.items():
                raw = _clean_value(geo_data.get(gis_key))
                row[col_name] = raw

            addr = str(row.get("Official_Address", "")).strip()
            if addr and addr.lower() not in ("", "n/a"):
                has_address = True

            if has_address:
                successful_hits += 1
                print("✅ Found!")
            else:
                row["Official_Address"] = "N/A"
                print("⚠️ Found PIN, but Address is empty")
        else:
            for col_name in FIELD_MAP.values():
                row[col_name] = ""
            row["Official_Address"] = "N/A"
            print("❌ No Match in GIS")

        # Write row immediately so progress isn't lost on crash
        row_df = pd.DataFrame([row])
        write_header = not os.path.exists(output_csv)
        row_df.to_csv(output_csv, mode="a", header=write_header, index=False)

        time.sleep(0.3)

    elapsed = time.time() - start_time
    total = len(df) - skipped

    print("\n" + "=" * 45)
    print("💎 Enrichment Complete!")
    print(f"⏱️  Total Time: {int(elapsed // 60)}m {int(elapsed % 60)}s")
    print(f"🎯 Success Rate: {successful_hits}/{total} properties found")
    print(f"🏠 Skipped (Mobile Homes): {skipped}")
    print(f"📁 Saved to: {output_csv}")
    print("=" * 45)


if __name__ == "__main__":
    enrich_data()
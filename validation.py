import pandas as pd
import numpy as np
import urllib.parse

def calculate_alpha(input_csv="enriched_list.csv", output_csv="FINAL_INVESTOR_REPORT.csv"):
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"❌ {input_csv} not found. Run enrich.py first!")
        return

    # Initialize scraper columns for future automation
    for col in ["Zillow_Val", "Redfin_Val", "Realtor_Val"]:
        if col not in df.columns:
            df[col] = np.nan

    print("📊 Calculating Consensus Valuation, Risk Flags, & Tooling Links...\n")

    def safe_num(val, default=0):
        """Coerce anything to a number, falling back to default."""
        n = pd.to_numeric(val, errors="coerce")
        return default if pd.isna(n) else n

    def process_row(row):
        # ── 1. Clean Address ──────────────────────────────────────────────
        raw_addr = str(row.get("Official_Address", ""))
        if pd.isna(row.get("Official_Address")) or raw_addr.lower() in ("nan", "n/a", ""):
            clean_addr = "N/A"
        else:
            clean_addr = " ".join(raw_addr.split())
        row["Official_Address"] = clean_addr

        # ── 2. Parse Core Numbers ─────────────────────────────────────────
        appraised   = safe_num(row.get("Appraised_Value"))
        bid         = safe_num(row.get("bid"))
        land_val    = safe_num(row.get("Land_Value"))
        improv_val  = safe_num(row.get("Improvement_Value"))
        acres       = safe_num(row.get("Acres"))
        sqft        = safe_num(row.get("Res_SqFt"))
        bldg_count  = safe_num(row.get("Bldg_Count"))
        yr_built    = safe_num(row.get("Year_Built_Min"))
        last_sale   = safe_num(str(row.get("Last_Sale_Price", "")).replace("$", "").replace(",", ""))
        prop_class  = str(row.get("Property_Class", "")).strip()

        row["Appraised_Value"]    = appraised
        row["Land_Value"]         = land_val
        row["Improvement_Value"]  = improv_val

        # ── 3. Derived Metrics ────────────────────────────────────────────
        # Price per sq ft (bid vs structure size)
        if sqft > 0 and bid > 0:
            row["Bid_Per_SqFt"] = round(bid / sqft, 2)
        else:
            row["Bid_Per_SqFt"] = 0

        # Price per acre
        if acres > 0 and bid > 0:
            row["Bid_Per_Acre"] = round(bid / acres, 2)
        else:
            row["Bid_Per_Acre"] = 0

        # Land-to-total ratio (high ratio = structure is worthless)
        if appraised > 0:
            row["Land_Pct_of_Appraised"] = round((land_val / appraised) * 100, 1)
        else:
            row["Land_Pct_of_Appraised"] = 0

        # ── 4. Consensus Market Value ─────────────────────────────────────
        z_val   = safe_num(row.get("Zillow_Val"), default=np.nan)
        r_val   = safe_num(row.get("Redfin_Val"), default=np.nan)
        real_val = safe_num(row.get("Realtor_Val"), default=np.nan)

        market_comps = [v for v in [z_val, r_val, real_val] if pd.notna(v) and v > 0]

        if len(market_comps) > 0:
            avg_mv = sum(market_comps) / len(market_comps)
            row["Avg_Estimated_MV"] = round(avg_mv, 2)
            row["MV_Source"] = f"{len(market_comps)} scrapers"
        elif last_sale > 0:
            # Second-best fallback: last actual sale price
            avg_mv = last_sale
            row["Avg_Estimated_MV"] = last_sale
            row["MV_Source"] = "Last Sale"
        else:
            # Final fallback: county appraised
            avg_mv = appraised
            row["Avg_Estimated_MV"] = appraised
            row["MV_Source"] = "County Appraised"

        # ── 5. Equity & ROI ───────────────────────────────────────────────
        row["Equity_Potential"] = round(avg_mv - bid, 2)
        row["ROI_Potential_Pct"] = round((row["Equity_Potential"] / bid) * 100, 2) if bid > 0 else 0

        # ── 6. Risk Flags ─────────────────────────────────────────────────
        warnings = []

        if clean_addr == "N/A":
            warnings.append("No Address")

        if bldg_count == 0:
            warnings.append("Vacant Land (0 Buildings)")
        elif improv_val == 0 and bldg_count > 0:
            warnings.append("Structure Valued at $0")

        if appraised > 0 and (land_val / appraised) > 0.90 and bldg_count > 0:
            warnings.append("Land >90% of Value (Structure Worthless)")

        if yr_built > 0 and yr_built < 1950:
            warnings.append(f"Very Old ({int(yr_built)}) — Foundation/Wiring Risk")
        elif yr_built > 0 and yr_built < 1978:
            warnings.append(f"Pre-1978 ({int(yr_built)}) — Lead Paint Risk")

        if acres > 0 and acres < 0.05:
            warnings.append(f"Tiny Lot ({acres} acres)")

        if sqft > 0 and bid > 0 and (bid / sqft) < 5:
            warnings.append(f"Bid <$5/sqft — Verify Condition")

        if row["ROI_Potential_Pct"] > 5000:
            warnings.append("Insane ROI — Verify Value")

        if appraised < 5000 and bldg_count == 0:
            warnings.append("Low-Value Vacant (Possible Dirt/Swamp)")

        if row["MV_Source"] == "County Appraised":
            warnings.append("MV = County Only (No Market Comps)")

        row["Risk_Flags"] = " | ".join(warnings) if warnings else "Clean"

        # ── 7. Quick Classification ───────────────────────────────────────
        if bldg_count == 0:
            row["Deal_Type"] = "Land"
        elif sqft > 0 and sqft < 800:
            row["Deal_Type"] = "Small Residential"
        elif sqft >= 800:
            row["Deal_Type"] = "Residential"
        else:
            row["Deal_Type"] = "Unknown"

        # ── 8. Tooling Links ──────────────────────────────────────────────
        if clean_addr != "N/A":
            search_query = urllib.parse.quote(f"{clean_addr} Beaufort SC")
            hyphen_addr = clean_addr.replace(" ", "-")

            row["Zillow_Link"]  = f"https://www.zillow.com/homes/{hyphen_addr}-Beaufort-SC_rb/"
            row["Realtor_Link"] = f"https://www.realtor.com/realestateandhomes-search/Beaufort_SC/{hyphen_addr}"
            row["Redfin_Link"]  = f"https://www.google.com/search?q={search_query}+redfin"
            row["County_GIS"]   = f"https://gis.beaufortcountysc.gov/server/rest/services/EnerGov/MapServer/1/query?where=GisFile_PIN%3D%27{row.get('pin', '')}%27&outFields=*&f=json"
        else:
            row["Zillow_Link"]  = "N/A"
            row["Realtor_Link"] = "N/A"
            row["Redfin_Link"]  = "N/A"
            row["County_GIS"]   = "N/A"

        return row

    df = df.apply(process_row, axis=1)

    # ── 9. Smart Sort ─────────────────────────────────────────────────────
    df["Is_Clean"] = df["Risk_Flags"] == "Clean"
    df = df.sort_values(by=["Is_Clean", "Equity_Potential"], ascending=[False, False])

    # ── 10. Final Column Order ────────────────────────────────────────────
    cols_to_keep = [
        # Identity
        "owner", "pin", "Official_Address", "Property_Class", "Deal_Type",
        # Financials
        "bid", "Appraised_Value", "Land_Value", "Improvement_Value",
        "Last_Sale_Price", "Last_Sale_Date",
        # Property Details
        "Acres", "Res_SqFt", "Bldg_Count", "Year_Built_Min", "Year_Built_Max",
        # Derived Metrics
        "Bid_Per_SqFt", "Bid_Per_Acre", "Land_Pct_of_Appraised",
        # Valuation
        "Zillow_Val", "Redfin_Val", "Realtor_Val",
        "Avg_Estimated_MV", "MV_Source",
        "Equity_Potential", "ROI_Potential_Pct",
        # Risk
        "Risk_Flags",
        # Links
        "Zillow_Link", "Redfin_Link", "Realtor_Link", "County_GIS",
        # Misc
        "Legal_Desc",
    ]

    final_cols = [c for c in cols_to_keep if c in df.columns]
    df = df[final_cols]

    df.to_csv(output_csv, index=False)

    # ── Summary Stats ─────────────────────────────────────────────────────
    clean_count = (df["Risk_Flags"] == "Clean").sum()
    land_count  = (df["Deal_Type"] == "Land").sum() if "Deal_Type" in df.columns else 0
    res_count   = df["Deal_Type"].str.contains("Residential", na=False).sum() if "Deal_Type" in df.columns else 0

    print("=" * 50)
    print("💎 FINAL INVESTOR REPORT READY")
    print("=" * 50)
    print(f"📊 Total Properties:   {len(df)}")
    print(f"✅ Clean Deals:        {clean_count}")
    print(f"🏠 Residential:        {res_count}")
    print(f"🌿 Vacant Land:        {land_count}")
    print(f"📁 Saved to: {output_csv}")
    print("=" * 50)


if __name__ == "__main__":
    calculate_alpha()
import pandas as pd
import requests
import time
import os

# ── Configuration ─────────────────────────────────────────────────────────────
# Actor: one-api/zillow-scrape-address-url-zpid
# Docs: https://apify.com/one-api/zillow-scrape-address-url-zpid
ACTOR_ID = "HGPHGu8INtQpCeF3x"
APIFY_BASE = "https://api.apify.com/v2"

# Set your token here or export APIFY_TOKEN in your shell
APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "YOUR_TOKEN_HERE")

# ── CHANGE THIS when porting to a different county ────────────────────────────
LOCATION_SUFFIX = "Beaufort, SC"

# How long to wait for the Apify run to finish (seconds)
MAX_WAIT = 600
POLL_INTERVAL = 10


def start_actor_run(addresses):
    """Kick off the Apify actor with a list of full addresses."""
    url = f"{APIFY_BASE}/acts/{ACTOR_ID}/runs?token={APIFY_TOKEN}"

    # Append city/state so Zillow can resolve the property
    full_addrs = [f"{a}, {LOCATION_SUFFIX}" for a in addresses]
    addr_block = "\n".join(full_addrs)
    payload = {
        "multiple_input_box": addr_block,
    }

    resp = requests.post(url, json=payload, timeout=30)

    if resp.status_code != 201:
        print(f"\n   🔍 DEBUG — Status: {resp.status_code}")
        print(f"   🔍 DEBUG — Response body:\n{resp.text[:1000]}")
        resp.raise_for_status()

    data = resp.json()["data"]
    return data["id"], data["defaultDatasetId"]


def wait_for_run(run_id):
    """Poll until the actor run finishes or times out."""
    url = f"{APIFY_BASE}/actor-runs/{run_id}?token={APIFY_TOKEN}"
    elapsed = 0

    while elapsed < MAX_WAIT:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        status = resp.json()["data"]["status"]

        if status == "SUCCEEDED":
            return True
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"❌ Apify run ended with status: {status}")
            return False

        print(f"⏳ Run status: {status} ({elapsed}s elapsed)...")
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    print(f"❌ Timed out after {MAX_WAIT}s waiting for Apify run.")
    return False


def fetch_dataset(dataset_id):
    """Download all items from the run's dataset."""
    url = f"{APIFY_BASE}/datasets/{dataset_id}/items?token={APIFY_TOKEN}&format=json"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def normalize_address(addr):
    """Lowercase, collapse whitespace, strip punctuation for fuzzy matching."""
    if not addr or str(addr).lower() in ("n/a", "nan", ""):
        return ""
    return " ".join(str(addr).lower().replace(",", "").replace(".", "").split())


def match_zillow_to_properties(df, zillow_items):
    """Match Zillow results back to our CSV rows by normalized address."""

    # Build a lookup from normalized zillow address -> zestimate
    # Actual fields from actor: PropertyAddress, zestimate, Price, PropertyZillowURL
    zillow_lookup = {}
    for item in zillow_items:
        z_addr = item.get("PropertyAddress") or ""
        zestimate = item.get("zestimate") or 0
        price = item.get("Price") or 0

        norm = normalize_address(z_addr)
        if norm:
            zillow_lookup[norm] = {
                "zestimate": zestimate,
                "price": price,
            }

    matched = 0
    for idx, row in df.iterrows():
        addr = str(row.get("Official_Address", ""))
        # Normalize our address (without city/state since GIS only has street)
        norm_ours = normalize_address(addr)

        if not norm_ours:
            continue

        # Try exact match first
        if norm_ours in zillow_lookup:
            hit = zillow_lookup[norm_ours]
            val = hit["zestimate"] or hit["price"] or 0
            if val:
                df.at[idx, "Zillow_Val"] = val
                matched += 1
            continue

        # Try partial match — Zillow returns full address with city/state
        for z_norm, hit in zillow_lookup.items():
            if norm_ours in z_norm or z_norm in norm_ours:
                val = hit["zestimate"] or hit["price"] or 0
                if val:
                    df.at[idx, "Zillow_Val"] = val
                    matched += 1
                break

    return df, matched


def scrape_zillow(
    input_csv="enriched_list.csv",
    output_csv="enriched_list.csv",  # Overwrites in place
    batch_size=50,
):
    """
    Reads the enriched CSV, sends valid addresses to Apify's Zillow scraper
    in batches, and writes Zestimate values back.

    Pipeline: ingest.py -> enrich.py -> scrape_zillow.py -> alpha.py
    """

    if APIFY_TOKEN == "YOUR_TOKEN_HERE":
        print("❌ Set your Apify token first!")
        print("   Option A: export APIFY_TOKEN=your_token")
        print("   Option B: edit APIFY_TOKEN at the top of this file")
        print("")
        print("   Get a free token at: https://console.apify.com/sign-up")
        print("   Then: Settings > Integrations > API Tokens")
        return

    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"❌ {input_csv} not found. Run enrich.py first!")
        return

    # Initialize Zillow column if missing
    if "Zillow_Val" not in df.columns:
        df["Zillow_Val"] = 0

    # Filter to properties that have a real address and haven't been scraped yet
    mask = (
        df["Official_Address"].notna()
        & (df["Official_Address"] != "N/A")
        & (df["Official_Address"].str.strip() != "")
        & ((df["Zillow_Val"].isna()) | (df["Zillow_Val"] == 0))
    )

    candidates = df[mask].copy()
    addresses = candidates["Official_Address"].tolist()

    if not addresses:
        print("⚠️ No properties with valid addresses need Zillow data.")
        df.to_csv(output_csv, index=False)
        return

    print(f"🏠 Found {len(addresses)} properties to look up on Zillow")
    print(f"   Sending in batches of {batch_size}...\n")

    total_matched = 0

    # Process in batches to avoid overwhelming the actor
    for i in range(0, len(addresses), batch_size):
        batch = addresses[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(addresses) + batch_size - 1) // batch_size

        print(f"── Batch {batch_num}/{total_batches} ({len(batch)} addresses) ──")

        try:
            run_id, dataset_id = start_actor_run(batch)
            print(f"   🚀 Started Apify run: {run_id}")

            if wait_for_run(run_id):
                items = fetch_dataset(dataset_id)
                print(f"   📦 Got {len(items)} results from Zillow")

                # DEBUG: Show the first result so we can see field names
                if items and batch_num == 1:
                    first = items[0]
                    print(f"\n   🔍 DEBUG — First result keys: {list(first.keys())}")
                    # Print a few key-value pairs (truncate long values)
                    for k, v in first.items():
                        val_str = str(v)[:80]
                        print(f"      {k}: {val_str}")
                    print()

                df, matched = match_zillow_to_properties(df, items)
                total_matched += matched
                print(f"   ✅ Matched {matched}/{len(batch)} to our list")
            else:
                print(f"   ⚠️ Batch {batch_num} failed, continuing...")

        except requests.exceptions.HTTPError as e:
            print(f"   ❌ API error: {e}")
            if "402" in str(e) or "Payment" in str(e):
                print("   💰 Out of Apify credits. Saving what we have so far.")
                break
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")

        # Be polite between batches
        if i + batch_size < len(addresses):
            print("   ⏳ Waiting 5s before next batch...")
            time.sleep(5)

    # Save results
    df.to_csv(output_csv, index=False)

    print("\n" + "=" * 50)
    print("💎 Zillow Scrape Complete!")
    print(f"🎯 Matched: {total_matched}/{len(addresses)} properties")
    print(f"📁 Updated: {output_csv}")
    print(f"🔜 Next step: python alpha.py")
    print("=" * 50)


if __name__ == "__main__":
    scrape_zillow()
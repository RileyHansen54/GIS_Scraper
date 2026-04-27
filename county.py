"""
Oklahoma County Assessor Scraper v2 - Browser Automation
Uses Playwright to submit through the actual search form.

Setup:
    pip install playwright pandas
    playwright install chromium

Usage:
    python ok_county_scraper_v2.py

Output: ok_county_results.csv
"""

import re
import time
import csv
import sys

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Install playwright first:")
    print("  pip install playwright pandas")
    print("  playwright install chromium")
    sys.exit(1)

import pandas as pd

# All 202 parcels from county-owned list
PARCELS = [
    (1, "1024-12-702-1015", 470, "CHOCTAW"),
    (2, "1026-19-795-2005", 445, "CHOCTAW"),
    (3, "1026-19-795-2070", 585, "CHOCTAW"),
    (4, "1070-12-328-1250", 790, "OKC"),
    (5, "1101-14-654-0400", 410, "OKC"),
    (6, "1215-19-466-9125", 525, "CHOCTAW"),
    (7, "1246-19-689-2500", 400, "CHOCTAW"),
    (8, "1246-19-689-5700", 405, "CHOCTAW"),
    (9, "1246-19-689-6060", 410, "CHOCTAW"),
    (10, "1275-12-552-1100", 425, "OKC"),
    (11, "1335-14-431-6000", 835, "OKC"),
    (12, "1336-14-431-3060", 555, "OKC"),
    (13, "1408-15-018-2020", 460, "MIDWEST CITY"),
    (14, "1414-15-020-4270", 445, "MIDWEST CITY"),
    (15, "1420-15-083-6095", 630, "DEL CITY"),
    (16, "1422-15-094-2145", 380, "DEL CITY"),
    (17, "1428-15-127-1879", 290, "DEL CITY"),
    (18, "1428-15-127-6601", 885, "DEL CITY"),
    (19, "1428-15-394-0200", 410, "DEL CITY"),
    (20, "1430-15-391-8195", 455, "DEL CITY"),
    (21, "1431-15-345-5225", 430, "DEL CITY"),
    (22, "1434-15-019-2320", 440, "MIDWEST CITY"),
    (23, "1470-15-316-3000", 520, "DEL CITY"),
    (24, "1471-15-320-3735", 280, "DEL CITY"),
    (25, "1475-11-530-1025", 350, "OKC"),
    (26, "1476-11-328-0850", 440, "OKC"),
    (27, "1476-13-166-2080", 390, "OKC"),
    (28, "1526-12-796-1470", 430, "OKC"),
    (29, "1527-12-973-2560", 1220, "OKC"),
    (30, "1603-14-303-5350", 450, "OKC"),
    (31, "1619-13-178-2985", 490, "OKC"),
    (32, "1622-10-314-7275", 505, "OKC"),
    (33, "1630-10-842-5285", 615, "OKC"),
    (34, "1633-09-651-5585", 535, "OKC"),
    (35, "1633-09-679-8280", 510, "OKC"),
    (36, "1637-09-735-3490", 435, "OKC"),
    (37, "1639-09-398-0340", 535, "OKC"),
    (38, "1640-09-637-2000", 320, "OKC"),
    (39, "1640-09-833-3205", 250, "OKC"),
    (40, "1640-09-833-3250", 290, "OKC"),
    (41, "1640-09-833-9350", 440, "OKC"),
    (42, "1644-14-994-1355", 290, "OKC"),
    (43, "1658-07-358-2850", 505, "OKC"),
    (44, "1658-07-358-8200", 330, "OKC"),
    (45, "1659-09-455-2410", 255, "OKC"),
    (46, "1659-09-455-2420", 250, "OKC"),
    (47, "1665-10-402-7600", 410, "OKC"),
    (48, "1665-10-402-8400", 265, "OKC"),
    (49, "1665-10-402-8900", 355, "OKC"),
    (50, "1665-10-556-5125", 260, "OKC"),
    (51, "1683-13-206-9395", 755, "OKC"),
    (52, "1687-08-634-4960", 585, "OKC"),
    (53, "1688-08-598-6000", 485, "OKC"),
    (54, "1705-08-603-1310", 245, "OKC"),
    (55, "1707-10-535-2810", 925, "OKC"),
    (56, "1709-10-974-1760", 485, "OKC"),
    (57, "1709-10-985-3805", 510, "OKC"),
    (58, "1713-07-446-0403", 400, "OKC"),
    (59, "1713-07-446-6200", 240, "OKC"),
    (60, "1713-20-926-1110", 525, "OKC"),
    (61, "1719-11-337-5010", 245, "OKC"),
    (62, "1723-14-839-2035", 380, "OKC"),
    (63, "1802-07-169-9245", 17840, "OKC"),
    (64, "1824-14-927-3360", 620, "OKC"),
    (65, "1845-08-673-0460", 485, "OKC"),
    (66, "1845-08-673-1316", 535, "OKC"),
    (67, "1845-08-673-1318", 350, "OKC"),
    (68, "1845-08-673-2140", 425, "OKC"),
    (69, "1845-08-673-4580", 445, "OKC"),
    (70, "1848-08-842-1805", 240, "OKC"),
    (71, "1870-12-066-2595", 345, "OKC"),
    (72, "2101-19-808-9100", 480, "HARRAH"),
    (73, "2104-19-826-1075", 440, "HARRAH"),
    (74, "2262-14-477-5515", 420, "OKC"),
    (75, "2283-19-583-1031", 515, "NICOMA PARK"),
    (76, "2313-19-582-3180", 535, "NICOMA PARK"),
    (77, "2489-19-921-1850", 550, "SPENCER"),
    (78, "2491-19-141-6810", 410, "SPENCER"),
    (79, "2493-19-915-0565", 405, "SPENCER"),
    (80, "2494-19-966-3745", 370, "SPENCER"),
    (81, "2503-16-855-3700", 420, "MIDWEST CITY"),
    (82, "2520-13-181-3000", 510, "OKC"),
    (83, "2520-15-532-4575", 570, "OKC"),
    (84, "2526-20-816-1125", 460, "DEL CITY"),
    (85, "2535-15-039-1005", 520, "MIDWEST CITY"),
    (86, "2542-20-231-1230", 485, "OKC"),
    (87, "2637-14-979-0815", 690, "OKC"),
    (88, "2637-14-979-1000", 530, "OKC"),
    (89, "2642-14-855-1010", 375, "OKC"),
    (90, "2643-14-980-2060", 380, "OKC"),
    (91, "2653-11-625-0100", 465, "OKC"),
    (92, "2658-08-687-7878", 635, "OKC"),
    (93, "2669-08-840-1480", 475, "OKC"),
    (94, "2672-08-927-5000", 210, "OKC"),
    (95, "2673-05-375-8400", 445, "OKC"),
    (96, "2675-06-718-1600", 1375, "OKC"),
    (97, "2679-05-235-2105", 480, "OKC"),
    (98, "2680-05-145-0700", 610, "OKC"),
    (99, "2680-05-145-1410", 535, "OKC"),
    (100, "2680-05-147-3000", 265, "OKC"),
    (101, "2680-05-147-3100", 535, "OKC"),
    (102, "2680-05-147-3500", 310, "OKC"),
    (103, "2680-05-147-4225", 430, "OKC"),
    (104, "2680-05-147-4260", 460, "OKC"),
    (105, "2680-05-147-4270", 470, "OKC"),
    (106, "2684-04-852-0705", 370, "OKC"),
    (107, "2684-04-900-4600", 390, "OKC"),
    (108, "2691-03-491-8010", 485, "OKC"),
    (109, "2691-13-336-5600", 620, "OKC"),
    (110, "2695-07-372-0150", 245, "OKC"),
    (111, "2695-13-338-7800", 495, "OKC"),
    (112, "2697-08-974-9142", 245, "OKC"),
    (113, "2700-03-003-7130", 245, "OKC"),
    (114, "2700-03-003-7137", 245, "OKC"),
    (115, "2701-03-296-0105", 405, "OKC"),
    (116, "2704-03-083-8800", 460, "OKC"),
    (117, "2704-03-083-9880", 560, "OKC"),
    (118, "2709-04-388-1862", 350, "OKC"),
    (119, "2714-06-166-2970", 255, "OKC"),
    (120, "2714-06-190-8325", 505, "OKC"),
    (121, "2717-06-374-6485", 385, "OKC"),
    (122, "2719-13-350-7315", 460, "OKC"),
    (123, "2722-07-185-2455", 380, "OKC"),
    (124, "2722-07-193-0290", 250, "OKC"),
    (125, "2722-07-193-0430", 355, "OKC"),
    (126, "2722-07-193-0470", 460, "OKC"),
    (127, "2723-07-209-0126", 450, "OKC"),
    (128, "2723-07-209-2700", 475, "OKC"),
    (129, "2725-01-592-9250", 245, "OKC"),
    (130, "2806-14-263-1140", 250, "OKC"),
    (131, "2810-14-255-1607", 245, "OKC"),
    (132, "2810-14-255-1617", 245, "OKC"),
    (133, "2810-14-255-1627", 245, "OKC"),
    (134, "2810-14-255-1637", 245, "OKC"),
    (135, "2810-14-255-1647", 245, "OKC"),
    (136, "2811-18-846-1015", 515, "WARR ACRES"),
    (137, "2812-18-846-2080", 570, "WARR ACRES"),
    (138, "2816-14-015-1944", 2510, "OKC"),
    (139, "2834-18-908-0500", 240, "WARR ACRES"),
    (140, "2834-18-908-4500", 240, "WARR ACRES"),
    (141, "2835-17-285-2000", 255, "BETHANY"),
    (142, "2835-17-376-8200", 435, "BETHANY"),
    (143, "2836-17-595-1030", 490, "BETHANY"),
    (144, "2836-18-432-1126", 440, "WARR ACRES"),
    (145, "2842-14-289-1015", 295, "OKC"),
    (146, "2852-07-278-1095", 250, "OKC"),
    (147, "2857-18-862-1280", 490, "WARR ACRES"),
    (148, "2857-18-897-1155", 255, "WARR ACRES"),
    (149, "2857-18-897-1305", 240, "WARR ACRES"),
    (150, "2874-17-239-1326", 445, "BETHANY"),
    (151, "2879-17-390-1085", 500, "BETHANY"),
    (152, "2879-17-390-1520", 245, "BETHANY"),
    (153, "2885-14-544-5670", 510, "OKC"),
    (154, "2892-14-610-1025", 245, "OKC"),
    (155, "2898-06-446-6000", 465, "OKC"),
    (156, "2914-14-798-2430", 1515, "OKC"),
    (157, "2915-12-018-1010", 530, "OKC"),
    (158, "2928-14-804-1515", 485, "OKC"),
    (159, "3421-18-308-1000", 625, "EDMOND"),
    (160, "3421-18-308-1080", 455, "EDMOND"),
    (161, "3613-14-452-0824", 370, "OKC"),
    (162, "3633-12-890-1235", 435, "OKC"),
    (163, "3633-20-225-1295", 750, "OKC"),
    (164, "3635-14-103-4877", 410, "OKC"),
    (165, "3645-18-257-1375", 375, "EDMOND"),
    (166, "3653-12-283-1265", 420, "OKC"),
    (167, "3671-11-012-2325", 410, "OKC"),
    (168, "3674-10-967-1075", 640, "OKC"),
    (169, "3681-13-076-6850", 605, "OKC"),
    (170, "3682-13-076-5376", 630, "OKC"),
    (171, "3682-13-076-6363", 455, "OKC"),
    (172, "3683-13-076-2493", 435, "OKC"),
    (173, "3683-13-076-5970", 455, "OKC"),
    (174, "3684-12-294-1930", 550, "OKC"),
    (175, "3697-13-492-0300", 1175, "OKC"),
    (176, "3721-17-958-2590", 505, "VILLAGE"),
    (177, "3726-13-160-2720", 515, "OKC"),
    (178, "3726-13-160-6440", 465, "OKC"),
    (179, "3726-13-160-9142", 250, "OKC"),
    (180, "3730-13-048-3635", 525, "OKC"),
    (181, "3858-12-048-1215", 445, "OKC"),
    (182, "3858-12-288-1255", 610, "OKC"),
    (183, "3862-10-032-2845", 495, "OKC"),
    (184, "3874-12-065-1005", 765, "OKC"),
    (185, "3882-12-637-1055", 450, "OKC"),
    (186, "3885-13-499-1055", 355, "OKC"),
    (187, "3896-12-034-1495", 455, "OKC"),
    (188, "3897-17-901-1200", 245, "VILLAGE"),
    (189, "3907-14-885-1585", 895, "OKC"),
    (190, "3915-14-216-1325", 1385, "OKC"),
    (191, "3921-17-125-5305", 490, "OKC"),
    (192, "3922-13-085-1665", 375, "OKC"),
    (193, "4107-18-531-4500", 1230, "LUTHER"),
    (194, "4108-18-563-3750", 520, "LUTHER"),
    (195, "4522-12-003-1385", 425, "EDMOND"),
    (196, "4655-20-041-1395", 430, "EDMOND"),
    (197, "4702-20-624-1150", 490, "EDMOND"),
    (198, "4732-12-995-1490", 570, "OKC"),
    (199, "4741-18-256-1228", 430, "OKC"),
    (200, "4743-18-270-1305", 645, "EDMOND"),
    (201, "4834-12-867-1450", 630, "UNINCORPORATED"),
    (202, "4834-12-867-1460", 630, "UNINCORPORATED"),
]

SEARCH_URL = "https://docs.oklahomacounty.org/AssessorWP5/DefaultSearch.asp"


def parcel_to_raccount(parcel: str) -> str:
    parts = parcel.split("-")
    return "R" + parts[1] + parts[2] + parts[3]


def extract_field(text: str, label: str, stop_labels: list[str] = None) -> str:
    """Extract a field value from page text given a label."""
    idx = text.find(label)
    if idx == -1:
        return ""
    start = idx + len(label)
    # Find the end - either next label or end of string
    end = len(text)
    if stop_labels:
        for sl in stop_labels:
            si = text.find(sl, start)
            if si != -1 and si < end:
                end = si
    return text[start:end].strip()


def parse_property_page(text: str) -> dict:
    """Parse the full text of a property display page."""
    result = {}

    # Acres
    m = re.search(r"Land Size:\s*([\d.]+)\s*Acres", text)
    result["acres"] = float(m.group(1)) if m else None

    # Address
    m = re.search(r"Location:\s*(.+?)(?:Building Name|Owner Name)", text, re.DOTALL)
    result["address"] = m.group(1).strip().replace("\n", " ")[:100] if m else ""

    # School
    m = re.search(r"School System:\s*(.+?)(?:Land Size|Country)", text, re.DOTALL)
    result["school"] = m.group(1).strip().replace("\n", " ")[:60] if m else ""

    # Section/Township/Range
    m = re.search(r"(Sect?\s+\d+[-–\s]T\d+N[-–\s]R\d+W)", text)
    result["str"] = m.group(1) if m else ""

    # Quarter
    m = re.search(r"Qtr\s+(\w+)", text)
    result["quarter"] = m.group(1) if m else ""

    # Legal description
    m = re.search(r"Full Legal Description:\s*(.+?)(?:No comparable|Value History|Click button)", text, re.DOTALL)
    result["legal_desc"] = m.group(1).strip().replace("\n", " ")[:200] if m else ""

    # Vacant or improved
    if re.search(r"Vacant", text) and not re.search(r"Improved.*Land", text):
        result["status"] = "Vacant"
    elif re.search(r"Improved", text):
        result["status"] = "Improved"
    else:
        result["status"] = "Unknown"

    # Market value (first non-zero in history)
    vals = re.findall(r"(\d{4})\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)", text)
    result["last_mkt_val"] = 0
    result["val_year"] = ""
    for row in vals:
        val = int(row[1].replace(",", ""))
        if val > 0:
            result["last_mkt_val"] = val
            result["val_year"] = row[0]
            break

    # Demolition permit
    result["demo_permit"] = "Demolish" in text

    return result


def main():
    print("=" * 70)
    print("Oklahoma County Assessor Scraper v2 (Playwright)")
    print("=" * 70)

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        for i, (record, parcel, bid, city) in enumerate(PARCELS):
            raccount = parcel_to_raccount(parcel)
            map_num = parcel.split("-")[0]

            sys.stdout.write(f"\r[{i+1}/202] #{record} {raccount} ({city})...          ")
            sys.stdout.flush()

            try:
                # Go to search page
                page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=15000)

                # Clear and fill the Real Acct # field
                acct_input = page.locator("input[name='RealAcct']")
                if acct_input.count() == 0:
                    # Try alternate field names
                    acct_input = page.locator("input[name='realacct']")
                if acct_input.count() == 0:
                    # Fallback: try map number search instead
                    map_input = page.locator("input[name='MapNum']")
                    if map_input.count() == 0:
                        map_input = page.locator("input[name='mapnum']")
                    if map_input.count() > 0:
                        map_input.fill(map_num)
                    else:
                        results.append({
                            "record": record, "parcel": parcel, "r_account": raccount,
                            "city": city, "bid": bid, "error": "Could not find input field"
                        })
                        continue

                if acct_input.count() > 0:
                    acct_input.fill(raccount)

                # Submit the form
                page.keyboard.press("Enter")
                page.wait_for_load_state("domcontentloaded", timeout=10000)
                time.sleep(1)

                # Get page text
                text = page.inner_text("body")

                # Check if we got a results page or property page
                if "Land Size:" in text:
                    # We're on a property detail page
                    data = parse_property_page(text)
                    results.append({
                        "record": record, "parcel": parcel, "r_account": raccount,
                        "map": map_num, "city": city, "bid": bid,
                        **data, "error": ""
                    })
                elif "No records found" in text or "no records" in text.lower():
                    results.append({
                        "record": record, "parcel": parcel, "r_account": raccount,
                        "map": map_num, "city": city, "bid": bid,
                        "error": "No records found"
                    })
                else:
                    # Might be a list of results - try clicking first one
                    links = page.locator("a:has-text('R0'), a:has-text('R1'), a:has-text('R2')")
                    if links.count() > 0:
                        links.first.click()
                        page.wait_for_load_state("domcontentloaded", timeout=10000)
                        time.sleep(0.5)
                        text = page.inner_text("body")
                        if "Land Size:" in text:
                            data = parse_property_page(text)
                            results.append({
                                "record": record, "parcel": parcel, "r_account": raccount,
                                "map": map_num, "city": city, "bid": bid,
                                **data, "error": ""
                            })
                        else:
                            results.append({
                                "record": record, "parcel": parcel, "r_account": raccount,
                                "map": map_num, "city": city, "bid": bid,
                                "error": "Could not parse result page"
                            })
                    else:
                        results.append({
                            "record": record, "parcel": parcel, "r_account": raccount,
                            "map": map_num, "city": city, "bid": bid,
                            "error": "Unexpected page content"
                        })

            except Exception as e:
                results.append({
                    "record": record, "parcel": parcel, "r_account": raccount,
                    "map": map_num, "city": city, "bid": bid,
                    "error": str(e)[:100]
                })

            # Polite delay
            time.sleep(1)

        browser.close()

    print("\n\nDone! Building report...\n")

    df = pd.DataFrame(results)
    df.to_csv("ok_county_all_parcels.csv", index=False)
    print(f"All results: ok_county_all_parcels.csv")

    # Filter interesting ones
    if "acres" in df.columns:
        interesting = df[
            (df["acres"].notna()) &
            (df["acres"] >= 0.10) &
            (df["error"] == "") &
            (~df["legal_desc"].str.contains("COMMON AREA|ALLEY|EASEMENT|STRIP", case=False, na=False))
        ].sort_values("acres", ascending=False)

        interesting.to_csv("ok_county_interesting.csv", index=False)
        print(f"Interesting parcels: ok_county_interesting.csv ({len(interesting)} parcels)")

        print("\n" + "=" * 70)
        print("TOP PARCELS BY ACREAGE")
        print("=" * 70)
        for _, row in interesting.head(20).iterrows():
            print(f"\n#{int(row['record']):>3} | {row['parcel']} | {row['city']}")
            print(f"      Bid: ${row['bid']:,.0f} | Acres: {row['acres']:.4f} | {row.get('status','?')}")
            print(f"      School: {row.get('school','')}")
            print(f"      Legal: {str(row.get('legal_desc',''))[:100]}")
            if row.get("last_mkt_val", 0) > 0:
                print(f"      Last value: ${row['last_mkt_val']:,} ({row.get('val_year','')})")

    print("\n" + "=" * 70)
    print("DONE.")
    print("=" * 70)


if __name__ == "__main__":
    main()
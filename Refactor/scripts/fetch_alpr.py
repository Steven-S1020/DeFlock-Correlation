"""
fetch_alpr.py
=============
Pulls ALPR camera counts by county from the Overpass API (OpenStreetMap)
and saves the raw results to:
  - raw_data/alpr.csv

Run:
    python fetch_alpr.py
"""

import os
import time
import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
HEADERS = {
    "User-Agent": "DeFlockCorrelations/1.0 (GH:Steven-S1020)",
    "Accept": "*/*",
}
OUTPUT_DIR = "./raw_data"
MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds between retries
STATE_PAUSE = 2  # seconds between states (be polite to the API)

STATES = {
    "AL": {"name": "Alabama", "fips": "01"},
    "AK": {"name": "Alaska", "fips": "02"},
    "AZ": {"name": "Arizona", "fips": "04"},
    "AR": {"name": "Arkansas", "fips": "05"},
    "CA": {"name": "California", "fips": "06"},
    "CO": {"name": "Colorado", "fips": "08"},
    "CT": {"name": "Connecticut", "fips": "09"},
    "DE": {"name": "Delaware", "fips": "10"},
    "DC": {"name": "District of Columbia", "fips": "11"},
    "FL": {"name": "Florida", "fips": "12"},
    "GA": {"name": "Georgia", "fips": "13"},
    "HI": {"name": "Hawaii", "fips": "15"},
    "ID": {"name": "Idaho", "fips": "16"},
    "IL": {"name": "Illinois", "fips": "17"},
    "IN": {"name": "Indiana", "fips": "18"},
    "IA": {"name": "Iowa", "fips": "19"},
    "KS": {"name": "Kansas", "fips": "20"},
    "KY": {"name": "Kentucky", "fips": "21"},
    "LA": {"name": "Louisiana", "fips": "22"},
    "ME": {"name": "Maine", "fips": "23"},
    "MD": {"name": "Maryland", "fips": "24"},
    "MA": {"name": "Massachusetts", "fips": "25"},
    "MI": {"name": "Michigan", "fips": "26"},
    "MN": {"name": "Minnesota", "fips": "27"},
    "MS": {"name": "Mississippi", "fips": "28"},
    "MO": {"name": "Missouri", "fips": "29"},
    "MT": {"name": "Montana", "fips": "30"},
    "NE": {"name": "Nebraska", "fips": "31"},
    "NV": {"name": "Nevada", "fips": "32"},
    "NH": {"name": "New Hampshire", "fips": "33"},
    "NJ": {"name": "New Jersey", "fips": "34"},
    "NM": {"name": "New Mexico", "fips": "35"},
    "NY": {"name": "New York", "fips": "36"},
    "NC": {"name": "North Carolina", "fips": "37"},
    "ND": {"name": "North Dakota", "fips": "38"},
    "OH": {"name": "Ohio", "fips": "39"},
    "OK": {"name": "Oklahoma", "fips": "40"},
    "OR": {"name": "Oregon", "fips": "41"},
    "PA": {"name": "Pennsylvania", "fips": "42"},
    "RI": {"name": "Rhode Island", "fips": "44"},
    "SC": {"name": "South Carolina", "fips": "45"},
    "SD": {"name": "South Dakota", "fips": "46"},
    "TN": {"name": "Tennessee", "fips": "47"},
    "TX": {"name": "Texas", "fips": "48"},
    "UT": {"name": "Utah", "fips": "49"},
    "VT": {"name": "Vermont", "fips": "50"},
    "VA": {"name": "Virginia", "fips": "51"},
    "WA": {"name": "Washington", "fips": "53"},
    "WV": {"name": "West Virginia", "fips": "54"},
    "WI": {"name": "Wisconsin", "fips": "55"},
    "WY": {"name": "Wyoming", "fips": "56"},
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_query(state_code: str) -> str:
    """Build Overpass QL query for ALPR counts per county in a state."""
    # DC uses admin_level 4 (district) instead of 6 (county)
    if state_code == "DC":
        return """
[out:csv(name, state, "nist:state_fips", "nist:fips_code", total)][timeout:180];
area["ISO3166-2"="US-DC"];
relation["admin_level"="4"](area)->.districts;
foreach.districts->.district(
  .district map_to_area->.district_area;
  node(area.district_area)["man_made"="surveillance"]["surveillance:type"="ALPR"];
  make count name = district.set(t["name"]),
             state = "DC",
             "nist:state_fips" = district.set(t["nist:state_fips"]),
             "nist:fips_code" = district.set(t["nist:fips_code"]),
             total = count(nodes);
  out;
);
"""
    return f"""
[out:csv(name, state, "nist:state_fips", "nist:fips_code", total)][timeout:180];
area["ISO3166-2"="US-{state_code}"];
relation["admin_level"="6"](area)->.counties;
foreach.counties->.county(
  .county map_to_area->.county_area;
  node(area.county_area)["man_made"="surveillance"]["surveillance:type"="ALPR"];
  make count name = county.set(t["name"]),
             state = "{state_code}",
             "nist:state_fips" = county.set(t["nist:state_fips"]),
             "nist:fips_code" = county.set(t["nist:fips_code"]),
             total = count(nodes);
  out;
);
"""


def _normalize_state_fips(val: str):
    """Pad to 2 digits; return None if empty."""
    s = str(val).strip()
    if not s:
        return None
    if len(s) == 1:
        return "0" + s
    if len(s) == 5:  # full county FIPS — take state portion
        return s[:2]
    return s


def _build_geo_id(state_fips: str, county_fips: str):
    """Combine state + county FIPS into a GEO_ID string, or return None."""
    sfips = str(state_fips).zfill(2) if state_fips != "NA" else None
    cfips = str(county_fips).strip()

    if not sfips or cfips in ("NA", ""):
        return None
    if len(cfips) == 5 and cfips[:2] == sfips:
        return "0500000US" + cfips
    if len(cfips) <= 3:
        return "0500000US" + sfips + cfips.zfill(3)
    return None


def _query_state(state_code: str, expected_fips: str) -> list:
    """Query Overpass for one state with retries. Returns a list of row dicts."""
    query = _build_query(state_code)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  [{state_code}] attempt {attempt}...")
            resp = requests.post(
                OVERPASS_URL, data={"data": query}, headers=HEADERS, timeout=120
            )
            resp.raise_for_status()

            lines = resp.text.strip().split("\n")
            if len(lines) <= 1:
                print(f"  [{state_code}] no data returned.")
                return []

            rows = []
            for line in lines[1:]:  # skip header
                parts = line.split("\t")
                if len(parts) != 5:
                    continue

                state_fips_norm = _normalize_state_fips(parts[2])

                # Drop rows tagged to the wrong state
                if state_fips_norm and state_fips_norm != expected_fips:
                    continue

                try:
                    total = int(parts[4].strip())
                except ValueError:
                    total = 0

                state_fips_raw = parts[2].strip() or "NA"
                county_fips_raw = parts[3].strip() or "NA"

                rows.append(
                    {
                        "county_name": parts[0].strip() or "NA",
                        "state": parts[1].strip() or state_code,
                        "state_fips": state_fips_raw,
                        "county_fips": county_fips_raw,
                        "alpr_total": total,
                        "GEO_ID": _build_geo_id(state_fips_raw, county_fips_raw),
                    }
                )

            print(f"  [{state_code}] {len(rows)} counties returned.")
            return rows

        except requests.exceptions.Timeout:
            print(f"  [{state_code}] timeout on attempt {attempt}.")
        except requests.exceptions.RequestException as e:
            print(f"  [{state_code}] request error: {e}")
        except Exception as e:
            print(f"  [{state_code}] unexpected error: {e}")
            return []

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    print(f"  [{state_code}] giving up after {MAX_RETRIES} attempts.")
    return []


# ---------------------------------------------------------------------------
# Main fetch function
# ---------------------------------------------------------------------------


def fetch_alpr() -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Fetching ALPR data from Overpass API")
    print("=" * 60)

    all_rows = []
    for state_code, info in STATES.items():
        print(f"\nQuerying {info['name']} ({state_code})...")
        all_rows.extend(_query_state(state_code, info["fips"]))
        time.sleep(STATE_PAUSE)

    df = pd.DataFrame(all_rows)
    if df.empty:
        print("WARNING: No ALPR data retrieved!")
        return df

    print(f"\nTotal rows:           {len(df)}")
    print(f"Rows with GEO_ID:     {df['GEO_ID'].notna().sum()}")
    print(f"Total ALPRs recorded: {df['alpr_total'].sum()}")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = fetch_alpr()
    out_path = f"{OUTPUT_DIR}/alpr.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}  ({len(df)} rows)")


if __name__ == "__main__":
    main()

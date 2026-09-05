"""
Pull ACS 5-Year DP03_0128PE (% of all people below poverty level)
for every county in all 50 states + DC.
Outputs a CSV with geo_id and poverty_pct for merging.
"""

import requests
import pandas as pd
import time
from typing import Dict

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
ACS_YEAR = 2024
VARIABLE = "DP03_0128PE"
BASE_URL = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5/profile"

# -------------------------------------------------------------------
# State + Alaska FIPS definitions
# -------------------------------------------------------------------
STATES: Dict[str, Dict] = {
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

ALASKA_COUNTY_FIPS = {
    "013",
    "016",
    "020",
    "050",
    "060",
    "063",
    "066",
    "068",
    "070",
    "090",
    "100",
    "105",
    "110",
    "122",
    "130",
    "150",
    "158",
    "164",
    "170",
    "180",
    "185",
    "188",
    "195",
    "198",
    "220",
    "230",
    "240",
    "261",
    "275",
    "282",
    "290",
}


# -------------------------------------------------------------------
# Fetch function
# -------------------------------------------------------------------
def fetch_counties(state_fips: str) -> list[dict] | None:
    params = {
        "get": f"NAME,{VARIABLE}",
        "for": "county:*",
        "in": f"state:{state_fips}",
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        rows = r.json()
        header, *data = rows
        results = []
        for row in data:
            record = dict(zip(header, row))
            results.append(record)
        return results
    except requests.HTTPError as e:
        print(f"  HTTP error for state {state_fips}: {e}")
        return None
    except Exception as e:
        print(f"  Unexpected error for state {state_fips}: {e}")
        return None


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    all_rows = []

    for abbr, info in STATES.items():
        state_fips = info["fips"]
        print(f"Fetching {info['name']} ({state_fips})...")

        records = fetch_counties(state_fips)
        if not records:
            print(f"  No data returned for {info['name']}, skipping.")
            continue

        for rec in records:
            county_fips = rec["county"]

            # Filter Alaska to borough-level only
            if state_fips == "02" and county_fips not in ALASKA_COUNTY_FIPS:
                continue

            geo_id = f"{state_fips}{county_fips}"  # e.g. "01001" for Autauga County, AL

            all_rows.append(
                {
                    "geo_id": geo_id,
                    "name": rec["NAME"],
                    "poverty_pct": rec[VARIABLE],
                }
            )

        time.sleep(0.1)  # be polite to the API

    # -------------------------------------------------------------------
    # Build DataFrame and clean up
    # -------------------------------------------------------------------
    df = pd.DataFrame(all_rows)

    # Convert to numeric; Census uses -666666666 for missing/suppressed values
    df["poverty_pct"] = pd.to_numeric(df["poverty_pct"], errors="coerce")
    df.loc[df["poverty_pct"] < 0, "poverty_pct"] = None

    output_path = "poverty_by_county.csv"
    df.to_csv(output_path, index=False)

    print(f"\nDone. {len(df)} counties written to {output_path}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()

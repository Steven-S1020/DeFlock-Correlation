"""
fetch_census.py
===============
Pulls raw ACS 5-Year Census data for all U.S. counties and saves it to CSV.

Fetches three Data Profile tables (DP02, DP03, DP05) and outputs one file
to the raw data directory:
  - census_acs.csv : county-level demographic/socioeconomic variables

Run:
    python fetch_census.py
"""

import os
import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CENSUS_YEAR = 2024
OUTPUT_DIR = "./raw_data"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetch_census_table(table_name: str, year: int = CENSUS_YEAR) -> pd.DataFrame:
    """Fetch a full ACS Data Profile table for all counties."""
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5/profile"
        f"?get=group({table_name})&for=county:*&in=state:*"
    )
    print(f"  Fetching {table_name}...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    print(f"  {table_name}: {len(df)} rows, {len(df.columns)} columns")
    return df


def _select_rename(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Keep only mapped columns and rename them."""
    existing = {k: v for k, v in mapping.items() if k in df.columns}
    return df[list(existing.keys())].rename(columns=existing)


# ---------------------------------------------------------------------------
# Main fetch function
# ---------------------------------------------------------------------------


def fetch_census_data(year: int = CENSUS_YEAR) -> pd.DataFrame:
    """Pull DP02, DP03, DP05 and return a cleaned Census DataFrame."""
    print("\n" + "=" * 60)
    print("Fetching Census ACS 5-Year data")
    print("=" * 60)

    dp02_map = {
        "GEO_ID": "GEO_ID",
        "state": "state",
        "county": "county",
        "NAME": "county_name",
        "DP02_0018E": "_pop_in_households",  # used for pct_unhoused calc, dropped after
        "DP02_0067PE": "_pct_hs_graduate_or_higher",
        "DP02_0094PE": "pct_foreign_born",
        "DP02_0097PE": "pct_not_citizen",
        "DP02_0114PE": "pct_language_not_english",
    }

    dp03_map = {
        "GEO_ID": "GEO_ID",
        "DP03_0007PE": "pct_not_in_labor_force",
        "DP03_0009PE": "rate_unemployment",
        "DP03_0019PE": "_pct_drove_alone",  # combined below, dropped after
        "DP03_0020PE": "_pct_carpooled",  # combined below, dropped after
        "DP03_0047PE": "pct_private_wage_salary_workers",
        "DP03_0074PE": "pct_households_fs_snap",
        "DP03_0099PE": "pct_no_health_insurance",
        "DP03_0128PE": "pct_people_impoverished",
    }

    dp05_map = {
        "GEO_ID": "GEO_ID",
        "DP05_0001E": "_pop_total",
        "DP05_0090PE": "pct_hispanic",
        "DP05_0096PE": "pct_NH_white",
        "DP05_0097PE": "pct_NH_black",
        "DP05_0098PE": "pct_NH_AIAN",
        "DP05_0099PE": "pct_NH_asian",
        "DP05_0100PE": "pct_NH_NHPI",
        "DP05_0101PE": "pct_NH_some_other",
        "DP05_0102PE": "pct_NH_two_or_more",
    }

    raw_dp02 = _fetch_census_table("DP02", year)
    raw_dp03 = _fetch_census_table("DP03", year)
    raw_dp05 = _fetch_census_table("DP05", year)

    df02 = _select_rename(raw_dp02, dp02_map)
    df03 = _select_rename(raw_dp03, dp03_map)
    df05 = _select_rename(raw_dp05, dp05_map)

    df = df02.merge(df03, on="GEO_ID", how="outer")
    df = df.merge(df05, on="GEO_ID", how="outer")

    print(f"  Combined rows before cleaning: {len(df)}")

    # Keep only true county-level rows (summary level 050)
    df = df[df["GEO_ID"].str.startswith("0500000US", na=False)].copy()

    # Drop Puerto Rico
    df = df[df["state"] != "72"].copy()

    # --- Derived variables ---

    # pct_unhoused: percent of population not living in households
    pop_total = pd.to_numeric(df["_pop_total"], errors="coerce")
    pop_in_hh = pd.to_numeric(df["_pop_in_households"], errors="coerce")
    df["pct_unhoused"] = (100 - 100 * (pop_in_hh / pop_total)).round(2)

    # Invert HS graduate rate to get "not HS graduate or higher"
    pct_hs = pd.to_numeric(df["_pct_hs_graduate_or_higher"], errors="coerce")
    df["pct_not_hs_graduate"] = (100 - pct_hs).round(4)

    # Combine drove alone + carpooled
    drove = pd.to_numeric(df["_pct_drove_alone"], errors="coerce")
    carpool = pd.to_numeric(df["_pct_carpooled"], errors="coerce")
    df["pct_commute_drove_or_carpooled"] = (drove + carpool).round(2)

    # Drop helper/intermediate columns
    df = df.drop(
        columns=[
            "_pop_in_households",
            "_pct_hs_graduate_or_higher",
            "_pct_drove_alone",
            "_pct_carpooled",
            "_pop_total",
            "county_name",
            "state",
            "county",
        ]
    )

    # Reorder columns cleanly
    id_cols = ["GEO_ID"]
    other_cols = [c for c in df.columns if c not in id_cols]
    df = df[id_cols + other_cols]

    print(f"  Rows after cleaning: {len(df)}")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df_census = fetch_census_data()
    out_path = f"{OUTPUT_DIR}/census_acs-PE.csv"
    df_census.to_csv(out_path, index=False)
    print(
        f"\nSaved: {out_path}  ({len(df_census)} rows, {len(df_census.columns)} columns)"
    )


if __name__ == "__main__":
    main()

"""
merge.py
========
Loads all raw data files from raw_data/, merges them into a single
analysis-ready CSV, and saves it to:
  - merged.csv

Expected raw_data/ files:
  - census_acs.csv   : from fetch_census.py  (required — master county list)
  - alpr.csv         : from fetch_alpr.py    (required)
  - Any additional CSVs you drop in raw_data/ will be auto-joined on GEO_ID

Run:
    python merge.py
"""

import os
import glob
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RAW_DIR = "./raw_data"
OUTPUT_FILE = "./final_data/merged.csv"

# CSVs handled explicitly — everything else in raw_data/ is joined automatically
CENSUS_FILE_E = f"{RAW_DIR}/census_acs-E.csv"
CENSUS_FILE_PE = f"{RAW_DIR}/census_acs-PE.csv"
ALPR_FILE = f"{RAW_DIR}/alpr.csv"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    os.makedirs("./final_data", exist_ok=True)

    # --- Census Estimates ---
    print(f"Loading {CENSUS_FILE_E}...")
    df = pd.read_csv(CENSUS_FILE_E, dtype={"state": str, "county": str})
    print(f"  {len(df)} counties")

    # --- Census Percents ---
    print(f"Loading {CENSUS_FILE_PE}...")
    df_pe = pd.read_csv(CENSUS_FILE_PE, dtype={"state": str, "county": str})
    print(f"  {len(df)} counties")

    # Drop any columns already in df (other than the join key) to avoid _x/_y suffixes
    id_cols = ["GEO_ID", "county_name", "state", "county"]
    pe_cols_to_drop = [c for c in df_pe.columns if c in df.columns and c != "GEO_ID"]
    df_pe = df_pe.drop(columns=pe_cols_to_drop)

    df = df.merge(df_pe, on="GEO_ID", how="left")
    print(f"  After census merge: {len(df)} rows, {len(df.columns)} columns")

    # --- ALPR ---
    print(f"Loading {ALPR_FILE}...")
    alpr = pd.read_csv(ALPR_FILE, dtype=str)

    # Aggregate in case a county appears more than once
    alpr["alpr_total"] = pd.to_numeric(alpr["alpr_total"], errors="coerce").fillna(0)
    alpr_agg = (
        alpr[alpr["GEO_ID"].notna()]
        .groupby("GEO_ID", as_index=False)["alpr_total"]
        .sum()
    )

    df = df.merge(alpr_agg[["GEO_ID", "alpr_total"]], on="GEO_ID", how="left")
    other_cols = [c for c in df.columns if c not in id_cols + ["alpr_total"]]
    df = df[id_cols + ["alpr_total"] + other_cols]
    df["alpr_total"] = df["alpr_total"].fillna(0).astype(int)
    print(
        f"  Total ALPRs: {df['alpr_total'].sum():,}  |  Counties with 0: {(df['alpr_total'] == 0).sum()}"
    )

    # --- Any extra CSVs dropped into raw_data/ ---
    known = {
        os.path.abspath(CENSUS_FILE_E),
        os.path.abspath(CENSUS_FILE_PE),
        os.path.abspath(ALPR_FILE),
    }
    extras = [
        f for f in glob.glob(f"{RAW_DIR}/*.csv") if os.path.abspath(f) not in known
    ]

    for path in sorted(extras):
        print(f"Joining extra file: {path}...")
        extra = pd.read_csv(path, dtype=str)
        if "GEO_ID" not in extra.columns:
            print(f"  Skipping — no GEO_ID column found.")
            continue
        # Convert numeric-looking columns
        for col in extra.columns:
            if col != "GEO_ID":
                extra[col] = pd.to_numeric(extra[col], errors="ignore")
        df = df.merge(extra, on="GEO_ID", how="left")
        print(f"  Joined {len(extra)} rows, {len(extra.columns) - 1} new columns.")

    # --- Final cleanup ---
    # Convert any remaining object columns to numeric where possible
    skip_cols = set(id_cols)
    for col in df.columns:
        if col not in skip_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Save
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved: {OUTPUT_FILE}  ({len(df)} rows, {len(df.columns)} columns)")


if __name__ == "__main__":
    main()

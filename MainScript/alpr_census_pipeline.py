"""
ALPR + Census Data Pipeline
============================
Single script that:
  1. Queries Overpass API for ALPR counts by county (all 50 states + DC)
  2. Pulls ACS 5-Year Census data (DP02, DP03, DP05 tables)
  3. Cleans and merges both datasets on GEO_ID / FIPS
  4. Outputs a single analysis-ready CSV

Cleaning rules applied:
  - Census: keep only true county-level rows (GEO_ID starts with '0500000US')
  - Census: county FIPS (last 3 digits of full FIPS) must be <= 840 to drop
    independent cities (Virginia etc., which start at 510/500) and other
    non-county equivalents. Independent cities have FIPS >= 510 in Virginia;
    for safety we keep anything <= 840 (the highest real county FIPS in any
    state). DC (11001) is kept as a single county-equivalent row.
  - Alaska: only rows whose 3-digit county FIPS appear in the known list of
    AK borough/census-area FIPS codes are kept (drops subareas, which are
    MCD-level, not county-level).
  - ALPR: rows with empty/NA county FIPS are flagged but kept with total=0
    so every Census county has a match; rows whose state FIPS doesn't match
    the queried state are dropped.
  - Merge: left join from Census (master list of ~3,144 counties) onto ALPR
    so counties with 0 ALPRs recorded are preserved with alpr_total=0.
  - Percentage variables: predictor columns are pulled as Census PE (percent)
    variables directly from the ACS API, which use the correct survey-weighted
    denominators (e.g. commute % is out of commuters, not total population).
    To switch back to raw counts, change USE_PERCENT = True to False below.
  - Gini index: pulled from ACS Detailed Table B19083 via a separate API call.
  - Republican vote share: downloaded from the MIT Election Lab / Harvard
    Dataverse county presidential returns (2000-2020). The script uses the 2020
    election by default (set ELECTION_YEAR below to change).
  - Undocumented immigrants: no public county-level API exists. The MPI
    publishes estimates for only ~287 counties. The best full-coverage ACS proxy
    is not_citizen (non-citizens), which is already in the dataset. A note is
    printed at runtime as a reminder.
"""

import time
import requests
import pandas as pd
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds between retries
STATE_PAUSE = 2  # seconds between state queries (be polite to API)
CENSUS_YEAR = 2024  # ACS 5-year estimates year

# Toggle: True  = pull PE (percent) variables from Census API (recommended)
#         False = pull E  (count/estimate) variables instead
# Switching this one flag is all you need — no other changes required.
USE_PERCENT = True

# Presidential election year to use for Republican vote share (2000-2020)
ELECTION_YEAR = 2024

# MIT Election Lab county presidential returns — Harvard Dataverse direct download
# Dataset: "County Presidential Election Returns 2000-2020", doi:10.7910/DVN/VOQCHQ
ELECTION_DATA_URL = (
    "https://dataverse.harvard.edu/api/access/datafile/:persistentId"
    "?persistentId=doi:10.7910/DVN/VOQCHQ/IJZINI"
)

# All 50 states + DC
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

# Alaska county-level FIPS codes (boroughs + census areas only, NOT subareas).
# Subareas are MCD-level (sub-county) and should be excluded.
# Source: Census Bureau FIPS list for Alaska county equivalents.
ALASKA_COUNTY_FIPS = {
    "013",  # Aleutians East Borough
    "016",  # Aleutians West Census Area
    "020",  # Anchorage Municipality
    "050",  # Bethel Census Area
    "060",  # Bristol Bay Borough
    "063",  # Chugach Census Area
    "066",  # Copper River Census Area
    "068",  # Denali Borough
    "070",  # Dillingham Census Area
    "090",  # Fairbanks North Star Borough
    "100",  # Haines Borough
    "105",  # Hoonah-Angoon Census Area
    "110",  # Juneau City and Borough
    "122",  # Kenai Peninsula Borough
    "130",  # Ketchikan Gateway Borough
    "150",  # Kodiak Island Borough
    "158",  # Kusilvak Census Area
    "164",  # Lake and Peninsula Borough
    "170",  # Matanuska-Susitna Borough
    "180",  # Nome Census Area
    "185",  # North Slope Borough
    "188",  # Northwest Arctic Borough
    "195",  # Petersburg Borough
    "198",  # Prince of Wales-Hyder Census Area
    "220",  # Sitka City and Borough
    "230",  # Skagway Municipality
    "240",  # Southeast Fairbanks Census Area
    "261",  # Valdez-Cordova Census Area (pre-2019 split; may appear in older data)
    "275",  # Wrangell City and Borough
    "282",  # Yakutat City and Borough
    "290",  # Yukon-Koyukuk Census Area
}

# ---------------------------------------------------------------------------
# STEP 1 — ALPR data from Overpass
# ---------------------------------------------------------------------------


def _build_overpass_query(state_code: str) -> str:
    """Build Overpass QL query for ALPR counts per county in a state."""
    # DC has admin_level 4 (district) rather than 6 (county), so we handle it separately.
    if state_code == "DC":
        query = """
[out:csv(name, state, "nist:state_fips", "nist:fips_code", total)][timeout:90];
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
    else:
        query = f"""
[out:csv(name, state, "nist:state_fips", "nist:fips_code", total)][timeout:90];
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
    return query


def _normalize_fips(fips_str: Optional[str]) -> Optional[str]:
    """Pad a state FIPS string to 2 digits; return None if empty."""
    if not fips_str or str(fips_str).strip() == "":
        return None
    s = str(fips_str).strip()
    if len(s) == 1:
        return "0" + s
    if len(s) == 2:
        return s
    if len(s) == 5:  # full county FIPS — take state portion
        return s[:2]
    return s


def _query_state_alpr(state_code: str, expected_state_fips: str) -> List[Dict]:
    """
    Query Overpass for one state; retry on timeout.
    Returns a list of dicts with keys: name, state, state_fips, county_fips, alpr_total.
    """
    query = _build_overpass_query(state_code)
    attempt = 1

    while True:
        try:
            print(f"  [{state_code}] attempt {attempt}...")
            resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=120)
            resp.raise_for_status()

            lines = resp.text.strip().split("\n")
            if len(lines) <= 1:
                print(f"  [{state_code}] no counties returned.")
                return []

            results = []
            for line in lines[1:]:  # skip header
                parts = line.split("\t")
                if len(parts) != 5:
                    print(f"  [{state_code}] skipping malformed line: {line!r}")
                    continue

                county_name = parts[0].strip() or "NA"
                state_abbr = parts[1].strip() or state_code
                state_fips_raw = parts[2].strip()
                county_fips_raw = parts[3].strip()
                total_raw = parts[4].strip()

                state_fips_norm = _normalize_fips(state_fips_raw)

                # Drop rows whose state FIPS tag exists but doesn't match
                if (
                    state_fips_norm is not None
                    and state_fips_norm != expected_state_fips
                ):
                    print(
                        f"  [{state_code}] filtered wrong-state row: {county_name} "
                        f"(got {state_fips_raw}, expected {expected_state_fips})"
                    )
                    continue

                try:
                    total = int(total_raw)
                except ValueError:
                    total = 0

                results.append(
                    {
                        "name": county_name,
                        "state": state_abbr,
                        "state_fips": state_fips_raw if state_fips_raw else "NA",
                        "county_fips": county_fips_raw if county_fips_raw else "NA",
                        "alpr_total": total,
                    }
                )

            print(f"  [{state_code}] {len(results)} counties returned.")
            return results

        except requests.exceptions.Timeout:
            print(f"  [{state_code}] TIMEOUT on attempt {attempt}.")
            if attempt >= MAX_RETRIES:
                print(f"  [{state_code}] giving up after {MAX_RETRIES} retries.")
                return []
            attempt += 1
            time.sleep(RETRY_DELAY)

        except requests.exceptions.RequestException as e:
            print(f"  [{state_code}] request error: {e}")
            if attempt >= MAX_RETRIES:
                return []
            attempt += 1
            time.sleep(RETRY_DELAY)

        except Exception as e:
            print(f"  [{state_code}] unexpected error: {e}")
            return []


def fetch_all_alpr() -> pd.DataFrame:
    """Query all states and return a cleaned ALPR DataFrame."""
    print("\n" + "=" * 60)
    print("STEP 1 — Fetching ALPR data from Overpass API")
    print("=" * 60)

    all_rows: List[Dict] = []
    for state_code, info in STATES.items():
        print(f"\nQuerying {info['name']} ({state_code})...")
        rows = _query_state_alpr(state_code, info["fips"])
        all_rows.extend(rows)
        time.sleep(STATE_PAUSE)

    df = pd.DataFrame(all_rows)
    if df.empty:
        print("WARNING: No ALPR data retrieved!")
        return df

    # Build a 5-digit full_fips for joining: state_fips (2) + county_fips (3)
    # county_fips from OSM tags varies in format; normalise to 3 digits
    def _build_full_fips(row):
        sfips = str(row["state_fips"]).zfill(2) if row["state_fips"] != "NA" else None
        cfips = str(row["county_fips"]).strip()
        if sfips is None or cfips == "NA" or cfips == "":
            return None
        # Some OSM tags store the full 5-digit FIPS in nist:fips_code
        if len(cfips) == 5 and cfips[:2] == sfips:
            return cfips
        if len(cfips) == 3:
            return sfips + cfips
        if len(cfips) < 3:
            return sfips + cfips.zfill(3)
        return None

    df["full_fips"] = df.apply(_build_full_fips, axis=1)
    df["GEO_ID"] = df["full_fips"].apply(lambda f: "0500000US" + f if f else None)

    print(f"\nALPR total rows before cleaning: {len(df)}")
    print(f"Rows with resolvable GEO_ID:     {df['GEO_ID'].notna().sum()}")
    print(f"Total ALPRs recorded:            {df['alpr_total'].sum()}")
    return df


# ---------------------------------------------------------------------------
# STEP 2 — Census ACS data
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


def _select_rename(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    existing = {k: v for k, v in mapping.items() if k in df.columns}
    return df[list(existing.keys())].rename(columns=existing)


def fetch_census_data() -> pd.DataFrame:
    """Pull DP02, DP03, DP05 and return a merged, cleaned Census DataFrame."""
    print("\n" + "=" * 60)
    print("STEP 2 — Fetching Census ACS 5-Year data")
    print("=" * 60)

    # --- Column mappings ---
    # Each entry is (PE_code, E_code, friendly_name).
    # USE_PERCENT selects which API code to request at runtime.
    # Columns with no meaningful percent (averages, incomes, totals, IDs)
    # always use the E code regardless of the toggle.
    suffix = "PE" if USE_PERCENT else "E"

    dp02_map = {
        # Always E — identifiers / averages
        "GEO_ID": "GEO_ID",
        "state": "state",
        "county": "county",
        "NAME": "county_name",
        "DP02_0016E": "avg_household_size",  # no PE — already a mean
        "DP02_0017E": "avg_family_size",  # no PE — already a mean
        # Toggled variables (PE = % of relevant universe; E = raw count)
        f"DP02_0018{suffix}": "pop_in_households",
        f"DP02_0053{suffix}": "pop_in_school",
        f"DP02_0058{suffix}": "pop_in_college",
        f"DP02_0060{suffix}": "attainment_lt_8th",
        f"DP02_0061{suffix}": "attainment_9th_to_12th",
        f"DP02_0067{suffix}": "attainment_gt_12th",
        f"DP02_0068{suffix}": "attainment_gt_bachelors",
        f"DP02_0070{suffix}": "pop_veterans",
        f"DP02_0089{suffix}": "pop_native",
        f"DP02_0094{suffix}": "pop_foreign_born",
        f"DP02_0096{suffix}": "naturalized_citizen",
        f"DP02_0097{suffix}": "not_citizen",
        f"DP02_0106{suffix}": "foreign_born_europe",
        f"DP02_0107{suffix}": "foreign_born_asia",
        f"DP02_0108{suffix}": "foreign_born_africa",
        f"DP02_0109{suffix}": "foreign_born_oceania",
        f"DP02_0110{suffix}": "foreign_born_latin_america",
        f"DP02_0111{suffix}": "foreign_born_north_america",
        f"DP02_0153{suffix}": "households_with_computer",
        f"DP02_0154{suffix}": "households_with_internet",
    }

    dp03_map = {
        "GEO_ID": "GEO_ID",
        # Always E — averages / incomes / no meaningful percent
        "DP03_0025E": "work_commute_avg_time_minutes",
        "DP03_0062E": "median_household_income_dollars",
        "DP03_0063E": "avg_household_income_dollars",
        # Toggled variables
        f"DP03_0002{suffix}": "pop_in_labor_force",
        f"DP03_0003{suffix}": "civilian_labor_force",
        f"DP03_0004{suffix}": "civilian_labor_force_employed",
        f"DP03_0005{suffix}": "civilian_labor_force_unemployed",
        f"DP03_0006{suffix}": "armed_forces",
        f"DP03_0007{suffix}": "pop_not_in_labor_force",
        f"DP03_0019{suffix}": "work_commute_drove_alone",
        f"DP03_0020{suffix}": "work_commute_carpooled",
        f"DP03_0021{suffix}": "work_commute_public_transport",
        f"DP03_0022{suffix}": "work_commute_walked",
        f"DP03_0023{suffix}": "work_commute_other",
        f"DP03_0024{suffix}": "work_commute_from_home",
        f"DP03_0047{suffix}": "worker_class_private_or_salary",
        f"DP03_0048{suffix}": "worker_class_government",
        f"DP03_0049{suffix}": "worker_class_self_employed",
        f"DP03_0050{suffix}": "worker_class_unpaid_family",
        f"DP03_0095{suffix}": "pop_noninstitutionalized",
        f"DP03_0096{suffix}": "pop_with_health_insurance",
        f"DP03_0099{suffix}": "pop_without_health_insurance",
        f"DP03_0128{suffix}": "pop_below_poverty",
    }

    dp05_map = {
        "GEO_ID": "GEO_ID",
        # Always E — need raw total for reference even in PE mode
        "DP05_0001E": "pop_total",
        "DP05_0002E": "pop_male",
        "DP05_0003E": "pop_female",
        # Toggled variables
        f"DP05_0090{suffix}": "pop_hispanic_alone",
        f"DP05_0096{suffix}": "pop_white_alone",
        f"DP05_0097{suffix}": "pop_black_alone",
        f"DP05_0098{suffix}": "pop_american_indian_alaska_native_alone",
        f"DP05_0100{suffix}": "pop_native_hawaiian_pacific_islander_alone",
        f"DP05_0101{suffix}": "pop_other_race_alone",
        f"DP05_0102{suffix}": "pop_two_or_more_races",
        f"DP05_0106{suffix}": "pop_citizen_over_18",
        f"DP05_0107{suffix}": "pop_citizen_over_18_male",
        f"DP05_0108{suffix}": "pop_citizen_over_18_female",
    }

    print(
        f"  Mode: {'PE (Census percent variables)' if USE_PERCENT else 'E (raw count variables)'}"
    )

    raw_dp02 = _fetch_census_table("DP02")
    raw_dp03 = _fetch_census_table("DP03")
    raw_dp05 = _fetch_census_table("DP05")

    df02 = _select_rename(raw_dp02, dp02_map)
    df03 = _select_rename(raw_dp03, dp03_map)
    df05 = _select_rename(raw_dp05, dp05_map)

    # Merge on GEO_ID
    df = df02.merge(df03, on="GEO_ID", how="outer")
    df = df.merge(df05, on="GEO_ID", how="outer")

    print(f"  Combined Census rows before cleaning: {len(df)}")

    # --- Cleaning: keep only true county-level rows ---
    # All county rows have GEO_ID starting with '0500000US' (summary level 050)
    df = df[df["GEO_ID"].str.startswith("0500000US", na=False)].copy()

    # Extract the 5-digit full FIPS and 3-digit county FIPS
    df["full_fips"] = df["GEO_ID"].str[-5:]
    df["state_fips"] = df["full_fips"].str[:2]
    df["county_fips_3"] = df["full_fips"].str[2:]

    # Drop independent cities and other non-county sub-divisions.
    # Virginia independent cities have county FIPS >= 510.
    # Other states' non-county places that sneak in also tend to have high codes.
    # Real county FIPS go up to ~840 in Virginia; we keep <= 840.
    # DC has county FIPS 001 and is kept (it IS a county equivalent).
    county_fips_int = pd.to_numeric(df["county_fips_3"], errors="coerce")
    df = df[county_fips_int <= 840].copy()

    # Alaska: keep only the known borough/census-area FIPS; drop subareas
    ak_mask = df["state_fips"] == "02"
    ak_valid = df.loc[ak_mask, "county_fips_3"].isin(ALASKA_COUNTY_FIPS)
    df = df[~ak_mask | ak_valid].copy()

    # Connecticut switched from counties to planning regions in 2022 ACS.
    # Their new planning-region codes are still summary level 050 so they
    # will appear normally; no special handling needed.

    # Drop Puerto Rico rows.
    df = df.drop(df[df["state"] == 72].index)

    # Derived variables (only meaningful in E/count mode;
    # in PE mode the Census already provides correct percent denominators
    # so we skip combining raw counts into new totals)
    if not USE_PERCENT:
        df = _calculate_derived(df)

    # Drop internal helper columns
    df = df.drop(columns=["county_fips_3"], errors="ignore")

    print(f"  Census rows after cleaning:           {len(df)}")
    return df


def _calculate_derived(df: pd.DataFrame) -> pd.DataFrame:
    """Create composite variables from raw counts."""
    df = df.copy()

    # School-age population (in school but not college)
    if {"pop_in_school", "pop_in_college"}.issubset(df.columns):
        s = pd.to_numeric(df["pop_in_school"], errors="coerce")
        c = pd.to_numeric(df["pop_in_college"], errors="coerce")
        df["pop_in_school_not_college"] = s - c

    # Less-than-high-school attainment
    if {"attainment_lt_8th", "attainment_9th_to_12th"}.issubset(df.columns):
        a = pd.to_numeric(df["attainment_lt_8th"], errors="coerce")
        b = pd.to_numeric(df["attainment_9th_to_12th"], errors="coerce")
        df["attainment_lt_12th_no_diploma"] = a + b
        df = df.drop(columns=["attainment_lt_8th", "attainment_9th_to_12th"])

    return df


# NOTE: No manual percent conversion is done here.
# When USE_PERCENT = True, percent variables are pulled directly from the
# Census API (PE codes) using the correct survey-weighted denominators.
# When USE_PERCENT = False, raw count (E) variables are pulled instead and
# no conversion is applied — add your own normalization in post-processing
# if needed, or set USE_PERCENT back to True.


# ---------------------------------------------------------------------------
# STEP 2b — Gini index (ACS Detailed Table B19083)
# ---------------------------------------------------------------------------


def fetch_gini(year: int = CENSUS_YEAR) -> pd.DataFrame:
    """
    Pull the Gini coefficient of income inequality from ACS Detailed Table B19083.
    Returns a DataFrame with GEO_ID and gini_index columns.
    The Gini index ranges from 0 (perfect equality) to 1 (perfect inequality).
    This is always an E variable — there is no PE equivalent.
    """
    print("\n" + "=" * 60)
    print("STEP 2b — Fetching Gini index (B19083)")
    print("=" * 60)

    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get=GEO_ID,B19083_001E&for=county:*&in=state:*"
    )
    print("  Fetching B19083 (Gini index)...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data[1:], columns=data[0])

    df = df[df["GEO_ID"].str.startswith("0500000US", na=False)].copy()
    df = df[["GEO_ID", "B19083_001E"]].rename(columns={"B19083_001E": "gini_index"})
    df["gini_index"] = pd.to_numeric(df["gini_index"], errors="coerce")

    print(f"  Gini index rows: {len(df)}")
    return df


# ---------------------------------------------------------------------------
# STEP 2c — Republican vote share (MIT Election Lab via Harvard Dataverse)
# ---------------------------------------------------------------------------


def fetch_republican_vote(election_year: int = ELECTION_YEAR) -> pd.DataFrame:
    """
    Download county-level presidential returns from the MIT Election Lab
    (Harvard Dataverse, doi:10.7910/DVN/VOQCHQ) and compute the Republican
    candidate's share of the total vote for the specified election year.

    The file is tab-separated and contains columns:
      year, state, state_po, county_name, county_fips, office, candidate,
      party, candidatevotes, totalvotes, version, mode

    Returns a DataFrame with GEO_ID and pct_republican_vote columns.

    NOTE on undocumented immigrants:
      No public county-level API exists for undocumented immigrant counts.
      The Migration Policy Institute (MPI) publishes estimates for only ~287
      counties. For full national coverage, use the 'not_citizen' variable
      already in the dataset as the closest ACS proxy — it captures non-citizens
      which heavily overlaps with the undocumented population.
    """
    print("\n" + "=" * 60)
    print(f"STEP 2c — Fetching Republican vote share ({election_year})")
    print("=" * 60)
    print("  NOTE on undocumented immigrants: No county-level public API exists.")
    print("  Use 'not_citizen' (already in dataset) as the best ACS-based proxy.")
    print(f"  Downloading MIT Election Lab county presidential returns...")

    df = pd.read_csv("./countypres_2000-2024.csv", low_memory=False)

    # Filter to the target election year and REPUBLICAN party
    df = df[df["year"] == election_year].copy()

    # Filter to TOTAL mode
    df = df[df["mode"].str.upper().str.strip() == "TOTAL"].copy()

    # Normalise party label — the dataset uses 'REPUBLICAN' in uppercase
    df["party"] = df["party"].str.upper().str.strip()

    # county_fips in this dataset is numeric; pad to 5 digits for GEO_ID
    df["county_fips"] = (
        pd.to_numeric(df["county_fips"], errors="coerce")
        .dropna()
        .astype(int)
        .astype(str)
        .str.zfill(5)
    )
    df["GEO_ID"] = "0500000US" + df["county_fips"]

    # Get total votes per county (same for all candidates in the county)
    # and Republican votes per county
    rep_votes = (
        df[df["party"] == "REPUBLICAN"]
        .groupby("GEO_ID", as_index=False)["candidatevotes"]
        .sum()
        .rename(columns={"candidatevotes": "rep_votes"})
    )
    total_votes = df.groupby("GEO_ID", as_index=False)[
        "totalvotes"
    ].first()  # totalvotes is the same for all rows within a county

    result = rep_votes.merge(total_votes, on="GEO_ID", how="left")
    result["pct_republican_vote"] = (
        result["rep_votes"] / result["totalvotes"] * 100
    ).round(4)
    result = result[["GEO_ID", "pct_republican_vote"]]

    print(f"  Counties with Republican vote data: {len(result)}")
    return result


def merge_and_export(
    df_census: pd.DataFrame,
    df_alpr: pd.DataFrame,
    df_gini: pd.DataFrame,
    df_repvote: pd.DataFrame,
    output_file: str = "alpr_census_merged.csv",
) -> pd.DataFrame:
    """
    Left-join Census (all ~3,144 counties) onto ALPR data.
    Counties with no ALPR records get alpr_total = 0.
    """
    print("\n" + "=" * 60)
    print("STEP 3 — Merging and exporting")
    print("=" * 60)

    # Aggregate ALPR: sum totals per GEO_ID in case a county appears more than once
    if not df_alpr.empty and "GEO_ID" in df_alpr.columns:
        alpr_agg = (
            df_alpr[df_alpr["GEO_ID"].notna()]
            .groupby("GEO_ID", as_index=False)["alpr_total"]
            .sum()
        )
    else:
        alpr_agg = pd.DataFrame(columns=["GEO_ID", "alpr_total"])

    # Merge: Census is the master list
    df = df_census.merge(alpr_agg, on="GEO_ID", how="left")
    df["alpr_total"] = df["alpr_total"].fillna(0).astype(int)

    # Join Gini index
    if not df_gini.empty:
        df = df.merge(df_gini, on="GEO_ID", how="left")

    # Join Republican vote share
    if not df_repvote.empty:
        df = df.merge(df_repvote, on="GEO_ID", how="left")

    # Reorder: identifiers first, response variable second, predictors last
    id_cols = ["GEO_ID", "county_name", "state", "county", "state_fips", "full_fips"]
    resp_col = ["alpr_total"]
    other_cols = [c for c in df.columns if c not in id_cols + resp_col]

    front = [c for c in id_cols if c in df.columns]
    df = df[front + resp_col + other_cols]

    # Convert all remaining object columns to numeric where possible
    for col in df.columns:
        if col not in {
            "GEO_ID",
            "county_name",
            "state",
            "county",
            "state_fips",
            "full_fips",
        }:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.to_csv(output_file, index=False)

    print(f"\n{'='*60}")
    print("DONE")
    print(f"{'='*60}")
    print(f"Output file : {output_file}")
    print(f"Rows        : {len(df)}  (target ~3,144)")
    print(f"Columns     : {len(df.columns)}")
    print(f"ALPR total  : {df['alpr_total'].sum():,}")
    print(f"Counties with 0 ALPRs: {(df['alpr_total'] == 0).sum()}")

    unmatched = (
        df[df["pop_total"].isna()] if "pop_total" in df.columns else pd.DataFrame()
    )
    if not unmatched.empty:
        print(
            f"\nWARNING: {len(unmatched)} rows missing Census pop_total — check GEO_ID alignment."
        )

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    df_alpr = fetch_all_alpr()
    df_census = fetch_census_data()
    df_gini = fetch_gini()
    df_repvote = fetch_republican_vote()
    df_final = merge_and_export(df_census, df_alpr, df_gini, df_repvote)
    return df_final


if __name__ == "__main__":
    main()

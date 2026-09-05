"""
ACS Census Data Puller
Pulls specified variables from ACS 5-year estimates and combines them into a single CSV file.
"""

import requests
import pandas as pd
from typing import Dict


def fetch_census_table(table_name: str, year: int = 2024) -> pd.DataFrame:
    """
    Fetch a specific ACS data profile table.

    Args:
        table_name: The table name (e.g., 'DP02', 'DP03', 'DP05')
        year: The year of the ACS data (default: 2024)

    Returns:
        DataFrame with the fetched data
    """
    url = f"https://api.census.gov/data/{year}/acs/acs5/profile?get=group({table_name})&for=county:*&in=state:*"

    print(f"Fetching {table_name} data...")
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    # First row contains headers
    df = pd.DataFrame(data[1:], columns=data[0])

    print(f"Successfully fetched {table_name}: {len(df)} rows")
    return df


def ensure_geo_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure GEO_ID column exists (it should come from the API response).

    Args:
        df: DataFrame with census data

    Returns:
        DataFrame with GEO_ID column
    """
    # The API returns GEO_ID in the format like '0500000US01001'
    # If for some reason it's missing, create it from state and county
    if 'GEO_ID' not in df.columns:
        print("Warning: GEO_ID not found in response, creating from state+county")
        df['GEO_ID'] = df['state'] + df['county']
    return df


def select_and_rename_columns(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Select specific columns and rename them.

    Args:
        df: Source DataFrame
        column_mapping: Dictionary mapping original column names to new names

    Returns:
        DataFrame with selected and renamed columns
    """
    # Select only columns that exist in the dataframe
    existing_cols = [col for col in column_mapping.keys() if col in df.columns]
    df_selected = df[existing_cols].copy()

    # Rename columns
    rename_dict = {col: column_mapping[col] for col in existing_cols}
    df_selected = df_selected.rename(columns=rename_dict)

    return df_selected


def calculate_derived_variables(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate derived variables from existing columns.

    Args:
        df: DataFrame with census variables

    Returns:
        DataFrame with added derived variables
    """
    df = df.copy()

    # Population in school not in college or graduate
    if 'pop_in_school' in df.columns and 'pop_in_college' in df.columns:
        pop_school = pd.to_numeric(df['pop_in_school'], errors='coerce')
        pop_college = pd.to_numeric(df['pop_in_college'], errors='coerce')
        df['pop_in_school_not_college'] = pop_school - pop_college

    # Educational Attainment, less than 12th no diploma
    if 'attainment_lt_8th' in df.columns and 'attainment_9th_to_12th' in df.columns:
        attain_lt_8 = pd.to_numeric(df['attainment_lt_8th'], errors='coerce')
        attain_9_12 = pd.to_numeric(
            df['attainment_9th_to_12th'], errors='coerce')
        df['attainment_lt_12th_no_diploma'] = attain_lt_8 + attain_9_12
        # Remove the intermediate columns after creating the derived variable
        df = df.drop(columns=['attainment_lt_8th', 'attainment_9th_to_12th'])

    return df


def main():
    """Main function to orchestrate the data pull and processing."""

    # Define column mappings for each table
    dp02_mapping = {
        'GEO_ID': 'GEO_ID',
        'state': 'state',
        'county': 'county',
        'NAME': 'county_name',
        'DP02_0016E': 'avg_household_size',
        'DP02_0017E': 'avg_family_size',
        'DP02_0018E': 'pop_in_households',
        'DP02_0053E': 'pop_in_school',
        'DP02_0058E': 'pop_in_college',
        'DP02_0060E': 'attainment_lt_8th',
        'DP02_0061E': 'attainment_9th_to_12th',
        'DP02_0067E': 'attainment_gt_12th',
        'DP02_0068E': 'attainment_gt_bachelors',
        'DP02_0070E': 'pop_veterans',
        'DP02_0089E': 'pop_native',
        'DP02_0094E': 'pop_foreign_born',
        'DP02_0096E': 'naturalized_citizen',
        'DP02_0097E': 'not_citizen',
        'DP02_0106E': 'foreign_born_europe',
        'DP02_0107E': 'foreign_born_asia',
        'DP02_0108E': 'foreign_born_africa',
        'DP02_0109E': 'foreign_born_oceania',
        'DP02_0110E': 'foreign_born_latin_america',
        'DP02_0111E': 'foreign_born_north_america',
        'DP02_0153E': 'households_with_computer',
        'DP02_0154E': 'households_with_internet'
    }

    dp03_mapping = {
        'GEO_ID': 'GEO_ID',
        'DP03_0002E': 'pop_in_labor_force',
        'DP03_0003E': 'civilian_labor_force',
        'DP03_0004E': 'civilian_labor_force_employed',
        'DP03_0005E': 'civilian_labor_force_unemployed',
        'DP03_0006E': 'armed_forces',
        'DP03_0007E': 'pop_not_in_labor_force',
        'DP03_0019E': 'work_commute_drove_alone',
        'DP03_0020E': 'work_commute_carpooled',
        'DP03_0021E': 'work_commute_public_transport',
        'DP03_0022E': 'work_commute_walked',
        'DP03_0023E': 'work_commute_other',
        'DP03_0024E': 'work_commute_from_home',
        'DP03_0025E': 'work_commute_avg_time_minutes',
        'DP03_0047E': 'worker_class_private_or_salary',
        'DP03_0048E': 'worker_class_government',
        'DP03_0049E': 'worker_class_self_employed',
        'DP03_0050E': 'worker_class_unpaid_family',
        'DP03_0062E': 'median_household_income_dollars',
        'DP03_0063E': 'avg_household_income_dollars',
        'DP03_0095E': 'pop_noninstitutionalized',
        'DP03_0096E': 'pop_with_health_insurance',
        'DP03_0099E': 'pop_without_health_insurance'
    }

    dp05_mapping = {
        'GEO_ID': 'GEO_ID',
        'DP05_0001E': 'pop_total',
        'DP05_0002E': 'pop_male',
        'DP05_0003E': 'pop_female',
        'DP05_0090E': 'pop_hispanic_alone',
        'DP05_0096E': 'pop_white_alone',
        'DP05_0097E': 'pop_black_alone',
        'DP05_0098E': 'pop_american_indian_alaska_native_alone',
        'DP05_0100E': 'pop_native_hawaiian_pacific_islander_alone',
        'DP05_0101E': 'pop_other_race_alone',
        'DP05_0102E': 'pop_two_or_more_races',
        'DP05_0106E': 'pop_citizen_over_18',
        'DP05_0107E': 'pop_citizen_over_18_male',
        'DP05_0108E': 'pop_citizen_over_18_female'
    }

    try:
        # Fetch data from each table
        df_dp02 = fetch_census_table('DP02')
        df_dp03 = fetch_census_table('DP03')
        df_dp05 = fetch_census_table('DP05')

        # Ensure GEO_ID exists for each table (comes from API)
        df_dp02 = ensure_geo_id(df_dp02)
        df_dp03 = ensure_geo_id(df_dp03)
        df_dp05 = ensure_geo_id(df_dp05)

        # Select and rename columns
        df_dp02_clean = select_and_rename_columns(df_dp02, dp02_mapping)
        df_dp03_clean = select_and_rename_columns(df_dp03, dp03_mapping)
        df_dp05_clean = select_and_rename_columns(df_dp05, dp05_mapping)

        # Merge tables on GEO_ID
        print("\nMerging tables...")
        df_combined = df_dp02_clean.merge(
            df_dp03_clean, on='GEO_ID', how='outer')
        df_combined = df_combined.merge(
            df_dp05_clean, on='GEO_ID', how='outer')

        # Calculate derived variables
        print("Calculating derived variables...")
        df_combined = calculate_derived_variables(df_combined)

        # Reorder columns to put identifying info first
        id_cols = ['GEO_ID', 'state', 'county', 'county_name']
        other_cols = [col for col in df_combined.columns if col not in id_cols]
        df_combined = df_combined[id_cols + other_cols]

        # Export to CSV
        output_file = 'census_data_combined.csv'
        df_combined.to_csv(output_file, index=False)
        print(f"\n✓ Data successfully exported to {output_file}")
        print(f"  Total rows: {len(df_combined)}")
        print(f"  Total columns: {len(df_combined.columns)}")

        return df_combined

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Census API: {e}")
        raise
    except Exception as e:
        print(f"Error processing data: {e}")
        raise


if __name__ == "__main__":
    df = main()

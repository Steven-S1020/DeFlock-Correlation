"""
Join Census Data with Totals CSV
Performs a left join to combine census data with a totals file.
"""

import pandas as pd
import sys

def load_and_clean_totals(totals_filepath: str) -> pd.DataFrame:
    """
    Load the totals CSV and clean it for joining.
    
    Args:
        totals_filepath: Path to the totals CSV file
    
    Returns:
        Cleaned DataFrame with GEO_ID ready for joining
    """
    print(f"Loading totals data from {totals_filepath}...")
    df_totals = pd.read_csv(totals_filepath)
    
    # Remove the first column if it's just row numbers (from R)
    # Check if the first column is unnamed or named like 'Unnamed: 0' or just numbers
    first_col = df_totals.columns[0]
    if first_col in ['Unnamed: 0', ''] or first_col.isdigit():
        df_totals = df_totals.drop(columns=[first_col])
        print(f"Removed row index column: {first_col}")
    
    # Ensure full_fips is a string and formatted properly
    # The census GEO_ID format is like '0500000US01035'
    # We need to convert full_fips to match this format
    if 'full_fips' in df_totals.columns:
        # Convert to string and pad with zeros if needed
        df_totals['full_fips'] = df_totals['full_fips'].astype(str).str.zfill(5)
        # Create GEO_ID to match census format: '0500000US' + FIPS code
        df_totals['GEO_ID'] = '0500000US' + df_totals['full_fips']
        print(f"Created GEO_ID column from full_fips")
    else:
        raise ValueError("'full_fips' column not found in totals file")
    
    print(f"Totals data loaded: {len(df_totals)} rows")
    return df_totals

def load_census_data(census_filepath: str) -> pd.DataFrame:
    """
    Load the census data CSV.
    
    Args:
        census_filepath: Path to the census data CSV file
    
    Returns:
        Census DataFrame
    """
    print(f"Loading census data from {census_filepath}...")
    df_census = pd.read_csv(census_filepath)
    print(f"Census data loaded: {len(df_census)} rows, {len(df_census.columns)} columns")
    return df_census

def perform_left_join(df_totals: pd.DataFrame, df_census: pd.DataFrame) -> pd.DataFrame:
    """
    Perform a left join of totals with census data on GEO_ID.
    
    Args:
        df_totals: Totals DataFrame (left table)
        df_census: Census DataFrame (right table)
    
    Returns:
        Merged DataFrame
    """
    print("\nPerforming left join on GEO_ID...")
    
    # Check if GEO_ID exists in both dataframes
    if 'GEO_ID' not in df_totals.columns:
        raise ValueError("GEO_ID column not found in totals data")
    if 'GEO_ID' not in df_census.columns:
        raise ValueError("GEO_ID column not found in census data")
    
    # Perform left join
    df_merged = df_totals.merge(df_census, on='GEO_ID', how='left')
    
    print(f"Merged data: {len(df_merged)} rows")
    
    # Check for any unmatched rows
    unmatched = df_merged[df_merged['pop_total'].isna()]
    if len(unmatched) > 0:
        print(f"Warning: {len(unmatched)} rows from totals did not match census data")
        print("Unmatched counties:")
        print(unmatched[['name', 'state', 'full_fips', 'GEO_ID']].head(10))
    
    return df_merged

def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder columns to put identifying information first.
    
    Args:
        df: Merged DataFrame
    
    Returns:
        DataFrame with reordered columns
    """
    # Define the order of identifying columns we want at the front
    id_cols_order = ['GEO_ID', 'name', 'state', 'state_fips', 'full_fips', 'total']
    
    # Get columns that exist in the dataframe from our preferred order
    front_cols = [col for col in id_cols_order if col in df.columns]
    
    # Get all other columns
    other_cols = [col for col in df.columns if col not in front_cols]
    
    # Reorder: front columns first, then everything else
    df = df[front_cols + other_cols]
    
    print(f"Reordered columns - first columns: {front_cols[:6]}")
    return df

def clean_merged_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean up the merged data by handling duplicate columns.
    
    Args:
        df: Merged DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    # The census data has 'state', 'county', and 'county_name' columns
    # We want to keep the ones from our totals file and remove the census ones
    
    # Remove state and county from census data (we have state_fips and full_fips from totals)
    columns_to_remove = ['state', 'county', 'county_name']
    
    for col in columns_to_remove:
        if col in df.columns:
            df = df.drop(columns=[col])
            print(f"Removed duplicate column from census data: {col}")
    
    # Handle _x and _y suffix columns if they exist (from duplicate column names)
    for col in df.columns:
        if col.endswith('_x'):
            base_col = col[:-2]
            if f'{base_col}_y' in df.columns:
                # Keep the _x version (from totals) and rename it
                df[base_col] = df[col]
                df = df.drop(columns=[col, f'{base_col}_y'])
                print(f"Resolved duplicate column: {base_col}")
    
    return df

def main(totals_filepath: str, census_filepath: str, output_filepath: str):
    """
    Main function to join totals and census data.
    
    Args:
        totals_filepath: Path to totals CSV file
        census_filepath: Path to census data CSV file
        output_filepath: Path for output CSV file
    """
    try:
        # Load data
        df_totals = load_and_clean_totals(totals_filepath)
        df_census = load_census_data(census_filepath)
        
        # Perform join
        df_merged = perform_left_join(df_totals, df_census)
        
        # Clean merged data (remove duplicate columns)
        df_merged = clean_merged_data(df_merged)
        
        # Reorder columns to put identifying info first
        df_merged = reorder_columns(df_merged)
        
        # Export to CSV
        df_merged.to_csv(output_filepath, index=False)
        print(f"\n✓ Data successfully exported to {output_filepath}")
        print(f"  Total rows: {len(df_merged)}")
        print(f"  Total columns: {len(df_merged.columns)}")
        
        # Show first few rows
        print("\nFirst few rows of merged data:")
        print(df_merged.head())
        
        return df_merged
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    # Default file paths - modify these as needed
    totals_file = "totals.csv"  # Your CSV with the 'total' column
    census_file = "census_data_combined.csv"  # Output from census_data_puller.py
    output_file = "merged_census_data.csv"  # Final output
    
    # You can also accept command line arguments
    if len(sys.argv) == 4:
        totals_file = sys.argv[1]
        census_file = sys.argv[2]
        output_file = sys.argv[3]
    elif len(sys.argv) > 1:
        print("Usage: python join_census_data.py [totals.csv] [census_data_combined.csv] [output.csv]")
        sys.exit(1)
    
    print(f"Totals file: {totals_file}")
    print(f"Census file: {census_file}")
    print(f"Output file: {output_file}\n")
    
    df = main(totals_file, census_file, output_file)

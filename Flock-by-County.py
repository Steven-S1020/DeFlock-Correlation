import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import requests
    import time
    import csv
    return csv, requests, time


@app.cell
def _():
    # All 50 US states + DC with their FIPS codes
    STATES = {
        'AL': {'name': 'Alabama', 'fips': '01'},
        'AK': {'name': 'Alaska', 'fips': '02'},
        'AZ': {'name': 'Arizona', 'fips': '04'},
        'AR': {'name': 'Arkansas', 'fips': '05'},
        'CA': {'name': 'California', 'fips': '06'},
        'CO': {'name': 'Colorado', 'fips': '08'},
        'CT': {'name': 'Connecticut', 'fips': '09'},
        'DE': {'name': 'Delaware', 'fips': '10'},
        'DC': {'name': 'District of Columbia', 'fips': '11'},
        'FL': {'name': 'Florida', 'fips': '12'},
        'GA': {'name': 'Georgia', 'fips': '13'},
        'HI': {'name': 'Hawaii', 'fips': '15'},
        'ID': {'name': 'Idaho', 'fips': '16'},
        'IL': {'name': 'Illinois', 'fips': '17'},
        'IN': {'name': 'Indiana', 'fips': '18'},
        'IA': {'name': 'Iowa', 'fips': '19'},
        'KS': {'name': 'Kansas', 'fips': '20'},
        'KY': {'name': 'Kentucky', 'fips': '21'},
        'LA': {'name': 'Louisiana', 'fips': '22'},
        'ME': {'name': 'Maine', 'fips': '23'},
        'MD': {'name': 'Maryland', 'fips': '24'},
        'MA': {'name': 'Massachusetts', 'fips': '25'},
        'MI': {'name': 'Michigan', 'fips': '26'},
        'MN': {'name': 'Minnesota', 'fips': '27'},
        'MS': {'name': 'Mississippi', 'fips': '28'},
        'MO': {'name': 'Missouri', 'fips': '29'},
        'MT': {'name': 'Montana', 'fips': '30'},
        'NE': {'name': 'Nebraska', 'fips': '31'},
        'NV': {'name': 'Nevada', 'fips': '32'},
        'NH': {'name': 'New Hampshire', 'fips': '33'},
        'NJ': {'name': 'New Jersey', 'fips': '34'},
        'NM': {'name': 'New Mexico', 'fips': '35'},
        'NY': {'name': 'New York', 'fips': '36'},
        'NC': {'name': 'North Carolina', 'fips': '37'},
        'ND': {'name': 'North Dakota', 'fips': '38'},
        'OH': {'name': 'Ohio', 'fips': '39'},
        'OK': {'name': 'Oklahoma', 'fips': '40'},
        'OR': {'name': 'Oregon', 'fips': '41'},
        'PA': {'name': 'Pennsylvania', 'fips': '42'},
        'RI': {'name': 'Rhode Island', 'fips': '44'},
        'SC': {'name': 'South Carolina', 'fips': '45'},
        'SD': {'name': 'South Dakota', 'fips': '46'},
        'TN': {'name': 'Tennessee', 'fips': '47'},
        'TX': {'name': 'Texas', 'fips': '48'},
        'UT': {'name': 'Utah', 'fips': '49'},
        'VT': {'name': 'Vermont', 'fips': '50'},
        'VA': {'name': 'Virginia', 'fips': '51'},
        'WA': {'name': 'Washington', 'fips': '53'},
        'WV': {'name': 'West Virginia', 'fips': '54'},
        'WI': {'name': 'Wisconsin', 'fips': '55'},
        'WY': {'name': 'Wyoming', 'fips': '56'},
    }

    OVERPASS_URL = "https://overpass-api.de/api/interpreter"
    MAX_RETRIES = 5  # Maximum number of retry attempts
    RETRY_DELAY = 1  # Seconds to wait between retries
    return MAX_RETRIES, OVERPASS_URL, RETRY_DELAY, STATES


@app.function
def build_query(state_code):
    """Build the Overpass QL query for a given state"""
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


@app.function
def normalize_fips(fips_str):
    """
    Normalize FIPS code to 2 digits with leading zero if needed.
    Returns None if fips_str is empty or None.
    """
    if not fips_str or fips_str.strip() == '':
        return None
    
    # Remove any whitespace
    fips_str = fips_str.strip()
    
    # If it's a single digit, add leading zero
    if len(fips_str) == 1:
        return '0' + fips_str
    
    # If it's already 2 digits, return as is
    if len(fips_str) == 2:
        return fips_str
    
    # If it's part of a full county FIPS (5 digits), take first 2
    if len(fips_str) == 5:
        return fips_str[:2]
    
    return fips_str


@app.cell
def _(MAX_RETRIES, OVERPASS_URL, RETRY_DELAY, STATES, requests, time):
    def query_state(state_code, expected_fips):
        """
        Query a single state and return the results.
        Retries on timeout until successful.
        Filters by FIPS only if the tag exists.
        """
        print(f"Querying {STATES[state_code]['name']} ({state_code})...")
    
        query = build_query(state_code)
        attempt = 1
    
        while True:
            try:
                print(f"  Attempt {attempt}...")
                response = requests.post(OVERPASS_URL, data={'data': query}, timeout=120)
                response.raise_for_status()
            
                # Parse CSV response
                lines = response.text.strip().split('\n')
                if len(lines) <= 1:  # Only header or empty
                    print(f"  No counties found for {state_code}")
                    return []
            
                # Skip the header line and parse data
                results = []
                filtered_count = 0
                no_fips_count = 0
            
                for line in lines[1:]:  # Skip header
                    parts = line.split('\t')
                
                    # Ensure we have exactly 5 parts
                    if len(parts) != 5:
                        print(f"  ⚠ Skipping malformed line: {line}")
                        continue
                
                    county_name = parts[0] if parts[0] else "NA"
                    state_abbr = parts[1] if parts[1] else state_code
                    state_fips_raw = parts[2]
                    county_fips_raw = parts[3]
                    total_count = parts[4] if parts[4] else "0"
                
                    # Normalize the state FIPS if present
                    state_fips_normalized = normalize_fips(state_fips_raw)
                
                    # Determine display values - keep "NA" for missing FIPS
                    state_fips_display = state_fips_raw if state_fips_raw else "NA"
                    county_fips_display = county_fips_raw if county_fips_raw else "NA"
                
                    # Filter logic:
                    # - If state FIPS exists and doesn't match expected, filter out
                    # - If state FIPS doesn't exist, include with "NA" so user can verify manually
                    if state_fips_normalized is not None:
                        if state_fips_normalized != expected_fips:
                            filtered_count += 1
                            print(f"  ⚠ Filtered out: {county_name} (state FIPS: {state_fips_raw} normalized to {state_fips_normalized}, expected {expected_fips})")
                            continue
                    else:
                        # No FIPS tag present, include with "NA" for manual verification
                        no_fips_count += 1
                
                    # Add to results
                    results.append({
                        'name': county_name,
                        'state': state_abbr,
                        'nist:state_fips': state_fips_display,
                        'nist:fips_code': county_fips_display,
                        'total': total_count
                    })
            
                print(f"  ✓ Found {len(results)} valid counties")
                if filtered_count > 0:
                    print(f"  ⚠ Filtered out {filtered_count} counties with incorrect FIPS")
                if no_fips_count > 0:
                    print(f"  ℹ {no_fips_count} counties had no FIPS tag (marked as 'NA' for manual review)")
            
                return results
            
            except requests.exceptions.Timeout:
                print(f"  ⏱ TIMEOUT on attempt {attempt}")
                if attempt >= MAX_RETRIES:
                    print(f"  ❌ Max retries ({MAX_RETRIES}) reached for {state_code}. Skipping this state.")
                    return []
                attempt += 1
                print(f"  Waiting {RETRY_DELAY} seconds before retry...")
                time.sleep(RETRY_DELAY)
            
            except requests.exceptions.RequestException as e:
                print(f"  ❌ REQUEST ERROR for {state_code}: {e}")
                if attempt >= MAX_RETRIES:
                    print(f"  ❌ Max retries ({MAX_RETRIES}) reached for {state_code}. Skipping this state.")
                    return []
                attempt += 1
                print(f"  Waiting {RETRY_DELAY} seconds before retry...")
                time.sleep(RETRY_DELAY)
            
            except Exception as e:
                print(f"  ❌ UNEXPECTED ERROR for {state_code}: {e}")
                # For unexpected errors, don't retry - skip the state
                return []
    return (query_state,)


@app.cell
def _(MAX_RETRIES, RETRY_DELAY, STATES, csv, query_state, time):
    def main():
        """Query all states and combine results"""
        print("="*60)
        print("ALPR County Counter - Starting")
        print("="*60)
        print(f"Querying {len(STATES)} states/territories")
        print(f"Max retries per state: {MAX_RETRIES}")
        print(f"Retry delay: {RETRY_DELAY} seconds")
        print("="*60)
    
        all_results = []
        successful_states = 0
        failed_states = []
    
        for state_code, state_info in STATES.items():
            results = query_state(state_code, state_info['fips'])
        
            if results or results == []:  # Empty list is valid (no counties with cameras)
                all_results.extend(results)
                successful_states += 1
            else:
                failed_states.append(state_code)
        
            # Be nice to the Overpass API - wait between requests
            print(f"  Waiting 2 seconds before next state...\n")
            time.sleep(2)
    
        # Write to CSV
        output_file = 'alpr_counts_by_county.csv'
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['name', 'state', 'nist:state_fips', 'nist:fips_code', 'total'])
            writer.writeheader()
            writer.writerows(all_results)
    
        # Print summary
        print("="*60)
        print("SUMMARY")
        print("="*60)
        print(f"✓ Successfully queried: {successful_states}/{len(STATES)} states")
        if failed_states:
            print(f"❌ Failed states: {', '.join(failed_states)}")
        print(f"✓ Total counties found: {len(all_results)}")
    
        total_cameras = sum(int(r['total']) for r in all_results)
        print(f"✓ Total ALPR cameras: {total_cameras}")
    
        print(f"\n✓ Results written to: {output_file}")
        print("="*60)
    return (main,)


@app.cell
def _(main):
    main()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

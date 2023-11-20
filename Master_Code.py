import pandas as pd
import requests
import time

# Record the start time
start_time = time.time()

"""
Overall structure of the code:
Part 1- Foundation: create get_hud_data as a function to trigger repetitive API requests. Define scope and variables.
Part 2- DataFrames: produce all needed DataFrames before merging.
"""

"""Part 1 - Foundation"""
# Define get_hud_data as a function that retrieves successfully API from https://www.huduser.gov/portal/dataset/fmr-api.html
def get_hud_data(api_endpoint, headers):
    response = requests.get(hud_base_url + api_endpoint, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        return []

# Base URL and headers
hud_base_url = "https://www.huduser.gov/hudapi/public"
hud_token = 'YOUR API HERE'
hud_headers = {"Authorization": "Bearer " + hud_token}

# Define available APIs and years of scope.
hud_available_apis = {
    'all_states': '/fmr/listStates',
    'all_counties': '/fmr/listCounties/',
    'all_metro_areas': '/fmr/listMetroAreas',
    'FMRs': '/fmr/data/',
    'state_FMR': '/fmr/statedata/',
    'zip_codes': '/usps'
}

years_of_scope = [2024, 2023, 2022, 2021, 2020] # Years to pull fmr data.
years_for_zips = [2023, 2022, 2021, 2020, 2019] # Years to pull zip code data.

api_sleep_per_request = 1.1

# Census.gov county and county equivalents manual file.
census_counties_file_path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/counties_from_census_dot_gov.csv'

"""Part 2- DataFrames"""
# Create a State DF
state_df = pd.DataFrame(get_hud_data(hud_available_apis['all_states'], hud_headers))
state_df = state_df[['state_code', 'state_name']]

# Create fmrs_state_df
state_list = list(state_df['state_code'].unique()) # Create a state list to iterate over for API requests.

fmr_state_raw = [] # To collect API requests without cleaning process.
for state in state_list:
    for year in years_of_scope:
        fmr_by_state = get_hud_data(hud_available_apis['state_FMR'] + state + '?year=' + str(year), hud_headers)['data']
        fmr_state_raw.append(fmr_by_state)
        time.sleep(2)

fmr_state_clean = [] # Clean fmr_state_raw
for item in fmr_state_raw:
    item['level'] = 'State'

    for metroarea in item['metroareas']:
        parent_dict = item.copy() # Copy the parent_dict to avoid modifying the original dictionary
        parent_dict.pop('counties', None) # Remove 'counties' key from the parent_dict
        parent_dict.pop('metroareas', None) # Remove 'metroareas' key
        parent_dict['sub_level'] = 'Metro Areas' # Add 'sub_level' key with the value 'Metro Areas' to the parent_dict
        metroarea.update(parent_dict) # Update the metroarea with the values from the parent_dict
        fmr_state_clean.append(metroarea)

    for county in item['counties']:
        parent_dict = item.copy() # Copy the parent_dict to avoid modifying the original dictionary
        parent_dict.pop('counties', None) # Remove 'counties' key from the parent_dict
        parent_dict.pop('metroareas', None) # Remove 'metroareas' key

        # Check if 'town_names' exists in the county
        if 'town_name' in county and county['town_name']:
            parent_dict['sub_level'] = 'Towns' # Add 'sub_level' key with the value 'Towns' to the parent_dict
        else:
            parent_dict['sub_level'] = 'Counties' # Add 'sub_level' key with the value 'Counties' to the parent_dict

        county.update(parent_dict) # Update the county with the values from the parent_dict
        fmr_state_clean.append(county)

fmr_state_df = pd.DataFrame(fmr_state_clean)

# Create fmr_cbsa_df
# Create a cbsa code list to iterate over for API requests.
metro_areas_df = pd.DataFrame(get_hud_data(hud_available_apis['all_metro_areas'], hud_headers))
unique_metro_areas_list = list(metro_areas_df['cbsa_code'].unique())

fmr_raw_cbsa = []
for cbsa_code in unique_metro_areas_list:
    for year in years_of_scope:
        fmr_data_cbsa = get_hud_data(hud_available_apis['FMRs'] + cbsa_code + '?year=' + str(year), hud_headers)

        for item in fmr_data_cbsa:
            # Use 'data' keys only. Add an identifier.
            if 'data' in fmr_data_cbsa:
                parent_dict = fmr_data_cbsa['data']
                parent_dict['cbsa_code'] = cbsa_code
                parent_dict['level'] = 'Zip'
                parent_dict['sub_level'] = 'Zip'
                basicdata = parent_dict.pop('basicdata')

                for entry in basicdata:
                    # Un-nest fmr_raw_cbsa with zip_code data only
                    if 'zip_code' in entry:
                        new_row = parent_dict.copy()
                        new_row.update(entry)
                        fmr_raw_cbsa.append(new_row)

        time.sleep(2)

fmr_cbsa_df = pd.DataFrame(fmr_raw_cbsa)

# Create a Zip Code DF
zips_from_fmr_cbsa = list(fmr_cbsa_df['zip_code'].unique()) # Create zip code list to iterate over for API requests

zip_code_raw = []
for zip in zips_from_fmr_cbsa:
    zip_data_found = False  # Flag to track if data is found for the current zip code
    for year in years_for_zips:
        for quarter in range(4, 0, -1):
            # type=2 gets zip & county_code as a geoid.
            response = get_hud_data(hud_available_apis['zip_codes']\
                        + '?type=' + str(2) + '&query=' + str(zip) + '&year=' + str(year) + '&quarter=' + str(quarter)\
                        , hud_headers)

            if 'data' in response:
                parent_dict = response['data']
                results = parent_dict.pop('results')

                for entry in results:
                    new_row = parent_dict.copy()
                    new_row.update(entry)
                    zip_code_raw.append(new_row)

                zip_data_found = True  # Set the flag to True once data is found
                break  # Exit the quarter loop once data is found

            time.sleep(api_sleep_per_request)

        if zip_data_found:
            break  # Exit the year loop once data is found

zip_code_df = pd.DataFrame(zip_code_raw)
zip_code_df = zip_code_df.loc[zip_code_df.groupby('zip')['tot_ratio'].idxmax()] # zip_code_df contains only the rows with the highest 'tot_ratio' for each zip

# Open county file and create a DataFrame
county_df = pd.read_csv(census_counties_file_path)

# Join counties with zips
county_df['geoid'] = county_df['STATEFP'] + county_df['COUNTYFP']
county_df['county_name'] = county_df['COUNTYNAME']
county_df['state_code'] = county_df['STATE']
zip_n_counties_df = pd.merge(how='left', left=zip_code_df, right=county_df, on='geoid')
zip_n_counties_df['zip_code'] = zip_n_counties_df['zip']
zip_n_counties_df = zip_n_counties_df[['zip_code', 'city', 'county_name', 'state_code']]

# Join zip_n_counties_df with state_df
zip_n_counties_df = pd.merge(how='inner', left=zip_n_counties_df, right=state_df, on='state_code')

# Get clean metro_names in fmr_state
# Get relevant columns to clean.
metro_names_to_merge = metro_areas_df[['cbsa_code', 'area_name']]

# Make a table of all metro names, codes and year uncleaned.
uncleaned_metro_code_year = fmr_state_df[fmr_state_df['sub_level']=='Metro Areas'][['metro_name', 'code', 'year']]

# Take most recent metro_name.
all_metro_names = uncleaned_metro_code_year.drop_duplicates().sort_values(by='year', ascending=False)\
                    .drop_duplicates(subset='code')

# Merge dataframes in order to get priority table vs non-included metro areas shown in fmr_state_df
metro_names_by_code = pd.merge(how='left', left=all_metro_names, left_on='code',\
                              right=metro_names_to_merge, right_on='cbsa_code')

# Make metro area names where prioritizes metro_areas_df names, otherwise use what is shown in fmr_state_df
metro_names_by_code['metro_area_name_by_code'] = metro_names_by_code['area_name'].fillna(metro_names_by_code['metro_name'])
metro_names_by_code = metro_names_by_code[['code', 'metro_area_name_by_code']]

# Create a table for uncleaned metro names.
metro_n_codes_only = uncleaned_metro_code_year.drop_duplicates(subset=['metro_name', 'code']).drop(columns='year')
metro_names_by_name = pd.merge(how='inner', left=metro_n_codes_only, right=metro_names_by_code, on='code')
metro_names_by_name['metro_area_name_by_name'] = metro_names_by_name['metro_area_name_by_code']
metro_names_by_name = metro_names_by_name[['metro_name', 'metro_area_name_by_name']]

# Merge metro area names to fmr_state, and clean fmr_state_df
fmr_state_df = pd.merge(how='left', left=fmr_state_df, right=metro_names_by_code, on='code')
fmr_state_df = pd.merge(how='left', left=fmr_state_df, right=metro_names_by_name, on='metro_name')
fmr_state_df['metro_area_name'] = fmr_state_df['metro_area_name_by_code'].fillna(fmr_state_df['metro_area_name_by_name'])
fmr_state_df['metro_name'] = fmr_state_df['metro_area_name'].fillna(fmr_state_df['metro_name'])
fmr_state_df['cbsa_code'] = fmr_state_df['code']
fmr_state_df['state_name'] = fmr_state_df['statename']
fmr_state_df['state_code'] = fmr_state_df['statecode']

fmr_state_df.drop(columns=\
                 ['metro_area_name_by_code', 'metro_area_name_by_name', 'metro_area_name', 'fips_code'\
                 , 'FMR Percentile', 'code', 'statename', 'statecode'],\
                 inplace=True)

# Get clean metro_names to fmr_cbsa, and clean fmr_cbsa_df
fmr_cbsa_df = pd.merge(how='left', left=fmr_cbsa_df, left_on='cbsa_code', right=metro_names_by_code, right_on='code')
fmr_cbsa_df['metro_name'] = fmr_cbsa_df['metro_area_name_by_code']

fmr_cbsa_df.drop(columns=\
                ['county_name', 'counties_msa', 'town_name', 'code', 'metro_area_name_by_code'],\
                inplace=True)

# Merge fmr_cbsa_df with zip_n_counties_df
fmr_cbsa_df = pd.merge(how='left', left=fmr_cbsa_df, right=zip_n_counties_df, on='zip_code')

# Concatenate fmr_cbsa_df and fmr_state_df as fmr_master_df
fmr_master_df  = pd.concat([fmr_state_df, fmr_cbsa_df], ignore_index=True)

# Record the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time_seconds = end_time - start_time
hours, remainder = divmod(elapsed_time_seconds, 3600)
minutes, seconds = divmod(remainder, 60)

print(f"Elapsed Time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")
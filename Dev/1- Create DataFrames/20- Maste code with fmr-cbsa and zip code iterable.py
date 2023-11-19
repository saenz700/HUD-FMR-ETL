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
hud_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aeiSI6IjBkM2M2NWNjYTc4NmY1ZThjZDliNDdmOTgxNzJlOTk0OWFjYmFlMzFhMmEyZGQ0Yzg0NzI3YWUxZWM1MTE1YmEwYjJkZDA3ZDY5MGNkMWRiIn0.eyJhdWQiOiI2IiwianRpIjoiMGQzYzY1Y2NhNzg2ZjVlOGNkOWI0N2Y5ODE3MmU5OTQ5YWNiYWUzMWEyYTJkZDRjODQ3MjdhZTFlYzUxMTViYTBiMmRkMDdkNjkwY2QxZGIiLCJpYXQiOjE2OTc0MDQ4OTEsIm5iZiI6MTY5NzQwNDg5MSwiZXhwIjoyMDEzMDI0MDkxLCJzdWIiOiI2MDMxNCIsInNjb3BlcyI6W119.hBm626SUPkrLfV2eg31PxJhWX6gUXPJo-N59Y7XcwPg5_IrrzorgyAlJ1qh5cmT6Wqv2Ya4kheKzdAV3CJKnFg"
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

# Save dataframes to CSV
state_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\state_df.csv', index=False)
zip_code_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\zip_code_df.csv', index=False)
fmr_cbsa_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\fmr_cbsa_df.csv', index=False)
fmr_state_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\fmr_state_df.csv', index=False)

# Record the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time_seconds = end_time - start_time
hours, remainder = divmod(elapsed_time_seconds, 3600)
minutes, seconds = divmod(remainder, 60)

print(f"Elapsed Time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")
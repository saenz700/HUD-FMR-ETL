import pandas as pd
import requests
import time

# Define get_hud_data as a function that retrieves successfully API from https://www.huduser.gov/portal/dataset/fmr-api.html
def get_hud_data(api_endpoint, headers):
    response = requests.get(hud_base_url + api_endpoint, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        return []

# Base URL and headers
hud_base_url = "https://www.huduser.gov/hudapi/public"
hud_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjBkM2M2NWNjYTc4NmY1ZThjZDliNDdmOTgxNzJlOTk0OWFjYmFlMzFhMmEyZGQ0Yzg0NzI3YWUxZWM1MTE1YmEwYjJkZDA3ZDY5MGNkMWRiIn0.eyJhdWQiOiI2IiwianRpIjoiMGQzYzY1Y2NhNzg2ZjVlOGNkOWI0N2Y5ODE3MmU5OTQ5YWNiYWUzMWEyYTJkZDRjODQ3MjdhZTFlYzUxMTViYTBiMmRkMDdkNjkwY2QxZGIiLCJpYXQiOjE2OTc0MDQ4OTEsIm5iZiI6MTY5NzQwNDg5MSwiZXhwIjoyMDEzMDI0MDkxLCJzdWIiOiI2MDMxNCIsInNjb3BlcyI6W119.hBm626SUPkrLfV2eg31PxJhWX6gUXPJo-N59Y7XcwPg5_IrrzorgyAlJ1qh5cmT6Wqv2Ya4kheKzdAV3CJKnFg"
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

years_of_scope = [2024, 2023, 2022, 2021, 2020]

api_sleep_per_request = 2

# Create a State DF
state_df = pd.DataFrame(get_hud_data(hud_available_apis['all_states'], hud_headers))

# Create a Zip Code DF
state_list = list(state_df['state_code'].unique()) # Create a state list to iterate over for API requests.

zip_code_df = pd.DataFrame()
for state in state_list:
    zip_code_data = (get_hud_data(hud_available_apis['zip_codes'] + '?type=' + str(2) + '&query=' + state, hud_headers)\
                     ['data']['results'])
    zip_code_df_to_append = pd.DataFrame(zip_code_data)
    zip_code_df = pd.concat([zip_code_df, zip_code_df_to_append], ignore_index=True)

    time.sleep(api_sleep_per_request)

# Create a county list
counties_df = pd.DataFrame()
for state in state_list:
    counties_data = get_hud_data(hud_available_apis['all_counties'] + state, hud_headers)
    counties_df_to_append = pd.DataFrame(counties_data)
    counties_df = pd.concat([counties_df, counties_df_to_append], ignore_index=True)

# Retrieve fmrs by state, and create a fmrs_by_state DF.
fmr_state_raw = []
for state in state_list:
    for year in years_of_scope:
        fmr_by_state = get_hud_data(hud_available_apis['state_FMR'] + state + '?year=' + str(year), hud_headers)['data']
        fmr_state_raw.append(fmr_by_state)
        time.sleep(2) # Sleep to avoid saturating API.

fmr_state_clean = []
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

# Retrieve fmrs using fips list in order to get fmrs by zip code.
fips_code_list = counties_df[['county_name', 'state_code', 'fips_code']].\
    drop_duplicates(subset=['county_name', 'state_code'])['fips_code'].tolist()

fmr_raw_fips = []
for fips_code in fips_code_list:
    for year in years_of_scope:
        fmr_data_fips = get_hud_data(hud_available_apis['FMRs'] + fips_code + '?year=' + str(year), hud_headers)

        for item in fmr_data_fips:
            # Use 'data' keys only. Add an identifier.
            if 'data' in fmr_data_fips:
                parent_dict = fmr_data_fips['data']
                parent_dict['fips_code'] = fips_code
                basicdata = parent_dict.pop('basicdata')

                for entry in basicdata:
                    # Un-nest fmr_raw_fips with zip_code data only
                    if 'zip_code' in entry:
                        new_row = parent_dict.copy()
                        new_row.update(entry)
                        fmr_raw_fips.append(new_row)

    time.sleep(api_sleep_per_request)  # Sleep to avoid saturating API.

fmr_fips_df = pd.DataFrame(fmr_raw_fips)

# Save dataframes to CSV
state_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\state_df.csv', index=False)
zip_code_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\zip_code_df.csv', index=False)
fmr_fips_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\fmr_fips_df.csv', index=False)
fmr_state_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\fmr_state_df.csv', index=False)
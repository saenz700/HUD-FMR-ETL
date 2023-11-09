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

# Define available APIs
hud_available_apis = {
    'all_states': '/fmr/listStates',
    'all_counties': '/fmr/listCounties/',
    'all_metro_areas': '/fmr/listMetroAreas',
    'FMRs': '/fmr/data/',
    'state_FMR': '/fmr/statedata/',
    'zip_codes': '/usps'
}

years_of_scope = [2024, 2023, 2022, 2021, 2020]

# Create a State DF
state_df = pd.DataFrame(get_hud_data(hud_available_apis['all_states'], hud_headers))

# Create a Zip Code DF
state_list = list(state_df['state_code'].unique())

# Retrieve fmrs by state
fmr_raw_state = []
for state in state_list:
    fmr_by_state = get_hud_data(hud_available_apis['state_FMR'] + state, hud_headers)['data']
    fmr_raw_state.append(fmr_by_state)

fmr_by_state_cleaned = []
for item in fmr_raw_state:

    item['level'] = 'State'

    for county in item['counties']:
        # Copy the parent_dict to avoid modifying the original dictionary
        parent_dict = item.copy()
        # Remove 'counties' key from the parent_dict
        parent_dict.pop('counties', None)
        # Remove 'metroareas' key
        parent_dict.pop('metroareas', None)
        # Add 'sub_level' key with the value 'Counties' to the parent_dict
        parent_dict['sub_level'] = 'Counties'
        # Update the county with the values from the parent_dict
        county.update(parent_dict)

        fmr_by_state_cleaned.append(county)

    for metroarea in item['metroareas']:
        # Copy the parent_dict to avoid modifying the original dictionary
        parent_dict = item.copy()
        # Remove 'counties' key from the parent_dict
        parent_dict.pop('counties', None)
        # Remove 'metroareas' key
        parent_dict.pop('metroareas', None)
        # Add 'sub_level' key with the value 'Metro Areas' to the parent_dict
        parent_dict['sub_level'] = 'Metro Areas'
        # Update the metroarea with the values from the parent_dict
        metroarea.update(parent_dict)

        fmr_by_state_cleaned.append(metroarea)

        time.sleep(2)

fmr_by_state_df = pd.DataFrame(fmr_by_state_cleaned)

fmr_by_state_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\fmr_by_state_df.csv', index=False)
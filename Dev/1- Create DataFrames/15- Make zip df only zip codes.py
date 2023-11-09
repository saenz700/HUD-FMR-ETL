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

# Create a State DF
state_df = pd.DataFrame(get_hud_data(hud_available_apis['all_states'], hud_headers))

# Create a Metro Area DF
metro_areas_df = pd.DataFrame(get_hud_data(hud_available_apis['all_metro_areas'], hud_headers))

# Retrieve fmrs using the unique metro areas list in order to get fmrs by zip code.
unique_metro_areas_list = list(metro_areas_df['cbsa_code'].unique())

fmr_raw_cbsa = []
for cbsa_code in unique_metro_areas_list:
    for year in years_of_scope:
        fmr_data_cbsa = get_hud_data(hud_available_apis['FMRs'] + cbsa_code + '?year=' + str(year), hud_headers)

        for item in fmr_data_cbsa:
            if 'data' in fmr_data_cbsa:
                parent_dict = fmr_data_cbsa['data']
                parent_dict['cbsa_code'] = cbsa_code
                basicdata = parent_dict.pop('basicdata')

                for entry in basicdata:
                    if 'zip_code' in entry:
                        new_row = parent_dict.copy()
                        new_row.update(entry)
                        fmr_raw_cbsa.append(new_row)

    time.sleep(2)  # Sleep 2 seconds to avoid saturating API.
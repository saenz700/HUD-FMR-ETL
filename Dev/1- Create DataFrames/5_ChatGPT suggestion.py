import pandas as pd
import requests
import random

# Define get_hud_data as a function that retrieves successfully API from https://www.huduser.gov/portal/dataset/fmr-api.html
def get_hud_data(api_endpoint, headers):
    response = requests.get(hud_base_url + api_endpoint, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data from {api_endpoint}")
        return []

# Base URL and headers
hud_base_url = "https://www.huduser.gov/hudapi/public/fmr"
hud_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjBkM2M2NWNjYTc4NmY1ZThjZDliNDdmOTgxNzJlOTk0OWFjYmFlMzFhMmEyZGQ0Yzg0NzI3YWUxZWM1MTE1YmEwYjJkZDA3ZDY5MGNkMWRiIn0.eyJhdWQiOiI2IiwianRpIjoiMGQzYzY1Y2NhNzg2ZjVlOGNkOWI0N2Y5ODE3MmU5OTQ5YWNiYWUzMWEyYTJkZDRjODQ3MjdhZTFlYzUxMTViYTBiMmRkMDdkNjkwY2QxZGIiLCJpYXQiOjE2OTc0MDQ4OTEsIm5iZiI6MTY5NzQwNDg5MSwiZXhwIjoyMDEzMDI0MDkxLCJzdWIiOiI2MDMxNCIsInNjb3BlcyI6W119.hBm626SUPkrLfV2eg31PxJhWX6gUXPJo-N59Y7XcwPg5_IrrzorgyAlJ1qh5cmT6Wqv2Ya4kheKzdAV3CJKnFg"
hud_headers = {"Authorization": "Bearer " + hud_token}

# Define available APIs
hud_available_apis = {
    'all_states': '/listStates',
    'all_counties': '/listCounties/',
    'all_metro_areas': '/listMetroAreas',
    'FMRs': '/data/'
}

# Create a State DF
state_df = pd.DataFrame(get_hud_data(hud_available_apis['all_states'], hud_headers))

# Create a Metro Area DF
metro_areas_df = pd.DataFrame(get_hud_data(hud_available_apis['all_metro_areas'], hud_headers))
unique_metro_areas_list = list(metro_areas_df['cbsa_code'].unique())
unique_metro_areas_list_sample = random.sample(unique_metro_areas_list, 10)  # Erase when done sandboxing.

# Retrieve FMRs using the unique metro areas list
FMR_raw = []

for metro_areas in unique_metro_areas_list_sample:
    fmr_data_when_200 = get_hud_data(hud_available_apis['FMRs'] + metro_areas, hud_headers)
    FMR_raw.extend(fmr_data_when_200)  # Use extend to add elements from a list

# Un-nest FMR_raw and create a DataFrame of FMR_df
FMR_cleaned = []

for item in FMR_raw:
    parent_dict = item.copy()
    basicdata = parent_dict.pop('basicdata')

    if isinstance(basicdata, list):
        for sub_dict in basicdata:
            parent_dict.update(sub_dict)
    elif isinstance(basicdata, dict):
        parent_dict.update(basicdata)

    FMR_cleaned.append(parent_dict)

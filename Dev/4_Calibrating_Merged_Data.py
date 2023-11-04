import pandas as pd
import requests
import random

# Connect to All DataFrames available in FMR API from https://www.huduser.gov/portal/dataset/fmr-api.html
hud_base_url = "https://www.huduser.gov/hudapi/public/fmr"
hud_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjBkM2M2NWNjYTc4NmY1ZThjZDliNDdmOTgxNzJlOTk0OWFjYmFlMzFhMmEyZGQ0Yzg0NzI3YWUxZWM1MTE1YmEwYjJkZDA3ZDY5MGNkMWRiIn0.eyJhdWQiOiI2IiwianRpIjoiMGQzYzY1Y2NhNzg2ZjVlOGNkOWI0N2Y5ODE3MmU5OTQ5YWNiYWUzMWEyYTJkZDRjODQ3MjdhZTFlYzUxMTViYTBiMmRkMDdkNjkwY2QxZGIiLCJpYXQiOjE2OTc0MDQ4OTEsIm5iZiI6MTY5NzQwNDg5MSwiZXhwIjoyMDEzMDI0MDkxLCJzdWIiOiI2MDMxNCIsInNjb3BlcyI6W119.hBm626SUPkrLfV2eg31PxJhWX6gUXPJo-N59Y7XcwPg5_IrrzorgyAlJ1qh5cmT6Wqv2Ya4kheKzdAV3CJKnFg"
hud_headers = {"Authorization": "Bearer "+hud_token}
hud_available_apis = {
    'all_states': '/listStates',
    'all_counties': '/listCounties/',
    'all_metro_areas': '/listMetroAreas',
    'FMRs': '/data/'
}

# Create a State DF
state_df = pd.DataFrame(requests.get(hud_base_url+hud_available_apis['all_states'],headers=hud_headers).\
                        json())

# Create a Metro Area DF.
# Create a list of unique metro areas in order to iterate over FMRs entityID in order to get zipcode data.
metro_areas_df = pd.DataFrame(requests.get(hud_base_url+hud_available_apis['all_metro_areas'],headers=hud_headers).\
                        json())
unique_metro_areas_list = list(metro_areas_df['cbsa_code'].unique())
unique_metro_areas_list_sample = random.sample(unique_metro_areas_list,10)

# Retrieve FMRs using the unique metro areas list. Create a for loop that filters 'data' key
FMR_raw = []

for metro_areas in unique_metro_areas_list_sample:
    resp = requests.get(hud_base_url+hud_available_apis['FMRs']+metro_areas,headers=hud_headers)

    if resp.status_code == 200:
        fmr_data_when_200 = resp.json()

        for item in fmr_data_when_200:
            if 'data' in fmr_data_when_200:
                FMR_raw.append(fmr_data_when_200['data'])

# Un-nest FMR_raw and create a DataFrame of FMR_df
FMR_cleaned = []

for item in FMR_raw:
    parent_dict = item.copy()  # Create a copy of the parent dictionary
    basicdata = parent_dict.pop('basicdata')  # Remove 'basicdata' and get its value

    # Check if 'basicdata' is a list or a dictionary
    if isinstance(basicdata, list):
        # Handle the case where 'basicdata' is a list of dictionaries
        for sub_dict in basicdata:
            parent_dict.update(sub_dict)
    elif isinstance(basicdata, dict):
        # Handle the case where 'basicdata' is a single dictionary
        parent_dict.update(basicdata)

    FMR_cleaned.append(parent_dict)

FMR_df = pd.DataFrame(FMR_cleaned)
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
    'zip_codes': '/usps'
}

# Create a State DF
state_df = pd.DataFrame(get_hud_data(hud_available_apis['all_states'], hud_headers))

# Create a Zip Code DF
state_list = list(state_df['state_code'].unique())

zip_code_df = pd.DataFrame()
for state in state_list:
    zip_code_data = (get_hud_data(hud_available_apis['zip_codes'] + '?type=' + str(2) + '&query=' + state, hud_headers)\
                     ['data']['results'])
    zip_code_df_to_append = pd.DataFrame(zip_code_data)
    zip_code_df = pd.concat([zip_code_df, zip_code_df_to_append], ignore_index=True)

    time.sleep(2)

# Create a Metro Area DF
metro_areas_df = pd.DataFrame(get_hud_data(hud_available_apis['all_metro_areas'], hud_headers))

# Retrieve fmrs using the unique metro areas list
unique_metro_areas_list = list(metro_areas_df['cbsa_code'].unique())

fmr_raw = []
for metro_areas in unique_metro_areas_list:
    fmr_data = get_hud_data(hud_available_apis['FMRs'] + metro_areas, hud_headers)

    for item in fmr_data:
        if 'data' in fmr_data:
            fmr_raw.append(fmr_data['data'])

    time.sleep(2)

# Un-nest fmr_raw and create a DataFrame of fmr_df
fmr_cleaned = []
for item in fmr_raw:
    parent_dict = item.copy()
    basicdata = parent_dict.pop('basicdata')

    if isinstance(basicdata, list):
        # Handle the case where 'basicdata' is a list of dictionaries
        for sub_dict in basicdata:
            new_row = parent_dict.copy()  # Create a new row
            new_row.update(sub_dict)  # Copy 'basicdata' fields to the new row
            fmr_cleaned.append(new_row)
    elif isinstance(basicdata, dict):
        # Handle the case where 'basicdata' is a single dictionary
        parent_dict.update(basicdata)
        fmr_cleaned.append(parent_dict)

fmr_df = pd.DataFrame(fmr_cleaned)

# Save dataframes to CSV
state_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\state_data.csv', index=False)
zip_code_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\zip_code_data.csv', index=False)
metro_areas_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\metro_areas_data.csv', index=False)
fmr_df.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\FMR_data.csv', index=False)
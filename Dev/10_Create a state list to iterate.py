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

unique_state_list = list(state_df['state_code'].unique())

print(state_list)
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
    'FMRs': '/data/',
    'FMR_by_state': '/statedata/{statecode}'
}

# Create a State-County-FMR DataFrame.
state_df = pd.DataFrame(requests.get(hud_base_url+hud_available_apis['all_states'],headers=hud_headers).\
                        json())

metro_areas_df = pd.DataFrame(requests.get(hud_base_url+hud_available_apis['all_metro_areas'],headers=hud_headers).\
                        json())
unique_metro_areas_list = list(metro_areas_df['cbsa_code'].unique())
unique_metro_areas_list_sample = random.sample(unique_metro_areas_list,10)

metro_areas_list = {}
for metro_areas in unique_metro_areas_list_sample:
    resp = requests.get(hud_base_url+hud_available_apis['FMRs']+metro_areas,headers=hud_headers)
    metro_areas_list = metro_areas_list | resp.json()

'''
Dev notes - ES 11/1/2023 11:19pm
I'm stuck at the for loop function over metro_areas_list dictionary. 
The dictionary gotten from the API contains several nested dictionaries.
I must find a way to depurate this dictionary and consolidated them all in order to create a simple dataframe, 
and then begin with merging process.

 Plan:
 - To merge State-County-FMR. We'll loop unique metro area cbsa codes over FMRs mandatory parameter in order to get
 zip code level data.
 - Once we get a DataFrame, we'll merge with other DataFrames.
 - Median Home Values data is still pending.
'''
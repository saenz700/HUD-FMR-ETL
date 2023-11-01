import pandas as pd
import requests

# Connect to All DataFrames available in FMR API from https://www.huduser.gov/portal/dataset/fmr-api.html
hud_base_url = "https://www.huduser.gov/hudapi/public/fmr"
hud_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjBkM2M2NWNjYTc4NmY1ZThjZDliNDdmOTgxNzJlOTk0OWFjYmFlMzFhMmEyZGQ0Yzg0NzI3YWUxZWM1MTE1YmEwYjJkZDA3ZDY5MGNkMWRiIn0.eyJhdWQiOiI2IiwianRpIjoiMGQzYzY1Y2NhNzg2ZjVlOGNkOWI0N2Y5ODE3MmU5OTQ5YWNiYWUzMWEyYTJkZDRjODQ3MjdhZTFlYzUxMTViYTBiMmRkMDdkNjkwY2QxZGIiLCJpYXQiOjE2OTc0MDQ4OTEsIm5iZiI6MTY5NzQwNDg5MSwiZXhwIjoyMDEzMDI0MDkxLCJzdWIiOiI2MDMxNCIsInNjb3BlcyI6W119.hBm626SUPkrLfV2eg31PxJhWX6gUXPJo-N59Y7XcwPg5_IrrzorgyAlJ1qh5cmT6Wqv2Ya4kheKzdAV3CJKnFg"
hud_headers = {"Authorization": "Bearer "+hud_token}
hud_available_apis = {
    'all_states':'/listStates',
    'all_counties': '/listCounties',
    'all_metro_areas': '/listMetroAreas',
    'FMRs': '/data/{entityid}',
    'FMR_by_state': '/statedata/{statecode}'
}


# Confirm URL, link, etc are responding.
hud_response = requests.get(hud_base_url+'/listStates',headers=hud_headers)
print(hud_response.status_code)

# Comment
df = pd.DataFrame(hud_response.json())

print(df)
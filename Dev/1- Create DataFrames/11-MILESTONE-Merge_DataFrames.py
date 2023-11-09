import pandas as pd

# Read cvs's.
state_file_path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/state_data.csv'
zip_code_file_path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/zip_code_data.csv'
metro_areas_file_path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/metro_areas_data.csv'
fmr_file_path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/FMR_data.csv'

# Create dfs from csvs-- These dfs should be the continuity of the mother workflow.
state_df = pd.read_csv(state_file_path)
zip_code_df = pd.read_csv(zip_code_file_path)
metro_areas_df = pd.read_csv(metro_areas_file_path)
fmr_df = pd.read_csv(fmr_file_path)

merge_state_with_zip = pd.merge(left=zip_code_df, right=state_df, how='inner', left_on='state', right_on='state_code')

print(merge_state_with_zip)
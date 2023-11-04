import pandas as pd

# Read cvs's.
fmr_file_path = 'C:\\Users\\Admin\\PycharmProjects\\FMR_To_Median-Home-Values\\Dev\\FMR_data.csv'
metro_areas_file_path = 'C:\\Users\\Admin\\PycharmProjects\\FMR_To_Median-Home-Values\\Dev\\metro_areas_data.csv'
state_file_path = 'C:\\Users\\Admin\\PycharmProjects\\FMR_To_Median-Home-Values\\Dev\\state_data.csv'
zip_code_file_path = 'C:\\Users\\Admin\\PycharmProjects\\FMR_To_Median-Home-Values\\Dev\\zip_code_data.csv'

state_df = pd.read_csv(state_file_path)
zip_code_df = pd.read_csv(zip_code_file_path)
metro_areas_df = pd.read_csv(metro_areas_file_path)
fmr_df = pd.read_csv(fmr_file_path)



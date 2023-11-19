import pandas as pd

# Read cvs's.
state_file_path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/state_df.csv'
zip_code_file_path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/zip_code_df.csv'
fmr_fips_path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/fmr_fips_df.csv'
fmr_state_path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/fmr_state_df.csv'

# Create dfs from csvs-- These dfs should be the continuity of the mother workflow.
state_df = pd.read_csv(state_file_path)
zip_code_df = pd.read_csv(zip_code_file_path)
fmr_fips_df = pd.read_csv(fmr_fips_path)
fmr_state_df = pd.read_csv(fmr_state_path)

# Join zips with fmr_fips_df
zip_code_df_to_merge = zip_code_df[['zip', 'geoid']]

merge_fmr_fips_with_zip = pd.merge(left=fmr_fips_df, right=zip_code_df_to_merge, how='inner',\
                                   left_on='zip_code', right_on='zip')

merge_fmr_fips_with_zip['starts_with_geoid'] = merge_fmr_fips_with_zip.apply(\
    lambda row: str(row['fips_code']).startswith(str(row['geoid'])),
    axis=1
)

fmr_fips_df_v2 = merge_fmr_fips_with_zip[merge_fmr_fips_with_zip['starts_with_geoid'] == True]
fmr_fips_df_v2.drop('starts_with_geoid', axis=1, inplace=True)

fmr_fips_df_v2.to_csv(r'C:\Users\Admin\PycharmProjects\FMR_To_Median-Home-Values\Dev\fmr_fips_df_v2.csv', index=False)
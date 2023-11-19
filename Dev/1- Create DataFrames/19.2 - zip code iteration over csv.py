import pandas as pd

path = '/Users/Admin/PycharmProjects/FMR_To_Median-Home-Values/Dev/zip_across_years.csv'

zip_code_df = pd.read_csv(path)

# Assuming zip_code_df is your DataFrame with columns: zip, geoid, year, quarter, and other fields

# Convert year and quarter to string for concatenation
zip_code_df['year_quarter'] = zip_code_df['year'].astype(str) + '-' + zip_code_df['quarter'].astype(str)

# Convert year_quarter to datetime for proper sorting
zip_code_df['year_quarter'] = pd.to_datetime(zip_code_df['year_quarter'])

# Sort the DataFrame by year_quarter in descending order
zip_code_df = zip_code_df.sort_values(by=['year_quarter'], ascending=False)

# Get the index of the first occurrence for each year_quarter group
idx = zip_code_df.groupby(['year_quarter'])['geoid'].idxmax()
print(idx.head(50))

# Select the rows with the most recent zip-geoid combination
most_recent_zip_geoid = zip_code_df.loc[idx, ['zip', 'geoid', 'year', 'quarter']]

# Drop the temporary column used for sorting
zip_code_df = zip_code_df.drop('year_quarter', axis=1)

# Display the result
print(most_recent_zip_geoid)

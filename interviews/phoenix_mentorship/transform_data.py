import pandas as pd
import numpy as np

# Read the data
df = pd.read_csv('data/agric_data.csv')
# print(df)
# Unnamed col
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Remove empty columns (classifications, grade,sex) since most of them dont have data so it wount help much in analysis.
 
df.drop(columns=['classification', 'grade', 'sex'], inplace=True)

# check for null values and remove
# Replace all - with null for concistency
df.replace({" - ": np.nan, "NULL": np.nan}, inplace=True)

# print(df.isnull().sum()) - makes us know the types in wholesale, retail $ supply volume are wrong
df['supply_volume'] = df['supply_volume'].astype(float)
df['wholesale'] = df['wholesale'].str.replace('/Kg', '').astype(float)
df['retail'] = df['retail'].str.replace('/Kg', '').astype(float)
df['date'] = pd.to_datetime(df['date'])
# print(df.dtypes)

# Replace null values with 0.0
df.fillna(0.0, inplace=True)
# print(df)
# print(df.isnull().sum())

# check for duplicate and remove if any
print(df.duplicated().sum())
df.to_csv('data/clean/cleaned_agric_data', index=False)



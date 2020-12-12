# Dependencies and Setup
import pandas as pd
import numpy as np
import requests
import gmaps
import os
import json
from scipy.stats import linregress
from config import api_key
from config import g_key
from census import Census
c = Census(api_key, year=2013)

# File to Load (Unclean member file)
file_to_load = "active_lapsed.csv"

# Read unclean member file and store into Pandas data frame
member_data = pd.read_csv(file_to_load)
member_data.head()

from sqlalchemy import create_engine

# Define DataFrame for member file
member_data_df = pd.DataFrame(member_data)

# Blurred address column for protection
def blurry(s):
    return 'color: transparent; text-shadow: 0 0 5px rgba(0,0,0,0.5)'

# Blur data for privacy
member_data_df.style.applymap(blurry, subset=['Address'])

# Total count of UNIQUE member addresses
number_of_rows = len(member_data_df['Address'].unique())
number_of_rows

# Rename Zip Code to Zipcode and Blur Data for Privacy
member_data_df = member_data_df.rename(columns={'Zip_Code':'Zipcode'})

member_data_df.style.applymap(blurry, subset=['Address'])

# Total count of addresses (including duplicates)
qty_of_addresses = len(member_data_df['Address'])
qty_of_addresses

revised_member_df = member_data_df.drop_duplicates(subset='Address', keep="first")
revised_member_df

revised_member_df.style.applymap(blurry, subset=['Address'])

# Remove last 4-digits of zip code
revised_member_df['Zipcode'] = revised_member_df['Zipcode'].where(revised_member_df['Zipcode'].str.len() == 5, 
                                               revised_member_df['Zipcode'].str[:5])

revised_member_df.head()
revised_member_df.style.applymap(blurry, subset=['Address'])


# Checking type of zip code
type('Zipcode')

# Retrieving second file via API
# Run Census Search to retrieve data on all zip codes (2013 ACS5 Census)
# See: https://github.com/CommerceDataService/census-wrapper for library documentation
# See: https://gist.github.com/afhaque/60558290d6efd892351c4b64e5c01e9b for labels
census_data = c.acs5.get(("NAME", "B19013_001E", "B01003_001E", "B01002_001E",
                          "B19301_001E",
                          "B17001_002E"), {'for': 'zip code tabulation area:*'})
# Convert to DataFrame
census_pd = pd.DataFrame(census_data)
# Column Reordering
census_pd = census_pd.rename(columns={"B01003_001E": "Population",
                                      "B01002_001E": "Median Age",
                                      "B19013_001E": "Household Income",
                                      "B19301_001E": "Per Capita Income",
                                      "B17001_002E": "Poverty Count",
                                      "NAME": "Name", "zip code tabulation area": "Zipcode"})
# Add in Poverty Rate (Poverty Count / Population)
census_pd["Poverty Rate"] = 100 * \
    census_pd["Poverty Count"].astype(
        int) / census_pd["Population"].astype(int)
# Final DataFrame
census_pd = census_pd[["Zipcode", "Population", "Median Age", "Household Income",
                       "Per Capita Income", "Poverty Count", "Poverty Rate"]]
# Visualize
print(len(census_pd))
census_pd.head()

# Convert to DataFrame
census_pd = pd.DataFrame(census_data)
# Column Reordering
census_pd = census_pd.rename(columns={"B01003_001E": "Population",
                                      "B01002_001E": "Median Age",
                                      "B19013_001E": "Household Income",
                                      "B19301_001E": "Per Capita Income",
                                      "B17001_002E": "Poverty Count",
                                      "NAME": "Name", "zip code tabulation area": "Zipcode"})
# Add in Poverty Rate (Poverty Count / Population)
census_pd["Poverty Rate"] = 100 * \
    census_pd["Poverty Count"].astype(
        int) / census_pd["Population"].astype(int)
# Final DataFrame
census_pd = census_pd[["Zipcode", "Population", "Median Age", "Household Income",
                       "Per Capita Income", "Poverty Count", "Poverty Rate"]]
# Visualize
print(len(census_pd))
census_pd.head()

# Check zipcode type
type('Zipcode')

# Merge data sets

merge_df = pd.merge(census_pd, revised_member_df, how = 'outer', on ='Zipcode')
merge_df

# Remove null rows
merge_df = merge_df.dropna()
merge_df

# Isolate data to zipcode, median age, household income, member level, status, expiration date,  city, state
merge_df = merge_df.drop(columns=['Per Capita Income', 'Poverty Count', 'Status', 'Address'])
merge_df

# Create database connection
import getpass

pwd_text=getpass.getpass('Enter password')

connection_string = f"postgres:{pwd_text}@localhost:5432/member_db"
engine = create_engine(f'postgresql://{connection_string}')

merge_df.to_sql(name='member', con=engine, if_exists='append', index=True)

# Check for table
engine.table_names()

# Confirm that data has been addedt to SQL by querying member merged file

pd.read_sql_query('select * from member', con=engine).head()

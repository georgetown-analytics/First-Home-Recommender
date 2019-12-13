
# Data Transformation

import pandas as pd
import os

# Initial Variable Definition

path = os.path.join(os.getcwd(), 'Workspace', 'First-Home-Recommender', 'data')

dfs = [household_2017, mortgage_2017, person_2017, project_2017, household_2015, mortgage_2015, person_2015, project_2015]


# Import Source Data

household_2017 = pd.read_csv(os.path.join(path, 'AHS', 'AHS 2017 National PUF v3.0 CSV', 'household.csv'))
mortgage_2017 = pd.read_csv(os.path.join(path, 'AHS', 'AHS 2017 National PUF v3.0 CSV', 'mortgage.csv'))
person_2017 = pd.read_csv(r'..\AHS\AHS 2017 National PUF v3.0 CSV\person.csv')
project_2017 = pd.read_csv(r'..\AHS\AHS 2017 National PUF v3.0 CSV\project.csv')
household_2015 = pd.read_csv(r'..\data\AHS\AHS 2015 National PUF v3.0 CSV\household.csv')
mortgage_2015 = pd.read_csv(r'..\AHS\AHS 2015 National PUF v3.0 CSV\mortgage.csv')
person_2015 = pd.read_csv(r'..\AHS\AHS 2015 National PUF v3.0 CSV\person.csv')
project_2015 = pd.read_csv(r'..\AHS\AHS 2015 National PUF v3.0 CSV\project.csv')


# Strip out extra ' and convert object-type numbers to integer

def convert_to_int(df):
    cols = list(df.columns)
    for col in cols:
        if df[col].dtype == object:
            df[col] = df[col].str.strip("'").astype('int64')
        else:
            pass
    print(df.dtypes.value_counts())
    
convert_to_int(household_2017)
convert_to_int(mortgage_2017)
convert_to_int(person_2017)
convert_to_int(project_2017)
convert_to_int(household_2015)
convert_to_int(mortgage_2015)
convert_to_int(person_2015)
convert_to_int(project_2015)

#Export Annual DataFrames to CSV Files
    
household_2017.to_csv(os.path.join(path, 'data', 'working', 'household_2017.csv'))
mortgage_2017.to_csv(os.path.join(path, 'data', 'working', 'AHS_mortgage_2017.csv'))
person_2017.to_csv(os.path.join(path, 'data', 'working', 'AHS_person_2017.csv'))
project_2017.to_csv(os.path.join(path, 'data', 'working', 'AHS_project_2017.csv'))
household_2015.to_csv(os.path.join(path, 'data', 'working', 'AHS_household_2015.csv'))
mortgage_2015.to_csv(os.path.join(path, 'data', 'working', 'AHS_mortgage_2015.csv'))
person_2015.to_csv(os.path.join(path, 'data', 'working', 'AHS_person_2015.csv'))
project_2015.to_csv(os.path.join(path, 'data', 'working', 'AHS_project_2015.csv'))

# Create Combined Datasets
# Set Year Value
household_2017['YEAR'] = 2017
mortgage_2017['YEAR'] = 2017
person_2017['YEAR'] = 2017
project_2017['YEAR'] = 2017
household_2015['YEAR'] = 2015
mortgage_2015['YEAR'] = 2015
person_2015['YEAR'] = 2015
project_2015['YEAR'] = 2015


# Concatonate 2015 and 2017 Datasets
def concat_datasets(df1, df2):
    return pd.concat([df1, df2], join='inner', ignore_index=True)

hh_combined = pd.concat([household_2017, household_2015], join='inner', ignore_index=True)
mort_combined = pd.concat([mortgage_2017, mortgage_2015], join='inner', ignore_index=True)
person_combined = pd.concat([person_2017, person_2015], join='inner', ignore_index=True)
project_combined = pd.concat([project_2017, project_2015], join='inner', ignore_index=True)

#Send Combined DataFrames to CSV files

hh_combined.to_csv(os.path.join(path, 'data', 'working', 'AHS Household Combined.csv'))
mort_combined.to_csv(os.path.join(path, 'data', 'working', 'AHS Mortgage Combined.csv'))
person_combined.to_csv(os.path.join(path, 'data', 'working', 'AHS Person Combined.csv'))
project_combined.to_csv(os.path.join(path, 'data', 'working', 'AHS Project Combined.csv'))

dfs_combined = {hh_combined:'AHS Household Combined',
                mort_combined:'AHS Mortgage Combined',
                person_combined:'AHS Person Combined',
                project_combined:'AHS Project Combined'}

# Export Combined
for df in dfs_combined.keys():
    df.to_csv(os.path.join(path, 'data', 'working', dfs_combined[df] + '.csv'))
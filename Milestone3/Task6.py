
#Update dim_date_times table and update to the correct data type

#+-----------------+-------------------+--------------------+
#| dim_date_times  | current data type | required data type |
#+-----------------+-------------------+--------------------+
#| month           | TEXT              | VARCHAR(?)         |
#| year            | TEXT              | VARCHAR(?)         |
#| day             | TEXT              | VARCHAR(?)         |
#| time_period     | TEXT              | VARCHAR(?)         |
#| date_uuid       | TEXT              | UUID               |
#+-----------------+-------------------+--------------------+

#updated  dim_date_times table

#+-----------------+-------------------+--------------------+
#| dim_date_times  | current data type | required data type |
#+-----------------+-------------------+--------------------+
#| month           | OBJECT              | VARCHAR(255)     |
#| year            | OBJECT              | VARCHAR(255)     |
#| day             | OBJECT              | VARCHAR(255)     |
#| time_period     | OBJECT              | VARCHAR(255)     |
#| date_uuid       | OBJECT              | UUID             |
#+-----------------+-------------------+--------------------+

# converting to the right data type.

import pandas as pd
import boto3
import json
from sqlalchemy import create_engine

bucket = 'daak-bucket'
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json.'

# link uploaded to daak-bucket to give a path
# link path in daak-bucket = 's3://daak-bucket/date_details.json'
path = 's3://daak-bucket/date_details.json'

client = boto3.client('s3')

path = 's3://daak-bucket/date_details.json'

df =  pd.read_json(path)

# s3f3 and botocore==1.33.5 were installed respectively to ensure efficient processing

print(df)

df.head()

# cleaning data

# - Drop duplicates 

df = df.drop_duplicates()
df


# removing null values
df = df.replace('null','')
df

df = df.fillna('')
df

# resetting index
df = df.reset_index(drop=True)
df

df.info()

RDS_HOST = 'database-1.c7f2i0mwhdgu.eu-west-2.rds.amazonaws.com'
RDS_PORT = 5432
RDS_DATABASE ='postgres'
RDS_PASSWORD ='Backoffice2*'
RDS_USER ='postgres'
DATABASE_TYPE = 'postgresql'
DBAPI = 'pyscopg2'

engine = create_engine(f"{DATABASE_TYPE}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")

#  Upload clean data to database with table name dim_date_times.

def upload_to_db(dim_date_times, df):
   df.upload_to_db = dim_date_times.replace
   dim_date_times.replace = dim_date_times
   return df
     
  
df.to_sql('dim_date_times', engine, if_exists='replace')

df.dtypes


replacements = {
    'object': 'varchar(255)',
    'object': 'uuid'
}


print(replacements)

col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes))

print(col_str)

col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))

print(col_str)
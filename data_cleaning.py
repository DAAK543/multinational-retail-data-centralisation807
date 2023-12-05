# Step 6 cleaning user data
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd

RDS_HOST ="data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DATABASE ='postgres'
RDS_PASSWORD ='AiCore2022'
RDS_USER ='aicore_admin'
DATABASE_TYPE = 'postgresql'

engine = sqlalchemy.create_engine(f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}") 

class DataCleaning: 
   
# legacy_users is the primary user data.
   
 def clean_user_data(df):
     clean_user_data.df()
     clean_user_data = pd.read_sql_table()
     return df

df = pd.read_sql_table("legacy_users", engine)
print(df)

# Drop duplicates in legacy_users table user data

df = df.drop_duplicates()
df

 # cleaning phone numbers  
#df["phone_number"].str.replace('[^a-zA-Z0-9]','')

#df["phone_number"] = df["phone_number"].apply(lambda x: str(x))

df["phone_number"] = df["phone_number"].apply(lambda x: x[0:3] + '-' + x[3:6] + '-' + x[6:10])

df

# removing null values
df = df.replace('null','')
df

df = df.fillna('')
df

# resetting index
df = df.reset_index(drop=True)
df

# cleaning dates

#df["join_date"] = pd.to_datetime(df['join_date'])


#df["date_of_birth"] = pd.to_datetime(df['date_of_birth'])

#df.date_of_birth = pd.to_datetime(df.date_of_birth).dt.strftime('%d-%b-%Y')
#print (df)


df.info()
#Update dim_card_details - update the last table for card details.


#dim_card_details 
#card_number            
#expiry_date            
#date_payment_confirmed 


#Current data type
#TEXT
#TEXT
#TEXT

#Required data type
#VARCHAR(255)
#VARCHAR (255)
#DATE

import sqlalchemy
import pandas as pd 
import tabula 
from sqlalchemy import create_engine

RDS_HOST ="data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DATABASE ='postgres'
RDS_PASSWORD ='AiCore2022'
RDS_USER ='aicore_admin'
DATABASE_TYPE = 'postgresql'
DBAPI = 'pyscopg2'
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

df = tabula.read_pdf(pdf_path, pages="all")

print(df)


df = pd.DataFrame()

df

df.dtypes

engine = create_engine(f"{DATABASE_TYPE}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")


#df.to_sql(name='dim_card_details', con=engine, if_exists='replace')


#sqlalchemy.exc.InternalError: (psycopg2.errors.ReadOnlySqlTransaction) cannot execute CREATE TABLE in a read-only transaction
     
  
#current data type for dim_card_details last table

("""
    card_number             TEXT,
    expiry_date             TEXT,
    date_payment_confirmed  TEXT
   
""")

# required data type for dim_card_details last table

replacements = {
     'TEXT': 'VARCHAR(255)',
     'TEXT': 'DATE'
}

print (replacements)

col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes))

print(col_str)

col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))

print(col_str)
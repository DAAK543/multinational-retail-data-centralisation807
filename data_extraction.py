# Step 5 
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd


RDS_HOST ="data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DATABASE ='postgres'
RDS_PASSWORD ='AiCore2022'
RDS_USER ='aicore_admin'
DATABASE_TYPE = 'postgresql'


class DataExtractor: 

    def read_rds_tables(connector):
            engine = connector.create_engine
            return engine
    def list_db_tables():
     list_db_tables = insp.get_table_names()
     return list_db_tables
    engine = create_engine(f"{DATABASE_TYPE}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
    insp = inspect(engine)
    print(insp.get_table_names())

# from list of tables (legacy_store_details, legacy_users, orders_table) read each table and return dataframe


    def read_rds_tables(df):
     read_rds_tables.df()
     read_rds_tables = pd.read_sql_table()
     return df

     
    engine = sqlalchemy.create_engine(f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
 
    df = pd.read_sql_table("legacy_store_details", engine)
    print(df)

    df = pd.read_sql_table("legacy_users", engine)
    print(df)
    
    df = pd.read_sql_table("orders_table", engine)
    print(df)

    
      

  
     



    
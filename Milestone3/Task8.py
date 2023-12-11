#Creating primary keys in dimensions tables

# Creating primary key for dim_users_table

import pandas as pd
from sqlalchemy import create_engine

RDS_HOST ="data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DATABASE ='postgres'
RDS_PASSWORD ='AiCore2022'
RDS_USER ='aicore_admin'
DATABASE_TYPE = 'postgresql'
DBAPI = 'pyscopg2'

engine = create_engine(f"{DATABASE_TYPE}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")


def create_table(curr):
 
 create_table_command = ("""CREATE TABLE IF NOT EXISTS dim_users_table(
                user_uuid      UUID  PRIMARY KEY,
                first_name     VARCHAR(255),
                last_name      VARCHAR(255),
                date_of_birth  DATE,
                country_code   VARCHAR(255),
                join_date      DATE
)""")

 curr.execute(create_table_command)


#df = pd.read_sql("dim_users_table", engine, index_col=0)




# creating primary key for dim_products table

def create_table(curr):
 
 create_table_command = ("""CREATE TABLE IF NOT EXISTS dim_products(
                product_code      VARCHAR(255) PRIMARY,          
                product_price     FLOAT,
                Weight            FLOAT,
                EAN               VARCHAR(255),
                date_added        DATE,
                uuid              UUID,
                still_available   BOOL,
                weight_class      VARCHAR(255)
)""")

 curr.execute(create_table_command)


#df = pd.read_sql("dim_products", engine, index_col=0)



# creating primary key for dim_date_times table


def create_table(curr):
 
 create_table_command = ("""CREATE TABLE IF NOT EXISTS dim_date_times(
                date_uuid    UUID PRIMARY KEY,
                month        VARCHAR(255),
                year         VARCHAR(255),
                day          VARCHAR(255),
                time_period  VARCHAR(255),
)""")

 curr.execute(create_table_command)


#df = pd.read_sql("dim_date_times", engine, index_col=0)


# creating primary key for dim_card_details table


def create_table(curr):
 
 create_table_command = ("""CREATE TABLE IF NOT EXISTS dim_card_details(
                card_number              VARCHAR(255) PRIMARY KEY,
                expiry_date              DATE,
                date_payment_confirmed   DATE,
                card_provider            VARCHAR(255)
)""")

 curr.execute(create_table_command)


#df = pd.read_sql("dim_card_details", engine, index_col=0)



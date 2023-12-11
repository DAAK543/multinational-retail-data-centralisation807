# How many stores does the business have and in which country
import sqlalchemy
import pandas as pd 
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy import text

RDS_HOST ="data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DATABASE ='postgres'
RDS_PASSWORD ='AiCore2022'
RDS_USER ='aicore_admin'
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'

engine = create_engine(f"{DATABASE_TYPE}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")

with engine.connect() as connection:
 result = connection.execute("""SELECT country_code, COUNT(*) AS number_of_stores
                                     FROM legacy_store_details
                                     GROUP BY country_code;
""")
for row in result: 
  print(result)


# Using pgAdmin to execute the query
("""SELECT country_code, COUNT(*) AS number_of_stores
FROM legacy_store_details
GROUP BY country_code;SELECT country_code, COUNT(*) AS number_of_stores
FROM legacy_store_details
GROUP BY country_code;
""")


 


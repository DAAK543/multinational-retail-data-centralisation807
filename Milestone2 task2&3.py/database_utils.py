#Task 3  step 2  

import yaml

RDS_HOST ="data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DATABASE ='postgres'
RDS_PASSWORD ='AiCore2022'
RDS_USER ='aicore_admin'
DATABASE_TYPE = 'postgresql'
DBAPI = 'pyscopg2'

class DatabaseConnector:



    def read_db_creds(self):
      
      # read_db_creds in db_creds.yaml, reads and returns a dictionary of the credentials.
      with open("db_creds.yaml", 'r') as file:
        read_db_creds = yaml.safe_load(file)

             
        print(read_db_creds)

     # __init__ is the constructor

    def __init__(self, RDS_HOST, RDS_PORT, RDS_DATABASE, RDS_USER, RDS_PASSWORD):
        self.RDS_HOST = RDS_HOST
        self.RDS_PORT = RDS_PORT
        self.RDS_DATABASE = RDS_DATABASE
        self.RDS_USER = RDS_USER
        self.RDS_PASSWORD = RDS_PASSWORD
        
       
   
    def database_connector(loader, node):
        values = loader.connect_mapping(node)
        return DatabaseConnector(values['RDS_HOST'], values['RDS_PORT'], values['RDS_DATABASE'], values['RDS_USER'], values['RDS_PASSWORD'])

   

# print(read_db_creds)

 # Creating an instance of the class

connector = DatabaseConnector('RDS_HOST', 'RDS_PORT', 'RDS_DATABASE', 'RDS_USER', 'RDS_PASSWORD') 
read_db_creds = connector.read_db_creds()   
 

# Task 3 Step 3 creating an _init_db_engine method to read credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.

# _init_db_engine method


from sqlalchemy import create_engine
from sqlalchemy import inspect

class Connector:

 def _init_db_engine():
     connector = DatabaseConnector('RDS_HOST', 'RDS_PORT', 'RDS_DATABASE', 'RDS_USER', 'RDS_PASSWORD') 
     engine.connect = connector.read_db_creds()  
      
     def __init__(self, engine):
       
       self.engine = engine
       
       return engine
       
engine = create_engine(f"{DATABASE_TYPE}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")

# Task 3 Step 4 getting a list of table names in the database using list_db_tables method

class List:
   
   def __init__(self, get_table_names):
      self.get_table_names = get_table_names

   def list_db_tables():
    
    list_db_tables = insp.get_table_names()
    return list_db_tables
   
engine = create_engine(f"{DATABASE_TYPE}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
insp = inspect(engine)
print(insp.get_table_names())


# To get schema information and schema public

inspector = inspect(engine)
schemas = inspector.get_schema_names()
for schema in schemas:
    print("schema: %s" % schema)

# To get column information

    for table_name in inspector.get_table_names(schema=schema):
        for column in inspector.get_columns(table_name, schema=schema):
            print("Column: %s" % column)

# To get column with name.

for table_name in inspector.get_table_names():
   for column in inspector.get_columns(table_name):
       print("Column: %s" % column['name'])





# Step 7 - in the DatabaseConnector class, create a method called upload_to_db
import sqlalchemy
import pandas as pd

engine = sqlalchemy.create_engine(f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}") 

def upload_to_db(df):
   upload_to_db.df() 
   upload_to_db = pd.read_sql_table(Connector)
   return df
# the related sale_data here is the orders_table, it will take up the orders_table as argument

df = pd.read_sql_table('orders_table', engine)

print(df)
df

#Step 8 cleaning order_tables

df = df.drop_duplicates()
df

# removing none and NaN values
df = df.replace('none','')
df

df = df.replace('NaN','')
df

df = df.fillna('')
df

# resetting index
df = df.reset_index(drop=True)
df

# drop column first_name and last_name in the orders_table

df = df.drop(columns=['first_name', 'last_name'], axis=1)

df

df.info()


# upload table with upload_to_db method to sale_data database called dim_users

# Task 2 step 7
def upload_to_db(dim_users, df):
   df.upload_to_db = dim_users.replace
   dim_users.replace = dim_users
   return df
     
 # Task 2 step 8 

df.to_sql('dim_users', engine, if_exists='replace')



     







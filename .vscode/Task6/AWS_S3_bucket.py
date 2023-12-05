import boto3
import pandas as pd
import csv
import io
from io import StringIO
# Set your AWS credentials and region

BUCKET_NAME = "daak-bucket"
KEY = "s3://data-handling-public/products.csv"


class DataExtractor:
  def extract_from_s3(DataExtractor, df):
   DataExtractor.df = pd.read_csv()
   return df
  
  def __init__(self, DataExtractor, read_csv, s3):
     
    self.DataExtractor = DataExtractor
    self.read_csv = read_csv
    self.s3 = s3


    # Initialize the s3c client
  def s3c(boto3):
   s3c.get_object(**kwargs)
   return s3c

s3c = boto3.client('s3')
    # Specify the S3 bucket and object key of the CSV file
Bucket_name = 'daak-bucket' 
file_key = 's3://data-handling-public/products.csv'

    # Read the CSV file from S3
object = s3c.get_object(Bucket=Bucket_name, Key=file_key)

csv_content = object['Body'].read().decode('utf-8')

    # Create a Pandas DataFrame
        
df = pd.read_csv(io.StringIO(csv_content))
print(df)
df.head(5)

# An error occured when calling getobject opertion
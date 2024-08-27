import boto3
import os
from env_vars import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# Create an S3 client
s3 = boto3.client(
    's3', 
    region_name='us-east-2',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

bucket_name = 'redistricting-data-2024'
file_name = '/home/miguel674-rev/2009_Big_Data/project-3/P3/Year_2010/clean/2010_cleaned_data.csv'
object_name = '2010_cleaned_data.csv' 

# Upload the file to S3
s3.upload_file(file_name, bucket_name, object_name)

print(f"File {file_name} uploaded to {bucket_name}/{object_name}")
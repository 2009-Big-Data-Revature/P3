import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# Get the AWS credentials from the environment
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


# Create an S3 client
s3 = boto3.client(
    's3', 
    region_name='us-east-2',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    )

bucket_name = 'redistricting-data-2024'
file_name = '/Users/matthewbernhardt/Desktop/P3/Year_2020/clean/2000_cleaned_data.csv'
object_name = '2000_cleaned_data.csv' 

# Upload the file to S3
s3.upload_file(file_name, bucket_name, object_name)

print(f"File {file_name} uploaded to {bucket_name}/{object_name}")
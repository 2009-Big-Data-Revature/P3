from pyspark.sql import SparkSession
import os
from env_vars import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
# from dotenv import load_dotenv

# load_dotenv()

# Install these JAR files in your Jars folder inside the Spark folder
# wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
# wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar

spark = SparkSession.builder \
    .appName("S3 to Spark DataFrame") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

bucket_name = 'redistricting-data-2024'
object_name = '2000_cleaned_data.csv'
s3_path = f's3a://{bucket_name}/{object_name}'

df = spark.read.csv(s3_path, header=True, inferSchema=True)
df.show()




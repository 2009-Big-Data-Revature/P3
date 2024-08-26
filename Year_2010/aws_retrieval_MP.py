from pyspark.sql import SparkSession
import os
from env_vars import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from pyspark.sql.functions import desc
# from dotenv import load_dotenv

# load_dotenv()

# Install these JAR files in your Jars folder inside the Spark folder
# cd $SPARK_HOME/jars
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
years = ['2000', '2010', '2020']
for year in years:
    object_name = f'{year}_cleaned_data.csv'
    s3_path = f's3a://{bucket_name}/{object_name}'
    df = spark.read.csv(s3_path, header=True, inferSchema=True)
    df.createOrReplaceTempView(f'{year}_data')

df = spark.sql('(SELECT * FROM 2000_data) UNION (SELECT * FROM 2010_data) UNION (SELECT * FROM 2020_data)')
df.sort(['State Abv','year']).show(1000)

# Get top 5 regions per year with greatest population
top_regions_per_year = df.select('Region').orderBy(['State Abv']).distinct().show(1000)
# .sum('Total Population').withColumnRenamed('sum(Total Population)', 'Total Population').sort(desc('Total Population'))
# top_regions_per_year.show()
 
# object_name = '2020_cleaned_data.csv'
# s3_path = f's3a://{bucket_name}/{object_name}'

# df = spark.read.csv(s3_path, header=True, inferSchema=True)
# df.show(1000)




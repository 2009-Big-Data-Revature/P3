from pyspark.sql import SparkSession
import os
# from env_vars import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

# Install these JAR files in your Jars folder inside the Spark folder
# wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
# wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

spark = SparkSession.builder \
    .appName("S3 to Spark DataFrame") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

bucket_name = 'redistricting-data-2024'
object_name = '2010_cleaned_data.csv'
s3_path = f's3a://{bucket_name}/{object_name}'

object_2000 = '2000_cleaned_data.csv'
object_2020 = '2020_cleaned_data.csv'
s3_2000_path = f's3a://{bucket_name}/{object_2000}'
s3_2020_path = f's3a://{bucket_name}/{object_2020}'

df = spark.read.csv(s3_path, header=True, inferSchema=True)
df_2000 = spark.read.csv(s3_2000_path, header=True, inferSchema=True)
df_2020 = spark.read.csv(s3_2020_path, header=True, inferSchema=True)
# Register the DataFrame as a temporary view
df.createOrReplaceTempView("cleaned_data_2010")
df_2000.createOrReplaceTempView("cleaned_data_2000")
df_2020.createOrReplaceTempView("cleaned_data_2020")

# df.show()
# Fastest growing regions
# Example SQL query to find the fastest-growing regions

result_df = spark.sql("""
    SELECT 
        data2010.Region AS Region,
        MAX(data2000.`Total Population`) AS population_2000,
        MAX(data2010.`Total Population`) AS population_2010,
        MAX(data2020.`Total Population`) AS population_2020,
        (population_2010 -population_2000) AS growth_2000_to_2010,
        (population_2020 - population_2010) AS growth_2010_to_2020,
        (population_2020 - population_2000) AS growth_2000_to_2020,
            ROUND(
                ((population_2010 -population_2000) / population_2000) * 100, 1
            ) AS percentage_growth_2000_to_2010,
            ROUND(
                ((population_2020 - population_2010) / population_2010) * 100, 1
            ) AS percentage_growth_2010_to_2020,
            ROUND(
                ((population_2020 - population_2000) / population_2000) * 100, 1
            ) AS percentage_growth_2000_to_2020
    FROM 
        cleaned_data_2000 AS data2000
    JOIN 
        cleaned_data_2010 AS data2010 
    ON 
        data2000.Region = data2010.Region
    JOIN 
        cleaned_data_2020 AS data2020
    ON 
        data2010.Region = data2020.Region
    GROUP BY 
        data2010.Region
    ORDER BY 
        percentage_growth_2000_to_2020 DESC
    LIMIT 10;

""")


# Renaming columns and rounding percentages
result_df_renamed = result_df \
    .withColumnRenamed("percentage_growth_2000_to_2010", "Percentage Growth from 2000 to 2010") \
    .withColumnRenamed("percentage_growth_2010_to_2020", "Percentage Growth from 2010 to 2020") \
    .withColumnRenamed("percentage_growth_2000_to_2020", "Percentage Growth from 2000 to 2020")

result_df_renamed.show(truncate=False)





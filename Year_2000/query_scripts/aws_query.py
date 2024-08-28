from pyspark.sql import SparkSession
import os
import sys
import auth
from pyspark.sql.functions import sum, max

# Install these JAR files in your Jars folder inside the Spark folder
# wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
# wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar

spark = SparkSession.builder \
    .appName("S3 to Spark DataFrame") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", auth.access) \
    .config("spark.hadoop.fs.s3a.secret.key", auth.secret) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

bucket_name = 'redistricting-data-2024'
object_names = ['2000_cleaned_data.csv', '2010_cleaned_data.csv', '2020_cleaned_data.csv']
s3_paths = [f's3a://{bucket_name}/{object_name}' for object_name in object_names]

dfs = [spark.read.csv(s3_path, header=True, inferSchema = True) for s3_path in s3_paths]

year = 2000
for df in dfs:
    df.show()
    df.write.format('csv').option('header', 'true').mode('overwrite').save(f'/out_{year}.csv')
    year += 10


#df.createOrReplaceTempView('census')
#spark.sql("SELECT * from census").show()
#spark.sql("SELECT sum('Total Population') as Total from census").show()
#df.select(sum(df['Total Population']).alias('Total Population')).show()
#df.select(max(df['Total Population']).alias('State with Population')).show()

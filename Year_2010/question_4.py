from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import DataFrame,functions as f
from env_vars import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

spark = SparkSession.builder \
    .appName("S3 to Spark DataFrame") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

years = ["2000_", "2010_", "2020_"]
bucket_name = 'redistricting-data-2024'
object_name = 'cleaned_data.csv'

df: DataFrame
for i in range(len(years)):
    file_name = f"{years[i]}" + object_name
    s3_path = f's3a://{bucket_name}/{file_name}'
    if i ==  0:
        df = spark\
            .read\
            .csv(s3_path, header=True, inferSchema=True)
    else:
        df = df\
            .union(spark.read.csv(s3_path, header=True, inferSchema=True))

df1 = spark.createDataFrame(df.where(df["year"] == 2000)\
    .select("State Abv", "Total Population", "year")\
    .orderBy(f.desc(df["Total Population"])).take(5))
df2 = spark.createDataFrame(df.where(df["year"] == 2010)\
    .select("State Abv", "Total Population", "year")\
    .orderBy(f.desc(df["Total Population"])).take(5))
df3 = spark.createDataFrame(df.where(df["year"] == 2020)\
    .select("State Abv", "Total Population", "year")\
    .orderBy(f.desc(df["Total Population"])).take(5))

df_top5_state_population_by_year = df.join(df1, df["State Abv"] == df1["State Abv"])\
    .union(df.join(df2, df["State Abv"] == df2["State Abv"]))\
    .union(df.join(df3, df["State Abv"] == df3["State Abv"]))\
    .groupBy(df["State Abv"], df["Total Population"], df["year"])\
    .count()\
    .select("State Abv", "Total Population", "year").orderBy("year")
    

df_top_region_population= df.groupBy("Region", "Year")\
    .agg(f.sum("Total Population")\
    .alias("Total_Population"))\
    .orderBy("year", "Region")

df_top5_state_population_by_year.show()
df_top_region_population.show()
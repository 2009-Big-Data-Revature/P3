from pyspark.sql import SparkSession
import os
from env_vars import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

#NOTE: env_vars.py is a file that contains the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables. It is not uploaded to the repository for security reasons.

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


def get_population_comparison_across_year_and_region():
    """
        Question 4:
        How does the population of each region change from 2000 to 2020?
        Written by Oluwatobi Kolawole Olukunle
    """
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
    return (df_top5_state_population_by_year, df_top_region_population)

def get_trend_for_year_2030():
    ...

def get_fastest_growing_regions():
    """
        Question 6:
        Which regions are the fastest-growing from 2000 to 2020?
        Written by Tony Erazo
    """
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
    return result_df_renamed

def save_dataframes(*args):
    ...

def main():
    (q1_df_a, q1_df_b) = get_population_comparison_across_year_and_region()
    q2_df = get_trend_for_year_2030()
    q3_df = get_fastest_growing_regions()

    #TODO: save dataframes.
    save_dataframes(q1_df_a, q1_df_b, q2_df, q3_df)

if __name__ == '__main__':
    main()
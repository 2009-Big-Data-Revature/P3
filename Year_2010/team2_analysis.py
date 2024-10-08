from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
import os
import boto3
from env_vars import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from tqdm import tqdm

#NOTE: env_vars.py is a python file that contains the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables. It is not uploaded to the repository for security reasons.

# Install these JAR files in your Jars folder inside the Spark folder
# cd $SPARK_HOME/jars
# wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
# wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar

#Install the following packages:
# pip install tqdm
# pip install boto3

working_dir =os.path.dirname(os.path.realpath(__file__))
results_path = working_dir + "/results"
if not os.path.exists(results_path):
    os.makedirs(results_path)

s3 = boto3.client(
    's3',
    region_name='us-east-2',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

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
list_of_dfs = []
for year in years:
    object_name = f'{year}_cleaned_data.csv'
    s3_path = f's3a://{bucket_name}/{object_name}'
    df = spark.read.csv(s3_path, header=True, inferSchema=True)
    df.createOrReplaceTempView(f'cleaned_data_{year}')
    list_of_dfs.append(df)


def get_population_comparison_across_year_and_region():
    """
        Question 4:
        How does the population of each region change from 2000 to 2020?
        Written by Oluwatobi Kolawole Olukunle
    """
    df: DataFrame
    for index, dataframe in enumerate(list_of_dfs):
        if index == 0:
            df = dataframe
        else:
            df = df.union(dataframe)

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
        .select("State Abv", "Total Population", "year").orderBy("year", "Total Population")
        

    df_top_region_population= df.groupBy("Region", "Year")\
        .agg(f.sum("Total Population")\
        .alias("Total_Population"))\
        .orderBy("year", "Total_Population")

    df_top5_state_population_by_year.show()
    df_top_region_population.show()
    return (df_top5_state_population_by_year, df_top_region_population)

def get_trend_for_year_2030():
    """
        Question 5:
        Trend Line predictions for states 2030 populations based on 2020 and earlier data.
        Written by Darryl Bunn
    """
    df_2000 = list_of_dfs[0]
    df_2010 = list_of_dfs[1]
    df_2020 = list_of_dfs[2]

    pop_state_df = df_2000.filter(df_2000["Summary Code"] == 40).join(df_2010, df_2000["State Abv"] == df_2010["State Abv"])\
    .join(df_2020, df_2000["State Abv"] == df_2020["State Abv"])\
    .select(
        df_2000["State Abv"], 
        df_2000["Total Population"].alias("2000 Population"), 
        df_2010["Total Population"].alias("2010 Population"),
        df_2020["Total Population"].alias("2020 Population")
    )

    # Population prediction for 2030 by state
    # Estimated using exponential growth model
    df_2030_projected = pop_state_df.withColumn(
        "2030 Projected Population", 
        f.floor(((pop_state_df["2020 Population"]**2)/pop_state_df["2010 Population"]+(pop_state_df["2020 Population"]**1.5)/pop_state_df["2000 Population"]**0.5)/2)
        )\
        .withColumn(
            "Growth Rate",
            f.log((pop_state_df["2020 Population"])/pop_state_df["2010 Population"]) + 0.5*f.log((pop_state_df["2020 Population"])/pop_state_df["2000 Population"])
        )\
            .select("State Abv", "2000 Population", "2010 Population", "2020 Population", "2030 Projected Population", "Growth Rate")
    new_df = df_2030_projected.select("State Abv", df_2030_projected["2000 Population"].alias("population"), "Growth Rate").withColumn("year", f.lit(2000))\
    .union(df_2030_projected.select("State Abv", df_2030_projected["2010 Population"].alias("population"), "Growth Rate").withColumn("year", f.lit(2010)))\
    .union(df_2030_projected.select("State Abv", df_2030_projected["2020 Population"].alias("population"), "Growth Rate").withColumn("year", f.lit(2020)))\
    .union(df_2030_projected.select("State Abv", df_2030_projected["2030 Projected Population"].alias("population"), "Growth Rate").withColumn("year", f.lit(2030)))
    new_df.show(truncate=False)
    return new_df


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
    """
        Takes in a list of dataframes and saves them to a csv file.
    """
    switcher = {
            0: "population_comparison_state_ranking",
            1: "population_comparison_region_ranking",
            2: "trend_line_prediction_2030",
            3: "fastest_growing_regions"
        }
    for index, df in tqdm(enumerate(args)):
        df_savepath = os.path.join(results_path,switcher.get(index) + ".csv")
        df.toPandas().to_csv(df_savepath, index=False)

def upload_to_s3():
    for filename in os.listdir(results_path):
        if filename.endswith(".csv"):
            s3.upload_file(os.path.join(results_path, filename), bucket_name, filename)

def main():
    (q1_df_a, q1_df_b) = get_population_comparison_across_year_and_region()
    q2_df = get_trend_for_year_2030()
    q3_df = get_fastest_growing_regions()

    save_dataframes(q1_df_a, q1_df_b, q2_df, q3_df)
    upload_to_s3()

if __name__ == '__main__':
    main()
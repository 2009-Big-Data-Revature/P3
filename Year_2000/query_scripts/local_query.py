from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import sum, max, col

spark = SparkSession.builder \
    .appName("Local to dataframe") \
    .getOrCreate()

#Read 2000, 2010, and 2020 csv's
dfs = [spark.read.csv(f'/home/michael/proj3/data_{year}.csv', header=True, inferSchema=True) for year in ['2000', '2010', '2020']]


#Union all three df's into one big one
union_df = dfs[0].union(dfs[1]).union(dfs[2])
union_df.show()


#Dict to store all following constructed dataframes
out_dfs = {}


#Display Total Population by Year. columns: (year, sum(Total_Population))
total_pop = (
    union_df.groupBy("year") \
    .sum('Total Population') \
    .alias("Total Population") \
    .orderBy("year")
)

for col_name in total_pop.columns:
    if col_name.startswith("sum("):
        new_name = col_name.replace("sum(", "").replace(")", "")
        total_pop = total_pop.withColumnRenamed(col_name, new_name)

out_dfs['Total_Pop'] = total_pop


#Display Total Population of all states by Year. columns: (AK, AL, AR, ..., WY)
out_dfs['State_Pops'] = (
    union_df.groupBy("year") \
    .pivot("State Abv", ["AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"]) \
    .sum("Total Population") \
    .orderBy("year") \
)


#Display Population of each category by year. columns: (White Alone, African-American Alone, American Indian and Alaska Native Alone, Asian Alone, Native Hawaiian/Pacific Islander Alone, Other Alone, Two or More Races, Hispanic or Latino, Not Hispanic or Latino)
category_pop = (
    union_df.groupby("year") \
    .sum("White Alone", "African-American Alone", "American Indian and Alaska Native Alone", 
        "Asian Alone", "Native Hawaiian/Pacific Islander Alone", "Other Alone", 
        "Two or More Races", "Hispanic or Latino", "Not Hispanic or Latino") \
    .orderBy("year")
)

for col_name in category_pop.columns:
    if col_name.startswith("sum("):
        new_name = col_name.replace("sum(", "").replace(")", "")
        category_pop = category_pop.withColumnRenamed(col_name, new_name)

out_dfs['Category_Pops'] = category_pop

#Display state with highest population by year. columns: (year, State_Abv, Total_Population)
w = Window.partitionBy('year')
out_dfs['Top_State'] = (
    union_df.withColumn('maxPop', max('Total Population').over(w)) \
    .where(col('Total Population') == col("maxPop")) \
    .select("year", "State Abv", "Total Population") \
    .orderBy("year")
)


#Display the population of each region by year. columns: (year, 1, 2, 3, 4, 5)
out_dfs['Region_Per_Year'] = (
    union_df.groupBy("year") \
    .pivot("Region", [1, 2, 3, 4, 9]) \
    .sum("Total Population") \
    .orderBy("year")
)


#Write all constructed dfs to csv
for name in out_dfs:
    out_dfs[name].show()
    out_dfs[name].write.format('csv').option('header', 'true').mode('overwrite').save(f'/proj3/{name}.csv')




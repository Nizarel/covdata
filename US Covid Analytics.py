# Databricks notebook source
#Mounting Bronze Containers to Databricks
# https://towardsdev.com/building-an-end-to-end-data-pipeline-with-delta-lake-and-databricks-337202a110a8

dbutils.fs.mount(
  source = "wasbs://bronze@covlake.blob.core.windows.net",
  mount_point = "/mnt/covlake/bronze",
  extra_configs = {"fs.azure.account.key.covlake.blob.core.windows.net":"VUcqNveOo1KtAsJDkJ9kxKfU3FTTEIXT/Qr/ncZzW0aBMY31OgGl08ea7EzKgiCNnuD/CYSYBpX3+AStk7ANMA=="})

# COMMAND ----------

#Mounting Silver Containers to Databricks

dbutils.fs.mount(
  source = "wasbs://silver@covlake.blob.core.windows.net",
  mount_point = "/mnt/covlake/silver",
  extra_configs = {"fs.azure.account.key.covlake.blob.core.windows.net":"VUcqNveOo1KtAsJDkJ9kxKfU3FTTEIXT/Qr/ncZzW0aBMY31OgGl08ea7EzKgiCNnuD/CYSYBpX3+AStk7ANMA=="})

# COMMAND ----------

#Mounting Gold Containers to Databricks

dbutils.fs.mount(
  source = "wasbs://gold@covlake.blob.core.windows.net",
  mount_point = "/mnt/covlake/gold",
  extra_configs = {"fs.azure.account.key.covlake.blob.core.windows.net":"VUcqNveOo1KtAsJDkJ9kxKfU3FTTEIXT/Qr/ncZzW0aBMY31OgGl08ea7EzKgiCNnuD/CYSYBpX3+AStk7ANMA=="})

# COMMAND ----------

# Create Covid19_db database
spark.sql("CREATE DATABASE IF NOT EXISTS covid19_db")

# COMMAND ----------

# Read the CSV files from the Bronze layer
df_us_daily_bronze = spark.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/mnt/covlake/bronze/us_covid19_daily.csv')
df_us_states_daily_bronze = spark.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/mnt/covlake/bronze/us_states_covid19_daily.csv')
df_us_counties_daily_bronze = spark.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/mnt/covlake/bronze/us_counties_covid19_daily.csv')

# COMMAND ----------

# Clean and transform the data
df_us_daily_silver = df_us_daily_bronze.filter(df_us_daily_bronze.date.isNotNull())
df_us_states_daily_silver = df_us_states_daily_bronze.filter(df_us_states_daily_bronze.date.isNotNull())
df_us_counties_daily_silver = df_us_counties_daily_bronze.filter(df_us_counties_daily_bronze.date.isNotNull())

# Write the cleaned and transformed data to the Silver layer as Delta tables
df_us_daily_silver.write.format('delta').mode('overwrite').save('/mnt/covlake/silver/us_covid19_daily')
df_us_states_daily_silver.write.format('delta').mode('overwrite').save('/mnt/covlake/silver/us_states_covid19_daily')
df_us_counties_daily_silver.write.format('delta').mode('overwrite').save('/mnt/covlake/silver/us_counties_covid19_daily')


# COMMAND ----------

# Creating External Tables and Querying Data (Use the covid19_db database)
# Before we move on to the Gold layer, let’s create external tables for our Delta tables in the Silver layer and query the data using SQL.

spark.sql("USE covid19_db")

# Create external tables for the Delta tables in the Silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS us_covid19_daily
USING DELTA
LOCATION '/mnt/covlake/silver/us_covid19_daily'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS us_states_covid19_daily
USING DELTA
LOCATION '/mnt/covlake/silver/us_states_covid19_daily'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS us_counties_covid19_daily
USING DELTA
LOCATION '/mnt/covlake/silver/us_counties_covid19_daily'
""")


# COMMAND ----------

# Query the data in the external tables
df_us_daily = spark.sql("SELECT * FROM us_covid19_daily")
df_us_states_daily = spark.sql("SELECT * FROM us_states_covid19_daily")
df_us_counties_daily = spark.sql("SELECT * FROM us_counties_covid19_daily")

# Display the data
df_us_daily.show()
df_us_states_daily.show()
df_us_counties_daily.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 7: Preparing Data for Visualization in the Gold Layer
# MAGIC
# MAGIC The Gold layer is where we prepare our data for visualization. This involves tasks like aggregating data, calculating metrics, and reshaping data to fit the needs of our visualizations.
# MAGIC

# COMMAND ----------

#  Preparing Data for Visualization in the Gold Layer

from pyspark.sql.functions import concat, lit, col, to_date, year, month

# Read the Delta tables from the Silver layer
df_us_daily_silver = spark.read.format('delta').load('/mnt/covlake/silver/us_covid19_daily')
df_us_states_daily_silver = spark.read.format('delta').load('/mnt/covlake/silver/us_states_covid19_daily')
df_us_counties_daily_silver = spark.read.format('delta').load('/mnt/covlake/silver/us_counties_covid19_daily')

# Convert 'date' from string to date
df_us_daily_silver = df_us_daily_silver.withColumn('date', to_date(df_us_daily_silver['date'], 'yyyyMMdd'))
df_us_states_daily_silver = df_us_states_daily_silver.withColumn('date', to_date(df_us_states_daily_silver['date'], 'yyyyMMdd'))
df_us_counties_daily_silver = df_us_counties_daily_silver.withColumn('date', to_date(df_us_counties_daily_silver['date'], 'yyyyMMdd'))

# Prepare the data
df_us_daily_silver = df_us_daily_silver.withColumn('location', lit('USA')).select('date', 'location', 'positive', col('death').cast('double'))
df_us_states_daily_silver = df_us_states_daily_silver.withColumnRenamed('state', 'location').select('date', 'location', 'positive', col('death').cast('double'))
df_us_counties_daily_silver = df_us_counties_daily_silver.withColumn('location', concat(df_us_counties_daily_silver['county'], lit(', '), df_us_counties_daily_silver['state'])).select('date', 'location', 'cases', 'deaths')

# Rename the 'cases' column in the counties dataframe to match the 'positive' column in the other dataframes
df_us_counties_daily_silver = df_us_counties_daily_silver.withColumnRenamed('cases', 'positive').withColumnRenamed('deaths', 'death')

# Combine the data
df_combined = df_us_daily_silver.union(df_us_states_daily_silver).union(df_us_counties_daily_silver)

# Filter the data (modify this to suit your needs)
df_filtered = df_combined.filter(df_combined['date'] >= '2020-01-01')

# Add 'year' and 'month' columns to the DataFrame
df_filtered = df_filtered.withColumn('year', year(df_filtered['date']))
df_filtered = df_filtered.withColumn('month', month(df_filtered['date']))

# Write the filtered data back to Delta Lake, partitioned by 'year' and 'month'
df_filtered.write.format('delta').mode('overwrite').partitionBy('year', 'month').save('/mnt/covlake/gold/covid19_combined')

# Filter the state-level data
df_state_level = df_us_states_daily_silver.select('date', 'location', 'positive', 'death')

# Write the state-level data back to Delta Lake, partitioned by 'location'
df_state_level.write.format('delta').mode('overwrite').partitionBy('location').save('/mnt/covlake/gold/covid19_state_level')



# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating External Tables and Querying Data
# MAGIC
# MAGIC Before we move on to the next step, let’s create external tables for our Delta tables in the Gold layer and query the data using SQL.

# COMMAND ----------

# Use the covid19_db database
spark.sql("USE covid19_db")

# Create an external table for the Delta table in the Gold layer
spark.sql("""
CREATE TABLE IF NOT EXISTS covid19_combined
USING DELTA
LOCATION '/mnt/covlake/gold/covid19_combined'
""")

# Create an external table for the Delta table in the Gold layer
spark.sql("""
CREATE TABLE IF NOT EXISTS covid19_state_level
USING DELTA
LOCATION '/mnt/covlake/gold/covid19_state_level'
""")

# Query the data in the external tables
df_us_combined = spark.sql("SELECT * FROM covid19_combined")

# Query the data in the external tables with partition clause
df_us_state = spark.sql("SELECT * FROM covid19_state_level where location = 'PA'")

# Display the data
df_us_combined.show(5)
df_us_state.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step 8: Visualizing Data with Databricks
# MAGIC After we have processed and refined our data through the Bronze, Silver, and Gold layers, we can now visualize the data directly within Databricks. Databricks provides built-in data visualization features that we can use to explore our data.
# MAGIC
# MAGIC In this step, we will create two visualizations: a bar chart to show the total positive cases and deaths by location, and a line chart to show the total positive cases and deaths over time.
# MAGIC

# COMMAND ----------

# Import required functions
from pyspark.sql.functions import sum

# Read the combined data from the Gold layer
df_gold = spark.read.format('delta').load('/mnt/covlake/gold/covid19_combined')

# Perform an aggregation by date
df_aggregated_by_date = df_gold.groupBy('date').agg(sum('positive').alias('total_positive'), sum('death').alias('total_death'))

# Display the data for line chart
display(df_aggregated_by_date)

# Read the Delta table from the Gold layer
df_state_level = spark.read.format('delta').load('/mnt/covlake/gold/covid19_state_level')

# Aggregate the data by location
df_aggregated = df_state_level.groupBy('location').agg(sum('positive').alias('total_positive'), sum('death').alias('total_death'))

# Display the aggregated data
display(df_aggregated)

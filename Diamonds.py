# Databricks notebook source
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC create database if not exists Diamonds_db;
# MAGIC
# MAGIC /*https://www.freecodecamp.org/news/databricks-sql-handbook/  */
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC use Diamonds_db;
# MAGIC
# MAGIC
# MAGIC create table if not exists diamonds
# MAGIC using csv
# MAGIC OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from diamonds

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe extended diamonds

# COMMAND ----------

diamondsdf = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
diamondsdf.write.format("delta").mode("overwrite").save("/delta/diamonds")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS diamonds;
# MAGIC  
# MAGIC CREATE TABLE diamonds USING DELTA LOCATION '/delta/diamonds/'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from diamonds

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe extended diamonds

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use diamonds_db;
# MAGIC
# MAGIC INSERT INTO diamonds(_c0, carat, cut,    color,    clarity,    depth,    table,    price,    x,    y,    z) values (53941, 0.22,    'Premium', 'I',    'SI2',    '60.3',    '62.1',    '334',    '3.79',    '3.75',    '2.27');
# MAGIC  
# MAGIC UPDATE diamonds SET carat = 0.20 WHERE _c0 = 53941;
# MAGIC  
# MAGIC select * from diamonds where _c0=53941;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM diamonds where _c0=53941;
# MAGIC  
# MAGIC select * from diamonds where _c0=53941;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE  if NOT EXISTS diamond__mini(_c0 int, carat double, cut string, color string, clarity string, depth double, table double, price int, x double, y double, z double);
# MAGIC
# MAGIC delete from diamond__mini;
# MAGIC  
# MAGIC
# MAGIC INSERT INTO diamond__mini(_c0, carat, cut, color, clarity, depth, table, price, x, y, z) values (1, 0.22, 'Premium', 'I', 'SI2', 60.3, 62.1, 334, 3.79, 3.75, 2.27);
# MAGIC INSERT INTO diamond__mini(_c0, carat, cut, color, clarity, depth, table, price, x, y, z) values (2, 0.22, 'Premium', 'I', 'SI2', 60.3, 62.1, 334, 3.79, 3.75, 2.27);
# MAGIC INSERT INTO diamond__mini(_c0, carat, cut, color, clarity, depth, table, price, x, y, z) values (90000, 0.22, 'Premium', 'I',    'SI2', 60.3, 62.1, 334, 3.79, 3.75, 2.27);
# MAGIC
# MAGIC select * from diamond__mini;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO diamonds as d USING diamond__mini as m
# MAGIC   ON d._c0 = m._c0
# MAGIC   WHEN MATCHED THEN 
# MAGIC     UPDATE SET *
# MAGIC   WHEN NOT MATCHED 
# MAGIC     THEN INSERT * ;
# MAGIC   
# MAGIC select * from diamonds where _c0 in (1 ,2, 90000)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diamonds

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. **Aggregate Functions:** SQL provides a variety of aggregate functions like COUNT, SUM, AVG, MIN, and MAX. By using these functions, you can summarize and visualize data at a higher level. You perform operations such as counting the number of records, calculating the average values, or finding the maximum and minimum values.
# MAGIC 2. **Grouping and Aggregating Data:** The GROUP BY clause in SQL allows you to group data based on specific columns, and then apply aggregate functions to each group. This enables generation of meaningful insights by analyzing data on a category-wise basis.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select color, round(avg(price)) as avg_price from diamonds
# MAGIC group by color ORDER BY color

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT clarity, COUNT(*) AS count
# MAGIC FROM diamonds
# MAGIC GROUP BY clarity
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- This SQL query calculates the average price of diamonds grouped into depth ranges (60-62 and 62-64), and 'Other' for all other depth values, from the 'diamonds' table. The results are ordered in descending order based on the average price.
# MAGIC  
# MAGIC SELECT CASE 
# MAGIC          WHEN depth BETWEEN 60 AND 62 THEN '60-62'
# MAGIC          WHEN depth BETWEEN 62 AND 64 THEN '62-64'
# MAGIC          ELSE 'Other'
# MAGIC        END AS depth_range,
# MAGIC        AVG(CAST(price AS DOUBLE)) AS avg_price
# MAGIC FROM diamonds
# MAGIC GROUP BY depth_range
# MAGIC ORDER BY avg_price DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Price Distribution by Table
# MAGIC
# MAGIC This SQL query calculates the median, first quartile (q1), and third quartile (q3) prices for each unique table value in the diamonds table. It uses the PERCENTILE_CONT() function to calculate these statistical measures.
# MAGIC
# MAGIC The function is applied to the price column, which is cast as a double for accurate calculations. The result set is grouped by the table column, providing insights into the price distribution within each table category.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --  Calculate the median, first quartile (q1), and third quartile (q3) prices for each unique 'table' in the 'diamonds' table based on the 'price' column. The results are grouped by 'table' and provide valuable statistical insights into the price distribution within each category.
# MAGIC  
# MAGIC SELECT table, 
# MAGIC        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(price AS DOUBLE)) AS median_price,
# MAGIC        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST(price AS DOUBLE)) AS q1_price,
# MAGIC        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST(price AS DOUBLE)) AS q3_price
# MAGIC FROM diamonds
# MAGIC GROUP BY table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT x, y, z, AVG(CAST(price AS DOUBLE)) AS avg_price
# MAGIC FROM diamonds
# MAGIC GROUP BY x, y, z
# MAGIC ORDER BY avg_price DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Constraints
# MAGIC - Enforced contraints ensure that the quality and integrity of data added to a table is automatically verified.
# MAGIC
# MAGIC - Informational primary key and foreign key constraints encode relationships between fields in tables and are not enforced.
# MAGIC
# MAGIC You manage CHECK constraints using the ALTER TABLE ADD CONSTRAINT and ALTER TABLE DROP CONSTRAINT commands. ALTER TABLE ADD CONSTRAINT verifies that all existing rows satisfy the constraint before adding it to the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- This SQL code snippet alters the 'diamonds' table by dropping the existing constraint 'id_not_null' if it exists. Then, it adds a new constraint named 'id_not_null' to ensure that the column '_c0' must not contain null values, enforcing data integrity in the table.
# MAGIC  
# MAGIC ALTER TABLE diamonds DROP CONSTRAINT IF EXISTS id_not_null;
# MAGIC ALTER TABLE diamonds ADD CONSTRAINT id_not_null CHECK (_c0 is not null);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- This command will fail as we insert a user with a null id::
# MAGIC INSERT INTO diamonds(_c0, carat, cut,    color,    clarity,    depth,    table,    price,    x,    y,    z) values (null, 0.22,    'Premium', 'I',    'SI2',    '60.3',    '62.1',    '334',    '3.79',    '3.75',    '2.27');

# COMMAND ----------

# MAGIC %md
# MAGIC ###DataFrames
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", inferSchema=True, header=True)

# COMMAND ----------

display(diamonds)
diamonds.printSchema()

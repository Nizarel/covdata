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
# MAGIC use Diamonds_db;

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
# MAGIC A DataFrame is a two-dimensional labeled data structure with columns of potentially different types. You can think of a DataFrame like a spreadsheet, a SQL table, or a dictionary of series objects. **Apache Spark DataFrames provide a rich set of functions (select columns, filter, join, aggregate) that allow you to solve common data analysis problems efficiently.**

# COMMAND ----------

diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", inferSchema=True, header=True)

# COMMAND ----------

display(diamonds)
diamonds.printSchema()

# COMMAND ----------

from pyspark.sql.functions import avg
 
display(diamonds.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

# select color, avg(price) as avg_price from diamonds
# group by color ORDER BY color

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY diamonds;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Restore Setup
# MAGIC
# MAGIC Delta provides built-in support for backup and restore strategies to handle issues like data corruption or accidental data loss. In our scenario, we'll intentionally delete some rows from the main table to simulate such situations.
# MAGIC
# MAGIC We'll then use Delta's restore capability to revert the table to a point in time before the delete operation. By doing so, we can verify if the deletion was successful or if the data was restored correctly to its previous state. This feature ensures data safety and provides an easy way to recover from undesirable changes or failures.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete 10 records from the main table
# MAGIC DELETE FROM diamonds where `_c0`in (1,2,3,4,5,6,7,8,9,10);
# MAGIC SELECT COUNT(*) from diamonds;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM diamonds VERSION AS OF 6;

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table diamonds to version as of 6;
# MAGIC
# MAGIC select * from diamonds
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Autogenerated Fields
# MAGIC
# MAGIC Let us see how to use auto-increment in Delta with SQL. The code below demonstrates the creation of a table called test__autogen with an "autogenerated" field named id. The id column is defined as BIGINT GENERATED ALWAYS AS IDENTITY, meaning its values will be automatically generated by the database engine during the insertion process.
# MAGIC
# MAGIC The id serves as an auto-incrementing primary key for the table, ensuring each new record receives a unique identifier without any manual input. This feature simplifies data insertion and guarantees the uniqueness of records within the table, enhancing database management efficiency.
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS test__autogen (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 10000 INCREMENT BY 1 ), 
# MAGIC   name STRING, 
# MAGIC   surname STRING, 
# MAGIC   email STRING, 
# MAGIC   city STRING) ;
# MAGIC  
# MAGIC -- Note that we don't insert data for the id. The engine will handle that for us:
# MAGIC INSERT INTO test__autogen (name, surname, email, city) VALUES ('Atharva', 'Shah', 'highnessatharva@gmail.com', 'Pune, IN');
# MAGIC INSERT INTO test__autogen (name, surname, email, city) VALUES ('James', 'Dean', 'james@proton.mail', 'Tokyo, JP');
# MAGIC  
# MAGIC -- The ID is automatically generated!
# MAGIC SELECT * from test__autogen;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Delta Table Cloning
# MAGIC
# MAGIC Cloning Delta tables allows you to create a replica of an existing Delta table at a specific version. This feature is particularly valuable when you need to transfer data from a production environment to a staging environment or when archiving a specific version for regulatory purposes.
# MAGIC
# MAGIC There are two types of clones available:
# MAGIC
# MAGIC **1. Deep Clone:** This type of clone **copies both the source table data and metadata to the clone target.** In other words, it replicates the entire table, making it independent of the source.
# MAGIC
# MAGIC **2. Shallow Clone:** A shallow clone **only replicates the table metadata without copying the actual data files to the clone target.** As a result, these clones are more cost-effective to create. However, it's crucial to note that shallow clones act as pointers to the main table. If a VACUUM operation is performed on the original table, it may delete the underlying files and potentially impact the shallow clone.
# MAGIC It's important to remember that any modifications made to either deep or shallow clones only affect the clones themselves and not the source table.
# MAGIC
# MAGIC Cloning Delta tables is a powerful feature that simplifies data replication and version archiving, enhancing data management capabilities within your Delta Lake environment.
# MAGIC
# MAGIC ![Alt text](https://www.freecodecamp.org/news/content/images/2023/08/image-207.png)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Shallow clone (zero copy)
# MAGIC CREATE TABLE IF NOT EXISTS diamonds__shallow__clone
# MAGIC   SHALLOW CLONE diamonds
# MAGIC   VERSION AS OF 6;
# MAGIC  
# MAGIC SELECT * FROM diamonds__shallow__clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Deep clone (copy data)
# MAGIC CREATE TABLE IF NOT EXISTS diamonds__deep__clone
# MAGIC   DEEP CLONE diamonds;
# MAGIC  
# MAGIC SELECT * FROM diamonds__deep__clone;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Delta Magic Commands
# MAGIC
# MAGIC There are convenient shortcuts in Databricks notebooks for managing Delta tables. They simplify common operations like displaying table metadata and running optimization.
# MAGIC
# MAGIC You can use these shortcut commands to improve productivity by streamlining Delta table management tasks within a notebook environment.
# MAGIC
# MAGIC 1. %run: runs a Python file or a notebook.
# MAGIC 2. %sh: executes shell commands on the cluster nodes.
# MAGIC 3. %fs: allows you to interact with the Databricks file system.
# MAGIC 4. %sql: allows you to run SQL queries.
# MAGIC 5. %scala: switches the notebook context to Scala.
# MAGIC 6. %python: switches the notebook context to Python.
# MAGIC 7. %md: allows you to write markdown text.
# MAGIC 8. %r: switches the notebook context to R.
# MAGIC 9. %lsmagic: lists all the available magic commands.
# MAGIC 10. %jobs: lists all the running jobs.
# MAGIC 11. %config: allows you to set configuration options for the notebook.
# MAGIC 12. %reload: reloads the contents of a module.
# MAGIC 13. %pip: allows you to install Python packages.
# MAGIC 14. %load: loads the contents of a file into a cell.
# MAGIC 15. %matplotlib: sets up the matplotlib backend.
# MAGIC

# Databricks notebook source
# The Spark Session is the core location for where Apache Spark related information is stored. We can check it by creating or accessing spark variable 
# Import SparkSession
#from pyspark.sql import SparkSession

# Create SparkSession 
#spark = SparkSession.builder \
#      .master("local[1]") \
#      .appName("Pyspark Session") \
#      .getOrCreate() 

##Check spark session
spark

# COMMAND ----------

#Check publicly available sample datasets

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv 

# COMMAND ----------

#Create a datafrmae : Option 1 : Pointing to ext storage
dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamonds = sqlContext.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(dataPath)
type(diamonds)

# COMMAND ----------

#Drop database if empty else use cascade if non empty

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database zee_ps_9143 cascade;

# COMMAND ----------

#Create Database and register above dataframe as a table. 

# COMMAND ----------

# MAGIC %sql
# MAGIC create database test_9143

# COMMAND ----------

# MAGIC %sql
# MAGIC create table test_9143.diamonds using csv as select * from diamonds

# COMMAND ----------

#Display Diamonds in NB UI

# COMMAND ----------

display(diamonds)

# COMMAND ----------

#Create dataframe : Option 2 : Fn call and defntns
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
empData = spark.createDataFrame(data=data, schema = columns)
empData.show()

# COMMAND ----------

#Viewing Dataframe
diamonds.printSchema()
diamonds.select("carat", "depth", "price").describe().show()

# COMMAND ----------

#Selecting and accessing data
diamonds.filter(diamonds.cut == "Good").display()
diamonds.select(diamonds.cut == "Good").display()

# COMMAND ----------

#Grouping data 
data_subset = diamonds.select("color","price")
data_subset.groupby('color').avg().display()

# COMMAND ----------

#Sample transformations
df1 = diamonds.groupBy("cut", "color").avg("price") # a simple grouping

df2 = df1\
  .join(diamonds, on='color', how='inner')\
  .select("`avg(price)`", "carat")
# a simple join and selecting some columns

# COMMAND ----------

#Query plain using explain()
df2.explain()

# COMMAND ----------

#Action
df2.count()

# COMMAND ----------

#Caching using cache method :: caches data after we compute for the first time 
df2.cache()

# COMMAND ----------

#lets call count() twice , itll take lesser the second time since cahing also happens only once action is called
df2.count()

# COMMAND ----------

df2.count()

# COMMAND ----------

#Accessing Table via SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diamonds limit 5 ;

# COMMAND ----------

out = spark.sql("select * from diamonds limit 5");
out.display();

# COMMAND ----------

#to_pandas() & collect() are intensive

# COMMAND ----------

##UDF
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    # Simply plus one by using pandas Series.
    return series + 1

diamonds.withColumn('pandas_plus_one', pandas_plus_one(diamonds.table)).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#Convert Dataframe into Table

# COMMAND ----------

empData.write.mode("overwrite").saveAsTable("empData2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from empData2 limit 5;

# COMMAND ----------

#Cache and Persist

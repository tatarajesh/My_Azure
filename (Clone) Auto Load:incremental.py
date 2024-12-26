# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

my_schema = StructType([StructField("EmpId", IntegerType(), False)\
                         ,StructField("Dept", StringType(), True)\
                             ,StructField("Gender", StructType(), True)\
                                 ,StructField("Phone", IntegerType(), True) ])

# COMMAND ----------


df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", f"{some_location}")\
    .option("cloudFiles.inferColumnTypes", "true")\
    .load("dbfs:/FileStore/tables/Emp_Data-1.csv")

#here the auto loader is gonna infer and create a schmea in the specified location

# COMMAND ----------

df.writeStream.option("mergeSchema", "true")\
    .option("checkpointLocation", f"{some_location}")\
        .option("cloudFiles.schemaLocation", f"{some_location}")\
        .option("path",f"{some_location}")\
            .table("rajesh.saved_data")

#saving the data in the table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rajesh.saved_data

# COMMAND ----------

# MAGIC %md
# MAGIC we can redefine the schema
# MAGIC
# MAGIC if the defined schmea is not matching with the actual data. we can use something called
# MAGIC .option("cloudFiles.schemaEvolutionMode","failonNewColumns)
# MAGIC
# MAGIC the output is gonna be error

# COMMAND ----------

# MAGIC %md
# MAGIC to see wt the error is and fix it. we can use rescue
# MAGIC
# MAGIC .option("cloudFiles.schemaEvolutionMode","rescue")
# MAGIC
# MAGIC .option("rescuedDataColumn","_rescued_data")
# MAGIC this column gonna give the data for which we han't defined schema
# MAGIC
# MAGIC We aslo have 
# MAGIC - addNewColumns
# MAGIC - none 

# COMMAND ----------

#df.writeStream.trigger(once=True).save("<target_location>")

# COMMAND ----------

#you schema is auto reading perfectly expect 1-column. so we can use schemaHINTS

# .option("cloudFiles.schemaHINTS","column name, DATE")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# File paths
source_path = "dbfs:/FileStore/tables/Emp_Data-1.csv"
checkpoint_path = "dbfs:/FileStore/tables/schema/"

# Read files incrementally
df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.inferColumnTypes", "true")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .schema(schema)  # Provide the schema here
          .load(source_path))

display(df)

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/subdirectory")

# Verify the directory creation
display(dbutils.fs.ls("dbfs:/FileStore/tables/"))

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/schema/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/schema/_schemas/")

# COMMAND ----------


# File paths
source_path = "dbfs:/FileStore/tables/Emp_Data.csv"
checkpoint_path = "dbfs:/FileStore/tables/schema/"

# Read files incrementally
df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.inferColumnTypes", "true")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .schema(schema)  # Provide the schema here
          .load(source_path))

display(df)

# COMMAND ----------

source_path = "/FileStore/tables/Emp_Data.csv"

df = (spark
        .readStream   # a streaming dataframe
        .format("cloudFiles")  # tells Spark to use AutoLoader
        .option("cloudFiles.format", ".csv")  # the actual format of out data files. Tells AutoLoader to expect json files
        .option("cloudFiles.useNotifications",True) # Should AutoLoader use a notification queue
        .schema(my_schema)  # custom schema of the actual data file
        .load(f"{source_path}") # location of the actual data file
    )

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC Data in Azure Data Lake.
# MAGIC We access the data and create Delta table.
# MAGIC
# MAGIC Why only Delta table? 
# MAGIC - optimizes storage(Apache Parquet Format)
# MAGIC - ACID Transcations support
# MAGIC - Time travel, etc..
# MAGIC
# MAGIC To track the changes in the source, perform analysis. We maintain different kinds of table i.e
# MAGIC - Bronz Table (raw data)
# MAGIC - Silver Table(clean/aggregated)
# MAGIC - Gold Table(analytical data)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,create df
p_schema = ["Country", "Product_Name", "Price", "Profit", "Quantity"]

products=[ ("USA", "Product1", 10, 100, 1000), \
            ("India", "Product2", 15, 150, 1500), \
            ("Mexico", "Product3", 20, 200, 2000) ]
    
df = spark.createDataFrame(products, schema=p_schema)

display(df)

# COMMAND ----------

# DBTITLE 1,convert to silver table
df.write.format("Delta").mode("overwrite").saveAsTable("Silver_Products")

#as we didn't mount our detla lake/any storage space. We cannot store in external storage. The table will be stored in spark.sql.warehouse.dir

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_products;

# COMMAND ----------

#Let's perform some aggregations and create gold table

#Let's create a col called new_profit.

df.withColumn("Net_Profit", col("Profit")*col("Quantity")).drop("Profit").drop("Quantity").write.format("Delta").mode("overwrite").saveAsTable("Gold_Products")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_products;

# COMMAND ----------

# DBTITLE 1,Enable CDF on silver_products
# MAGIC %sql
# MAGIC ALTER TABLE silver_products SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1,insert-silver
#Let's add a new role to silver table

new_product = [ ("France","product4",34,56,7653)]

new_p_df = spark.createDataFrame(data=new_product,schema=p_schema)

new_p_df.write.format("delta").mode("append").saveAsTable("Silver_Products")

# COMMAND ----------

# DBTITLE 1,update-silver
# MAGIC %sql
# MAGIC UPDATE silver_products SET Profit=200 WHERE Country='USA';

# COMMAND ----------

# DBTITLE 1,delete-silver
# MAGIC %sql
# MAGIC DELETE from silver_products where Product_Name = "Product3"

# COMMAND ----------

# MAGIC %sql
# MAGIC --DESCRIBE silver_products
# MAGIC
# MAGIC DESCRIBE HISTORY silver_products

# COMMAND ----------

# MAGIC %md
# MAGIC At Version-0 we created our actual gold table. So, from version-1 we need to track changes. Below it won't list v1 bcz it actually not making any changes to data.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Let's see the changes
# MAGIC
# MAGIC select * from table_changes('silver_products',7);

# COMMAND ----------

#we can do the above step using pyspark

changes = spark.read.format("delta").option("readChangeData", True).option("startingVersion", 7).table("Silver_Products")

display(changes)

# COMMAND ----------

updates = changes.filter(changes["_change_type"] == "update_postimage")
deletes = changes.filter(changes._change_type == "delete")
inserts = changes.filter(changes._change_type == "insert")  

changes_to_gold = updates.union(inserts)

display(changes_to_gold)

# COMMAND ----------

#Let's read the gold_products

gold_products = spark.read.format("delta").table("gold_products")


display(gold_products)

#unable to load, bcz of path issues

# COMMAND ----------

changes_to_gold.write.format("delta").mode("") \
    .option("mergeSchema", "true") \
    .option("delta.merge.keyColumns", ProductName) \
    .saveAsTable("gold_products") 

#it's not easy to update the SQL table with python. So, lets move on with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or REPLACE TEMPORARY view silver_table_changes as
# MAGIC select * from table_changes('silver_products',7) where _change_type != 'update_preimage';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_table_changes;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- if there is any primary key constraint, we can use it to do the update the gold table. 
# MAGIC -- as we din't have any primary key, let's consider (country+products) bcz we didnt updated them
# MAGIC
# MAGIC merge into gold_products g using silver_table_changes s on 
# MAGIC     (g.Country = s.Country and g.Product_Name = s.Product_Name) 
# MAGIC         WHEN MATCHED AND s._change_type = 'update_postimage' THEN 
# MAGIC                 UPDATE SET g.Net_Profit= s.Price*s.Quantity
# MAGIC         WHEN MATCHED AND s._change_type = 'delete' THEN
# MAGIC                 DELETE 
# MAGIC         WHEN NOT MATCHED THEN
# MAGIC                 INSERT (Country, Product_Name, Price, Net_Profit) VALUES (s.Country, s.Product_Name, s.Price, s.Price*s.Quantity) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_products;

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.ls('/mnt')

# COMMAND ----------

dbutils.fs.ls('/FileStore')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

# MAGIC %md
# MAGIC we can mount an external storage and we can perform CDF/CDC

# COMMAND ----------

#our data is in data lake storage. Let's mount.

dbutils.fs.mount(
                source="wasbs://raw-data@storageacc1resgrp1.blob.core.windows.net",
                mount_point="/mnt/rawww",
                extra_configs={'fz.azure.account.key.storageacc1resgrp1.blob.core.windows.net':Access_key}
                )
    

# COMMAND ----------

emp = spark.read.csv("/FileStore/tables/Emp_Data.csv", header=True, inferSchema=True)

# COMMAND ----------

emp.write.format("delta").mode("overwrite").saveAsTable("Emp_Data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Emp_Data;

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table emp_data set tblproperties (delta.enableChangeDataFeed=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history Emp_Data;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes("Emp_Data",1);

# COMMAND ----------

emp1= emp.filter(emp.Phone.isNotNull())
display(emp1)

# COMMAND ----------



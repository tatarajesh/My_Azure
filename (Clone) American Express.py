# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/activity.csv", header=True)
df2= spark.read.csv("/FileStore/tables/activity_2.csv", header=True)

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("Date",col('Date').cast(DateType()))

# COMMAND ----------

spent = df.select('Amount').where(df.Amount>0).agg(sum(df.Amount).alias("Spent")).collect()[0]['Spent']
display(spent)

# COMMAND ----------

credit = df.select('Amount').where(df.Amount<0).agg(sum(df.Amount).alias("Credit")).collect()[0]['Credit']
display(credit)

# COMMAND ----------

spent2 = df2.select('Amount').where(df2.Amount>0).agg(sum(df2.Amount).alias("Spent")).collect()[0]['Spent']
display(spent2)

# COMMAND ----------

credit2 = df2.select('Amount').where(df2.Amount<0).show()
display(credit2)

#no credits

# COMMAND ----------

df.select('Date','Description','Amount').where(df.Amount<0).show(truncate=False)

# COMMAND ----------

type(credit)

# COMMAND ----------

Total_spent = spent+spent2
display(Total_spent)

# COMMAND ----------

Balance = Total_spent+credit
display(Balance)

# COMMAND ----------

# MAGIC %md
# MAGIC The amount shown by American Express is 1,542.26
# MAGIC
# MAGIC The next transaction is Mint Mobile~164, which will be in next statement

# COMMAND ----------

from pyspark.sql.functions import month, year

# Extract year and month and get distinct combinations
distinct_months = df.select(year("date").alias("year"), month("date").alias("month")).distinct()
distinct_months.show()


# COMMAND ----------

df.select(upper('Description')).show(truncate=False)

# COMMAND ----------

df.select(length('Description')).show()
substring('col_name',1,8)
upper() , lower()
concat(col1,lit("-"),col2)
trim()
split(col_name,delimiter)
df.filter(df.column_name.like("%pattern%")).show()

df.filter(df.column_name.startswith("prefix")).show()
df.filter(df.column_name.endswith("suffix")).show()

df.filter(df.column_name.isNotNull()).show()


# COMMAND ----------



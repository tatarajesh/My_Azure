# Databricks notebook source
#to connect to DB's

jdbcHostname = "your_sql_server_hostname"
jdbcPort = 1433
jdbcDatabase = "your_database_name"
jdbcUsername = "your_username"
jdbcPassword = " "

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"

connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(url=jdbcUrl, table="your_table_name", properties=connectionProperties)
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

from datetime import date
from pyspark.sql import Row, SparkSession

# COMMAND ----------

df = spark.read.csv(path="/FileStore/tables/Emp_Data.csv", header="True", inferSchema="True")

# COMMAND ----------

df.display() #display(df) both are same

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print(df)

# COMMAND ----------

type(df)

# COMMAND ----------

dir(spark.read)

# COMMAND ----------

help(spark.read)

# COMMAND ----------

#asc() & desc()
df.select('Emp_id','DOB').sort(df.DOB.desc()).show()

# COMMAND ----------

df.orderBy(df.DOB.desc()).show()

# COMMAND ----------

#let's add a duplicate row

new_row = Row(Emp_id=1002, Dept="Marketing", Gender="F", Phone='8162917777', DOB=date(2000,6, 16), Months=3) 

new_row_df = spark.createDataFrame([new_row])

df_updated = df.union(new_row_df)

df_updated.show()

display(df_updated)

# COMMAND ----------

total = df.count()
count = df.dropDuplicates().count()
print("no of duplicate rows:",total-count)

# COMMAND ----------

df.fillna({"Phone": 0000000000}).show()

#df.fillna("NA").show() #it ain't gonna replace integer columns, bcz of type missmatch

# COMMAND ----------

#df.drop("DOB").show()


# COMMAND ----------

data = [(1,'Raj'),(2,'Charan'),(3,'Iqbal')] #we can also use dictionary
schema_values = ['ID','Name']

df_create = spark.createDataFrame(data,schema_values)
df_create.show()

df_create.printSchema()


# COMMAND ----------

#from pyspark.sql.types import *

new_schema = StructType( [ StructField(name="Emp_id", dataType=IntegerType(), nullable=False), 
                           StructField(name="Dept", dataType=StringType(), nullable=True), 
                           StructField(name="Gender", dataType=StringType(), nullable=True), 
                           StructField(name="Phone", dataType=LongType(), nullable=True),
                           StructField(name="DOB", dataType=DateType(), nullable=True),
                           StructField(name="Months", dataType=IntegerType(), nullable=True) ] )
#here the 3rd parameter is True/False i.e nullable or not.

df2 = spark.read.csv(path="/FileStore/tables/Emp_Data.csv", header="True", schema=new_schema)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

#we can also read the file using the following command
df_z = spark.read.format("csv").option("header", True).option("sep", ",").option("inferSchema", "true").load("/FileStore/tables/Emp_Data.csv")

#df2_z = spak.read.format("json").load("/FileStore/tables/Emp_Data.json")


# COMMAND ----------

df.show(n=3, truncate=False) #truncate=3 shows only 3 chars

# COMMAND ----------

df.count()

# COMMAND ----------

#df.select('Dept').show()

#df.select('Dept','Gender').show()

df.select('Dept').distinct().show()

# COMMAND ----------

df_g = df.select('Emp_id', when(df.Gender=='M','Male').when(df.Gender=='F','Female').otherwise('unknown').alias('Sex')).show()

# COMMAND ----------

#df.groupBy('Dept','Gender').count().show() this is gonna be little weird, so we use the pivot 

#pivot

df_g.groupBy('Dept').pivot('Gender').count().show() 

# COMMAND ----------

#Let's add a city column to the DataFrame

df = df.withColumn('City', lit('Kansas City'))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumnRenamed('Months', 'No_of_Months')

#df = df.withColumn(colName='Emp_ID', col=col('ID').cast('Integer'))

#df = df.withColumn('No. of Months', col('No. of Months') * 2)

df = df.withColumn('Status', when(col('No_of_Months') > 15, 'Senior').otherwise('Junior'))

#df = df.withColumn('ShortName', substring('Name', 1, 5))

#df = df.withColumn('Age', col('Age').cast('string'))

#df = df.withColumn('NameUpper', upper(col('Name')))

#df = df.withColumn('Date', to_date(col('DateString'), 'yyyy-MM-dd'))

df.show()

# COMMAND ----------

df.groupBy('Dept').agg(avg('No_of_Months').alias('Average_Months')).show()

#df.groupBy('Dept').agg(sum('Salary').alias('TotalSalary')).orderBy(col('TotalSalary').desc()).show()

# COMMAND ----------

#df.select(df['Dept'].alias('Department')).show()

df.select( concat(col('Dept'), lit('_'), col('Gender')).alias('Dept_Gender') ).show()

# COMMAND ----------

df.filter(col('No_of_Months') > 5).select(upper('Dept'), 'Emp_id').show()

# COMMAND ----------


df.select( expr("No_of_Months * 2").alias('Double_Months'), expr("concat(Dept, '_', Gender)").alias('Dept_Gender') ).show()


# COMMAND ----------

# to save the daframe to a table after data mining

df.createOrReplaceTempView('employees')

#or df.write.mode('overwrite').option('path','/tmp/employees').saveAsTable('employees')

#mode =  ignore, overwrite, append, error

# COMMAND ----------

#df3 = df2.join(df_freq,on='CustomerID',how='inner')

# COMMAND ----------

help(df.join)

# COMMAND ----------

# MAGIC %md
# MAGIC  we can access a column in multiple ways
# MAGIC
# MAGIC  df.select(df.gender).show()
# MAGIC
# MAGIC  df.select(df['gender'].show() )
# MAGIC  
# MAGIC df.select(col('gender').show() )

# COMMAND ----------

#we have array type, struct type, map type.

# COMMAND ----------

# DBTITLE 1,Array Type
data =[ ('abc',[1,2,3]), ('xyz',[6,5,4]), ('pqr',[8,7,9]), ('lmn',[10,11]) ]

schemaa = StructType( [ StructField(name='X',dataType=StringType()), \
                        StructField(name='Y',dataType=ArrayType(IntegerType())) ])

adf = spark.createDataFrame(data, schema=schemaa)

adf.show()
adf.printSchema()

# COMMAND ----------

#adf.withColumn("Seperated array values", explode(col('Y'))).show()

adf.select( size(col('Y')) ).show()# gives size of array

adf.select(array_contains(col('Y'),lit(5)).alias("has_5") ).show()

adf.select( sort_array(col("Y")) ).show()

#slice()

#array_distinct()

#reverse()

#array_union()


# COMMAND ----------

# DBTITLE 1,Struct type
data = [(1,('rajesh','tata'),3000) , (2,('Pranitha','Princess'),1200)]

#this is called structure type | we have array type, map type, struct type.

StructName = StructType( [ StructField("firstname", StringType()), StructField("secondname", StringType()) ])   

schemaa = StructType( [ StructField(name='id',dataType=IntegerType()), \
                        StructField(name='name',dataType=StructName), \
                        StructField(name='Salary', dataType=IntegerType() )  ])


random_df = spark.createDataFrame(data,schema=schemaa)

random_df.show()
random_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Map Type

schema = StructType([ StructField("name", StringType(), True),
        StructField("attributes", MapType(StringType(), IntegerType()), True)])

# Sample data
data = [    ("John", {"height": 180, "weight": 75}),
            ("Jane", {"height": 165, "weight": 60}),
            ("Sam", {"height": 170, "weight": 68}) ]

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.show(truncate=False)


# COMMAND ----------

df.withColumn('Values', map_values(df.attributes)).show(truncate=False)

# COMMAND ----------


df.select("name", map_keys("attributes").alias("keys"), map_values("attributes").alias("values")).show(truncate=False)

# COMMAND ----------


df.select("name", col("attributes")["height"].alias("height")).show()

# COMMAND ----------

data = [("John", "Python,Java,SQL"), ("Jane", "Scala,Python")]
df = spark.createDataFrame(data, ["name", "Y"])

df.show()

df.withColumn('skills_array', split(col('Y'), ',')).show()

#we can only split a string. As it is a string, but using seperator we can achieve array.

# COMMAND ----------

#UserDefined Functions

# Define UDF
def greet(name):
    return f"Hello, {name}"

greet_udf = udf(greet, StringType())

# Use UDF in DataFrame
df.withColumn("Greeting", greet_udf(df.Name)).show()


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

temp = df.collect() #an itterable object

# COMMAND ----------



# COMMAND ----------

# working with JSON data
df.select(col('info.age').alias('Age') ).show() #to access nested columns


# COMMAND ----------

from pyspark.sql.functions import col

# Sample JSON with nested structure
data = [
    {"name": "John", "address": {"city": "New York", "zip": "10001"}},
    {"name": "Jane", "address": {"city": "Los Angeles", "zip": "90001"}}
]

# Create DataFrame
df = spark.createDataFrame(data)

# Flatten the nested 'address' field
df.select(
    "name", 
    col("address.city").alias("city"), 
    col("address.zip").alias("zip")
).show()


# COMMAND ----------

from pyspark.sql.functions import get_json_object

# Sample DataFrame with a JSON string column
data = [("1", '{"name": "John", "age": 30}'), ("2", '{"name": "Jane", "age": 25}')]
df = spark.createDataFrame(data, ["id", "json"])

# Extract the 'name' field from the JSON string column
df.select(
    "id",
    get_json_object("json", "$.name").alias("name")
).show()


#we use the same method i.e get_json_object in hive


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

#We can select/retrive data 2 diff notations
df.select('Phone').show()
#df['phone']

#col('Phone') -> can access in this way too

# COMMAND ----------



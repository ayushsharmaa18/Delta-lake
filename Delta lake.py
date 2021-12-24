# Databricks notebook source
df = spark.createDataFrame([
  (1, 'Peter Parker', 'New York City'),
  (2, 'Eddie Brock', 'San Francisco'),
  (3, 'Gwen Stacy', 'New York City')]
  ,['id', 'name', 'location'])
df.show()


# COMMAND ----------

df.write.format('parquet').save('/FileStore/tables/parquetdata') 

# COMMAND ----------

df.write.format('delta').save('/FileStore/tables/deltadata')

# COMMAND ----------

df1 = spark.createDataFrame([
  (1, 'Peter Parker', 'New York City', 9875654215),
  (2, 'Eddie Brock', 'San Francisco', 8765489540),
  (3, 'Gwen Stacy', 'New York City', 9123545742)]
  ,['id', 'name', 'location', 'contact'])
df1.show()

# COMMAND ----------

df1.write.mode('overwrite').format('parquet').save('/FileStore/tables/parquetdata')

# COMMAND ----------

df1.write.mode('overwrite').format('delta').save('/FileStore/tables/deltadata')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##schema enforcement

# COMMAND ----------

df1.write.mode('overwrite').format('delta').option('mergeSchema','true').save('/FileStore/tables/deltadata')

# COMMAND ----------

spark.read.format('delta').load('/FileStore/tables/deltadata').show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###time travel

# COMMAND ----------

#updated on 10:41
df.write.mode('append').format('delta').option('mergeSchema','true').save('/FileStore/tables/deltadata')

# COMMAND ----------

spark.read.format('delta').load('/FileStore/tables/deltadata').show()

# COMMAND ----------

#Updated at 10:46
df.write.mode('append').format('delta').option('mergeSchema','true').save('/FileStore/tables/deltadata')
spark.read.format('delta').load('/FileStore/tables/deltadata').show()

# COMMAND ----------

spark.read.format('delta').load('/FileStore/tables/deltadata/', timestampAsOf='2021-09-17 10:40:00').show()

# COMMAND ----------

spark.read.format('delta').load('/FileStore/tables/deltadata/', timestampAsOf='2021-09-17 10:45:00').show()

# COMMAND ----------

spark.read.format('delta').load('/FileStore/tables/deltadata/', timestampAsOf='2021-09-17 10:43:00').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##from version

# COMMAND ----------

spark.read.format('delta').load("/FileStore/tables/deltadata/",versionAsOf = 0).show()

# COMMAND ----------

spark.read.format('delta').load("/FileStore/tables/deltadata/",versionAsOf = 2).show()

# COMMAND ----------

spark.read.format('delta').load("/FileStore/tables/deltadata/",versionAsOf = 3).show()

# COMMAND ----------

#for creating delta table firstly we have to import DeltaTable from delta.tables
from delta.tables import DeltaTable
dtable = DeltaTable.forPath(spark,'/FileStore/tables/deltadata/')

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

# Removing null in a dataframe

# COMMAND ----------

dframe = dtable.toDF()

# COMMAND ----------

dframe.na.fill(value=0,subset=["contact"]).show()

# COMMAND ----------

# Removing null from dtable

# COMMAND ----------

# MAGIC %md
# MAGIC ##UPDATE

# COMMAND ----------

#updating the column
dtable.update('contact is NULL' , {'contact':'9131333581'})
dtable.toDF().show()

# COMMAND ----------

dtable.toDF().orderBy('id').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##DELETE

# COMMAND ----------

dtable.delete("name == 'Peter Parker'")

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##creating delta table using %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists mytable 
# MAGIC using delta
# MAGIC location '/FileStore/tables/deltadata'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mytable;

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,name from mytable where location = 'San Francisco';

# COMMAND ----------

# MAGIC %sql 
# MAGIC update mytable set contact = "7415307041" where id = 2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from mytable;

# COMMAND ----------


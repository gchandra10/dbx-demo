# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Connecting to Github Repo from Databricks

# COMMAND ----------

data = [('2021-01-25T13:33:44.343Z'),('2021-02-25T11:12:11.103Z')]

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

df = spark.createDataFrame(data,StringType())

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.withColumn("dt",to_timestamp('value',"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
display(df1)

# COMMAND ----------

df.createOrReplaceTempView("vw_sample")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select date_format(value,'yyyy-MM-dd HH:mm:ss') from vw_sample

# COMMAND ----------



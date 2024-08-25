# Databricks notebook source
df = (spark.read
      .format('parquet')
      .option('header', 'true')
      .option('inferSchema','true')
      .load('/mnt/prd/bronze/gkg/gkg/*.parquet')
      )

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.option('location','mnt/prd/silver/gkg').saveAsTable('silver.gkg')

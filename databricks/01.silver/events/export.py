# Databricks notebook source
# MAGIC %md
# MAGIC ## Bibliotecas

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, sha2, concat_ws
from pyspark.sql.types import *
import time


# COMMAND ----------

extracted = dbutils.fs.ls('/mnt/prd/bronze/events/export/')
max_date = max([time.strftime("%Y%m%d", time.localtime(f[3] / 1000)) for f in extracted if f[1].endswith('parquet')])
list_base = [f[1] 
             for f in extracted if f[1].endswith('parquet')
             and time.strftime("%Y%m%d", time.localtime(f[3] / 1000)) == max_date]


# COMMAND ----------

print(list_base)

# COMMAND ----------

read_list = ['/mnt/prd/bronze/events/export/'+f for f in list_base]

df = (spark.read
      .format('parquet')
      .option('header', 'true')
      .option("ignoreCorruptFiles", "true")
      .option("badRecordsPath", "/mnt/prd/bronze/events/export/corrupted")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .load(read_list)
      )


# COMMAND ----------

df = df.withColumn("GlobalEventID", col('GlobalEventID').cast(LongType()))
df = df.withColumn("Day", col('Day').cast(LongType()))
df = df.withColumn("MonthYear", col('MonthYear').cast(LongType()))
df = df.withColumn("Year", col('Year').cast(LongType()))
df = df.withColumn("FractionDate", col('FractionDate').cast(DoubleType()))

df = df.withColumn("Actor1Code", col('Actor1Code').cast(StringType()))
df = df.withColumn("Actor1Name", col('Actor1Name').cast(StringType()))
df = df.withColumn("Actor1CountryCode", col('Actor1CountryCode').cast(StringType()))
df = df.withColumn("Actor1KnownGroupCode", col('Actor1KnownGroupCode').cast(StringType()))
df = df.withColumn("Actor1EthnicCode", col('Actor1EthnicCode').cast(StringType()))
df = df.withColumn("Actor1Religion1Code", col('Actor1Religion1Code').cast(StringType()))
df = df.withColumn("Actor1Religion2Code", col('Actor1Religion2Code').cast(StringType()))
df = df.withColumn("Actor1Type1Code", col('Actor1Type1Code').cast(StringType()))
df = df.withColumn("Actor1Type2Code", col('Actor1Type2Code').cast(StringType()))
df = df.withColumn("Actor1Type3Code", col('Actor1Type3Code').cast(StringType()))
df = df.withColumn("Actor2Code", col('Actor2Code').cast(StringType()))
df = df.withColumn("Actor2Name", col('Actor2Name').cast(StringType()))
df = df.withColumn("Actor2CountryCode", col('Actor2CountryCode').cast(StringType()))
df = df.withColumn("Actor2KnownGroupCode", col('Actor2KnownGroupCode').cast(StringType()))
df = df.withColumn("Actor2EthnicCode", col('Actor2EthnicCode').cast(StringType()))
df = df.withColumn("Actor2Religion1Code", col('Actor2Religion1Code').cast(StringType()))
df = df.withColumn("Actor2Religion2Code", col('Actor2Religion2Code').cast(StringType()))
df = df.withColumn("Actor2Type1Code", col('Actor2Type1Code').cast(StringType()))
df = df.withColumn("Actor2Type2Code", col('Actor2Type2Code').cast(StringType()))
df = df.withColumn("Actor2Type3Code", col('Actor2Type3Code').cast(StringType()))

df = df.withColumn("IsRootEvent", col('IsRootEvent').cast(LongType()))
df = df.withColumn("EventCode", col('EventCode').cast(StringType()))
df = df.withColumn("EventBaseCode", col('EventBaseCode').cast(StringType()))
df = df.withColumn("EventRootCode", col('EventRootCode').cast(StringType()))
df = df.withColumn("QuadClass", col('QuadClass').cast(LongType()))
df = df.withColumn("GoldsteinScale", col('GoldsteinScale').cast(DoubleType()))
df = df.withColumn("NumMentions", col('NumMentions').cast(LongType()))
df = df.withColumn("NumSources", col('NumSources').cast(LongType()))
df = df.withColumn("NumArticles", col('NumArticles').cast(LongType()))
df = df.withColumn("AvgTone", col('AvgTone').cast(DoubleType()))

df = df.withColumn("Actor1Geo_Type", col('Actor1Geo_Type').cast(LongType()))
df = df.withColumn("Actor1Geo_Fullname", col('Actor1Geo_Fullname').cast(StringType()))
df = df.withColumn("Actor1Geo_CountryCode", col('Actor1Geo_CountryCode').cast(StringType()))
df = df.withColumn("Actor1Geo_ADM1Code", col('Actor1Geo_ADM1Code').cast(StringType()))
df = df.withColumn("Actor1Geo_Lat", col('Actor1Geo_Lat').cast(DoubleType()))
df = df.withColumn("Actor1Geo_Long", col('Actor1Geo_Long').cast(DoubleType()))
df = df.withColumn("Actor1Geo_FeatureID", col('Actor1Geo_FeatureID').cast(StringType()))
df = df.withColumn("Actor2Geo_Type", col('Actor2Geo_Type').cast(LongType()))
df = df.withColumn("Actor2Geo_Fullname", col('Actor2Geo_Fullname').cast(StringType()))
df = df.withColumn("Actor2Geo_CountryCode", col('Actor2Geo_CountryCode').cast(StringType()))
df = df.withColumn("Actor2Geo_ADM1Code", col('Actor2Geo_ADM1Code').cast(StringType()))
df = df.withColumn("Actor2Geo_Lat", col('Actor2Geo_Lat').cast(DoubleType()))
df = df.withColumn("Actor2Geo_Long", col('Actor2Geo_Long').cast(DoubleType()))
df = df.withColumn("Actor2Geo_FeatureID", col('Actor2Geo_FeatureID').cast(StringType()))
df = df.withColumn("ActionGeo_Type", col('ActionGeo_Type').cast(LongType()))
df = df.withColumn("ActionGeo_Fullname", col('ActionGeo_Fullname').cast(StringType()))
df = df.withColumn("ActionGeo_CountryCode", col('ActionGeo_CountryCode').cast(StringType()))
df = df.withColumn("ActionGeo_ADM1Code", col('ActionGeo_ADM1Code').cast(StringType()))
df = df.withColumn("ActionGeo_Lat", col('ActionGeo_Lat').cast(DoubleType()))
df = df.withColumn("ActionGeo_Long", col('ActionGeo_Long').cast(DoubleType()))
df = df.withColumn("ActionGeo_FeatureID", col('ActionGeo_FeatureID').cast(StringType()))

df = df.withColumn("DATEADDED", col('DATEADDED').cast(LongType()))
df = df.withColumn("SOURCEURL", col('SOURCEURL').cast(StringType()))

# COMMAND ----------

df = df.withColumn("_hashed_rows", sha2(concat_ws("||", *df.columns), 256))
df = df.withColumn('_updated_at', current_timestamp())

# COMMAND ----------

df = df.dropDuplicates(['GlobalEventID'])

# COMMAND ----------

from delta.tables import *

delta_df = DeltaTable.forName(spark, "silver.events_export")


delta_df.alias('old') \
  .merge(
    df.alias('new'),
    'new.GlobalEventID = old.GlobalEventID'
  ) \
  .whenMatchedUpdateAll(condition = 'old._hashed_rows <> new._hashed_rows') \
  .whenNotMatchedInsertAll() \
  .execute()


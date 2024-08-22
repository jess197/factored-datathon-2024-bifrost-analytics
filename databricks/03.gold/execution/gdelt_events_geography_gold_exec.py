# Databricks notebook source
# MAGIC %md
# MAGIC #Import Libs
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as fn 
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

gdelt_geography_silver = (
    table('silver.events_export')
    .select(
        fn.col('GlobalEventID'),
        fn.col('Actor1Geo_Type'),
        fn.col('Actor1Geo_Fullname'),
        fn.col('Actor1Geo_CountryCode'),
        fn.col('Actor1Geo_ADM1Code'),
        fn.col('Actor1Geo_Lat'),
        fn.col('Actor1Geo_long').alias('Actor1Geo_Long'),
        fn.col('Actor1Geo_FeatureID'),
        fn.col('Actor2Geo_Type'),
        fn.col('Actor2Geo_Fullname'),
        fn.col('Actor2Geo_CountryCode'),
        fn.col('Actor2Geo_ADM1Code'),
        fn.col('Actor2Geo_Lat'),
        fn.col('Actor2Geo_Long'),
        fn.col('Actor2Geo_FeatureID'),
        fn.col('ActionGeo_Type'),
        fn.col('ActionGeo_Fullname'),
        fn.col('ActionGeo_CountryCode'),
        fn.col('ActionGeo_ADM1Code'),
        fn.col('ActionGeo_Lat'),
        fn.col('ActionGeo_Long'),
        fn.col('ActionGeo_FeatureID'),
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

gdelt_gold = (
    gdelt_geography_silver
)

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark,'/mnt/prd/gold/gdelt_events_geography/')

# COMMAND ----------

start_counter = deltaTable.toDF().count()
print(f"Dados antes do delete: {start_counter}")

(
    deltaTable.alias('delta')
    .merge(
        gdelt_gold.alias('updates')
        , 'delta.GlobalEventID = updates.GlobalEventID'
    ).whenMatchedDelete()
    .execute()
)

end_counter = deltaTable.toDF().count()
print(f"Dados após o delete: {end_counter}")
print(f"Diferença: {start_counter - end_counter}")

# COMMAND ----------

start_counter = deltaTable.toDF().count()
print(f"Dados antes do insert: {start_counter}")

(
    deltaTable.alias('delta')
    .merge(
        gdelt_gold.alias('updates')
        , 'delta.GlobalEventID = updates.GlobalEventID'
    ).whenNotMatchedInsertAll()
    .execute()
)

end_counter = deltaTable.toDF().count()
print(f"Dados após o insert: {end_counter}")
print(f"Diferença: {end_counter - start_counter}")

# Databricks notebook source
# MAGIC %md
# MAGIC #Import Libs

# COMMAND ----------

import pyspark.sql.functions as fn 
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver

# COMMAND ----------

gdelt_actions_silver = (
    table('silver.events_export')
    .select(
        fn.col('GlobalEventID'),
        fn.col('IsRootEvent'),
        fn.col('EventCode'),
        fn.col('EventBaseCode'),
        fn.col('EventRootCode'),
        fn.col('QuadClass'),
        fn.col('GoldsteinScale'),
        fn.col('NumMentions'),
        fn.col('NumSources'),
        fn.col('NumArticles'),
        fn.col('AvgTone')
    )
)


# COMMAND ----------

gdelt_actions_silver_treated = (
    gdelt_actions_silver
    .withColumn("IsRootEvent", fn.col("IsRootEvent").cast("boolean"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

gdelt_gold = (
    gdelt_actions_silver_treated
    .select(
        fn.col('GlobalEventID'),
        fn.col('IsRootEvent'),
        fn.col('EventCode'),
        fn.col('EventBaseCode'),
        fn.col('EventRootCode'),
        fn.col('QuadClass'),
        fn.col('GoldsteinScale'),
        fn.col('NumMentions'),
        fn.col('NumSources'),
        fn.col('NumArticles'),
        fn.col('AvgTone')
    )
)

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark,'/mnt/prd/gold/gdelt_events_actions/')

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

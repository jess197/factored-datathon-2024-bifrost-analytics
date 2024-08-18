# Databricks notebook source
import pyspark.sql.functions as fn 
from delta.tables import DeltaTable

# COMMAND ----------

gdelt_events_data_management_silver = (
    table('silver.events_export')
    .select(
        fn.col('GlobalEventID'),
        fn.col('DATEADDED'),
        fn.col('SOURCEURL')
    )
)


# COMMAND ----------

gdelt_gold = (
    gdelt_events_data_management_silver
)

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark,'/mnt/prd/gold/gdelt_events_data_management/')

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

# Databricks notebook source
import pyspark.sql.functions as fn 
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC select * from silver.events_export

# COMMAND ----------

gdelt_silver = (
    table('silver.events_export')
    .select(
        fn.col('GlobalEventID'),
        fn.col('Day'),
        fn.col('MonthYear'),
        fn.col('Year'),
        fn.col('FractionDate')
    )
)


# COMMAND ----------

gdelt_silver_treated = (
    gdelt_silver
    .withColumn("DayOnly", fn.col("Day").substr(7, 2))
    .withColumn("Month", fn.col("MonthYear").substr(5,2))
    .withColumnRenamed("Day","DateEventOcurred")
)

# COMMAND ----------

# MAGIC %md
# MAGIC display(gdelt_silver_treated)

# COMMAND ----------

gdelt_gold = (
    gdelt_silver_treated
    .select(
        fn.col('GlobalEventID'),
        fn.col('DateEventOcurred'),
        fn.col('DayOnly').alias('Day'),
        fn.col('Month'),
        fn.col('Year'),
        fn.col('MonthYear'),
        fn.col('FractionDate')
    )
)

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark,'/mnt/prd/gold/gdelt_events_and_dates/')

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

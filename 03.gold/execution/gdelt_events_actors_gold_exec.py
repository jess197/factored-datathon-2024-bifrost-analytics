# Databricks notebook source
import pyspark.sql.functions as fn 
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.events_export

# COMMAND ----------

gdelt_actors_silver = (
    table('silver.events_export')
    .select(
        fn.col('GlobalEventID'),
        fn.col('Actor1Code'),
        fn.col('Actor1Name'),
        fn.col('Actor1CountryCode'),
        fn.col('Actor1KnownGroupCode'),
        fn.col('Actor1EthnicCode'),
        fn.col('Actor1Religion1Code'),
        fn.col('Actor1Religion2Code'),
        fn.col('Actor1Type1Code'),
        fn.col('Actor1Type2Code'),
        fn.col('Actor1Type3Code'),
        fn.col('Actor2Code'),
        fn.col('Actor2Name'),
        fn.col('Actor2CountryCode'),
        fn.col('Actor2KnownGroupCode'),
        fn.col('Actor2EthnicCode'),
        fn.col('Actor2Religion1Code'),
        fn.col('Actor2Religion2Code'),
        fn.col('Actor2Type1Code'),
        fn.col('Actor2Type2Code'),
        fn.col('Actor2Type3Code')
    )
)


# COMMAND ----------

gdelt_actors_silver.display()

# COMMAND ----------

gdelt_actors_silver_treated = (
    gdelt_actors_silver
    .withColumn("Actor1EthnicCode", fn.upper("Actor1EthnicCode"))
    .withColumn("Actor2EthnicCode", fn.upper("Actor2EthnicCode"))
)

# COMMAND ----------

gdelt_gold = (
    gdelt_actors_silver_treated
    .select(
        fn.col('GlobalEventID'),
        fn.col('Actor1Code'),
        fn.col('Actor1Name'),
        fn.col('Actor1CountryCode'),
        fn.col('Actor1KnownGroupCode'),
        fn.col('Actor1EthnicCode'),
        fn.col('Actor1Religion1Code'),
        fn.col('Actor1Religion2Code'),
        fn.col('Actor1Type1Code'),
        fn.col('Actor1Type2Code'),
        fn.col('Actor1Type3Code'),
        fn.col('Actor2Code'),
        fn.col('Actor2Name'),
        fn.col('Actor2CountryCode'),
        fn.col('Actor2KnownGroupCode'),
        fn.col('Actor2EthnicCode'),
        fn.col('Actor2Religion1Code'),
        fn.col('Actor2Religion2Code'),
        fn.col('Actor2Type1Code'),
        fn.col('Actor2Type2Code'),
        fn.col('Actor2Type3Code')
    )
)

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark,'/mnt/prd/gold/gdelt_events_actors/')

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

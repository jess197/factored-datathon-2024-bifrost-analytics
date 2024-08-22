# Databricks notebook source
# MAGIC %md
# MAGIC ## Install soda-spark

# COMMAND ----------

# MAGIC %sh sudo apt-get -y install unixodbc-dev libsasl2-dev gcc python3-dev

# COMMAND ----------

pip install -i https://pypi.cloud.soda.io soda-spark-df


# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bibliotecas e Import Config

# COMMAND ----------

from soda.scan import Scan


# COMMAND ----------

# MAGIC %run ./config/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação de Data Quality com SODA

# COMMAND ----------

from soda.scan import Scan
# Create a Spark DataFrame, or use the Spark API to read data and create a DataFrame
df_silver_events_dq = spark.table("delta.`/mnt/prd/silver/events_export`")

# Create a view that SodaCL uses as a dataset
df_silver_events_dq.createOrReplaceTempView("gdelt_events_silver")

# Create a Scan object, set a scan definition, and attach a Spark session
scan = Scan()
scan.set_scan_definition_name("data_quality")
scan.set_data_source_name("gdelt_events")
scan.add_spark_session(spark, data_source_name="gdelt_events")

# Define checks for datasets 
checks  ="""
checks for gdelt_events_silver:
  - row_count <= 2095432:
        name: Data Quantity
  - duplicate_count(GlobalEventID) = 0:
        name: No duplicated events
  - duplicate_percent(sourceurl):
        name: Percent of Duplicate URL
        warn: when != 0%
  - schema:
       warn:
         when schema changes: any
       fail:
         when wrong column type:
            GlobalEventID: bigint
            Day: bigint 
            MonthYear: bigint 
            Year: bigint 
            FractionDate: double 
            Actor1Code: string 
            Actor1Name: string 
            Actor1CountryCode: string 
            Actor1KnownGroupCode: string 
            Actor1EthnicCode: string 
            Actor1Religion1Code: string 
            Actor1Religion2Code: string 
            Actor1Type1Code: string 
            Actor1Type2Code: string 
            Actor1Type3Code: string 
            Actor2Code: string 
            Actor2Name: string 
            Actor2CountryCode: string 
            Actor2KnownGroupCode: string 
            Actor2EthnicCode: string 
            Actor2Religion1Code: string 
            Actor2Religion2Code: string 
            Actor2Type1Code: string 
            Actor2Type2Code: string 
            Actor2Type3Code: string 
            IsRootEvent: bigint 
            EventCode: string 
            EventBaseCode: string 
            EventRootCode: string 
            QuadClass: bigint 
            GoldsteinScale: double 
            NumMentions: bigint 
            NumSources: bigint 
            NumArticles: bigint 
            AvgTone: double 
            Actor1Geo_Type: bigint 
            Actor1Geo_Fullname: string 
            Actor1Geo_CountryCode: string
            Actor1Geo_ADM1Code: string
            Actor1Geo_Lat: double
            Actor1Geo_long: double
            Actor1Geo_FeatureID: string
            Actor2Geo_Type: bigint
            Actor2Geo_Fullname: string
            Actor2Geo_CountryCode: string
            Actor2Geo_ADM1Code: string
            Actor2Geo_Lat: double
            Actor2Geo_Long: double
            Actor2Geo_FeatureID: string
            ActionGeo_Type: bigint
            ActionGeo_Fullname: string
            ActionGeo_CountryCode: string
            ActionGeo_ADM1Code: string
            ActionGeo_Lat: double
            ActionGeo_Long: double 
            ActionGeo_FeatureID: string 
            DATEADDED: bigint
            SOURCEURL: string
"""
# If you defined checks in a file accessible via Spark, you can use the scan.add_sodacl_yaml_file method to retrieve the checks
scan.add_sodacl_yaml_str(checks)

scan.add_configuration_yaml_str(config)

# Execute a scan
scan.execute()

# Check the Scan object for methods to inspect the scan result; the following prints all logs to console
print(scan.get_logs_text())


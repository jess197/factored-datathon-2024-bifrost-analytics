# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from gold.vw_news_detailed

# COMMAND ----------

# MAGIC %sql
# MAGIC     SELECT dm.GlobalEventID
# MAGIC          , nd.SOURCEURL as site_base
# MAGIC          , nd.SOURCEBASEURL as sourceurl
# MAGIC          , nd.TITLE as news_title
# MAGIC          , nd.news_body as news_body
# MAGIC       FROM gold.gdelt_events_news_detailed nd
# MAGIC       JOIN gold.gdelt_events_data_management dm on nd.SOURCEBASEURL = dm.sourceurl
# MAGIC       WHERE nd.news_body != ''

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_news as (
# MAGIC select distinct sourceurl from gold.vw_news_detailed
# MAGIC )
# MAGIC select count(*) from cte_news

# COMMAND ----------

# MAGIC %sql
# MAGIC select  SOURCEURL, COUNT(*) AS COUNT from gold.gdelt_events_data_management GROUP BY SOURCEURL HAVING COUNT >= 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from gold.gdelt_events_news_detailed where SUCCESSFUL = False;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'error' as description, COUNT(*) as count from gold.gdelt_events_news_detailed where news_body = ''
# MAGIC union
# MAGIC select 'success' as description, COUNT(*) as count from gold.gdelt_events_news_detailed where news_body <> '';

# COMMAND ----------

# MAGIC %sql
# MAGIC select ed.GlobalEventID
# MAGIC      , ed.DateEventOcurred
# MAGIC      , ed.Day
# MAGIC      , ed.Month
# MAGIC      , ed.Year
# MAGIC      , ed.MonthYear
# MAGIC      , ed.FractionDate
# MAGIC      , ea.Actor1Code
# MAGIC      , ea.Actor1Name
# MAGIC      , ea.Actor1CountryCode
# MAGIC      , ea.Actor1KnownGroupCode
# MAGIC      , ea.Actor1EthnicCode
# MAGIC      , ea.Actor1Religion1Code
# MAGIC      , ea.Actor1Religion2Code
# MAGIC      , ea.Actor1Type1Code
# MAGIC      , ea.Actor1Type2Code
# MAGIC      , ea.Actor1Type3Code
# MAGIC      , ea.Actor2Code
# MAGIC      , ea.Actor2Name
# MAGIC      , ea.Actor2CountryCode
# MAGIC      , ea.Actor2KnownGroupCode
# MAGIC      , ea.Actor2EthnicCode
# MAGIC      , ea.Actor2Religion1Code
# MAGIC      , ea.Actor2Religion2Code
# MAGIC      , ea.Actor2Type1Code
# MAGIC      , ea.Actor2Type2Code
# MAGIC      , ea.Actor2Type3Code
# MAGIC      , eac.IsRootEvent
# MAGIC      , eac.EventCode
# MAGIC      , eac.EventBaseCode
# MAGIC      , eac.EventRootCode
# MAGIC      , eac.QuadClass
# MAGIC      , eac.GoldsteinScale
# MAGIC      , eac.NumMentions
# MAGIC      , eac.NumSources
# MAGIC      , eac.NumArticles
# MAGIC      , eac.AvgTone
# MAGIC      , eg.Actor1Geo_Type
# MAGIC      , eg.Actor1Geo_Fullname
# MAGIC      , eg.Actor1Geo_CountryCode
# MAGIC      , eg.Actor1Geo_ADM1Code
# MAGIC      , eg.Actor1Geo_Lat
# MAGIC      , eg.Actor1Geo_Long
# MAGIC      , eg.Actor1Geo_FeatureID
# MAGIC      , eg.Actor2Geo_Type
# MAGIC      , eg.Actor2Geo_Fullname
# MAGIC      , eg.Actor2Geo_CountryCode
# MAGIC      , eg.Actor2Geo_ADM1Code
# MAGIC      , eg.Actor2Geo_Lat
# MAGIC      , eg.Actor2Geo_Long
# MAGIC      , eg.Actor2Geo_FeatureID
# MAGIC      , eg.ActionGeo_Type
# MAGIC      , eg.ActionGeo_Fullname
# MAGIC      , eg.ActionGeo_CountryCode
# MAGIC      , eg.ActionGeo_ADM1Code
# MAGIC      , eg.ActionGeo_Lat
# MAGIC      , eg.ActionGeo_Long
# MAGIC      , eg.ActionGeo_FeatureID
# MAGIC      , nd.site_base
# MAGIC      , nd.SOURCEURL
# MAGIC      , nd.TITLE
# MAGIC      , nd.NEWS_BODY
# MAGIC   from gold.vw_news_detailed nd
# MAGIC   join gold.gdelt_events_and_dates ed on nd.GlobalEventID = ed.GlobalEventID
# MAGIC   join gold.gdelt_events_actors ea on ed.GlobalEventID = ea.GlobalEventID
# MAGIC   join gold.gdelt_events_actions eac on ea.GlobalEventID = eac.GlobalEventID
# MAGIC   join gold.gdelt_events_geography eg on eac.GlobalEventID = eg.GlobalEventID
# MAGIC  where nd.sourceurl = 'https://nystateofpolitics.com/state-of-politics/new-york/politics/2023/08/24/hochul-asks-biden-for-more-support-in-addressing-influx-of-migrants-in-ny'
# MAGIC    and ea.Actor1CountryCode = 'USA' or ea.Actor1CountryCode = 'USA'

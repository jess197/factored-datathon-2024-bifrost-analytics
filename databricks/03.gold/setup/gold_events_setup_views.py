# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_news_detailed (GlobalEventID,site_base, sourceurl, title, news_body) as (
# MAGIC     SELECT dm.GlobalEventID
# MAGIC          , nd.SOURCEURL as site_base
# MAGIC          , nd.SOURCEBASEURL as sourceurl
# MAGIC          , nd.TITLE as news_title
# MAGIC          , nd.news_body as news_body
# MAGIC       FROM gold.gdelt_events_news_detailed nd
# MAGIC       JOIN gold.gdelt_events_data_management dm on nd.SOURCEBASEURL = dm.sourceurl
# MAGIC       JOIN gold.gdelt_events_actors ea on dm.GlobalEventID = ea.GlobalEventID
# MAGIC       WHERE nd.news_body != ''     
# MAGIC         and ea.Actor1CountryCode = 'USA' or ea.Actor2CountryCode = 'USA'
# MAGIC )
# MAGIC
# MAGIC
# MAGIC

# Databricks notebook source
# MAGIC %md
# MAGIC # Querys

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH site_base_cte as (
# MAGIC select distinct SPLIT(SPLIT(sourceurl,'://')[1],'/')[0] as site_base
# MAGIC   from gold.gdelt_events_data_management
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_news AS (
# MAGIC SELECT DISTINCT 1 as GlobalEventID
# MAGIC               , SPLIT(SPLIT(dm.sourceurl,'://')[1],'/')[0] as site_base
# MAGIC               , dm.sourceurl
# MAGIC   FROM gold.gdelt_events_data_management dm 
# MAGIC   JOIN gold.gdelt_events_actors at on dm.GlobalEventID = at.GlobalEventID
# MAGIC  WHERE (at.Actor1CountryCode in ('USA','US') or at.Actor2CountryCode in ('USA','US')) 
# MAGIC )
# MAGIC SELECT count(*)
# MAGIC   FROM cte_news 
# MAGIC   where site_base in ('news.yahoo.com',
# MAGIC                       'www.yahoo.com',
# MAGIC                       'www.dailymail.co.uk',
# MAGIC                       'www.theepochtimes.com',
# MAGIC                       'nypost.com',
# MAGIC                       'www.foxnews.com',
# MAGIC                       'www.washingtonpost.com',
# MAGIC                       'www.cbsnews.com',
# MAGIC                       'dailycaller.com',
# MAGIC                       'www.aol.com',
# MAGIC                       'www.nbcnews.com',
# MAGIC                       'www.globalsecurity.org',
# MAGIC                       'www.arkansasonline.com',
# MAGIC                       'www.independent.co.uk',
# MAGIC                       'www.bostonglobe.com',
# MAGIC                       'www.cnn.com',
# MAGIC                       'gazette.com',
# MAGIC                       'menafn.com',
# MAGIC                       'timesofindia.indiatimes.com',
# MAGIC                       'www.forbes.com',
# MAGIC                       'www.jpost.com',
# MAGIC                       'abcnews.go.com',
# MAGIC                       'www.chicagotribune.com',
# MAGIC                       'www.nydailynews.com',
# MAGIC                       'www.startribune.com',
# MAGIC                       'www.breitbart.com:443',
# MAGIC                       'www.sandiegouniontribune.com',
# MAGIC                       'www.ajc.com',
# MAGIC                       'www.hngn.com',
# MAGIC                       'townhall.com',
# MAGIC                       'www.postandcourier.com',
# MAGIC                       'www.mirror.co.uk',
# MAGIC                       'www.scmp.com',
# MAGIC                       'www.nbcchicago.com',
# MAGIC                       'www.firstpost.com',
# MAGIC                       'www.wsws.org',
# MAGIC                       'www.thecentersquare.com',
# MAGIC                       'www.theblaze.com',
# MAGIC                       'www.inquirer.com',
# MAGIC                       'www.heritage.org',
# MAGIC                       'www.newsday.com',
# MAGIC                       'www.zerohedge.com',
# MAGIC                       'www.miragenews.com',
# MAGIC                       'edition.cnn.com',
# MAGIC                       'www.columbian.com',
# MAGIC                       'www.breitbart.com',
# MAGIC                       'www.clevelandjewishnews.com',
# MAGIC                       'chicago.suntimes.com',
# MAGIC                       'www.businessinsider.com',
# MAGIC                       'www.prnewswire.com',
# MAGIC                       'redstate.com',
# MAGIC                       'www.masslive.com',
# MAGIC                       'www.aljazeera.com',
# MAGIC                       'nystateofpolitics.com'
# MAGIC                       )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_duplicates as (
# MAGIC SELECT DISTINCT dm.sourceurl, SPLIT(SPLIT(sourceurl,'://')[1],'/')[0] as site_base
# MAGIC   FROM gold.gdelt_events_data_management dm JOIN gold.gdelt_events_actors ac on dm.GlobalEventID = ac.GlobalEventID WHERE (ac.Actor1CountryCode in ('USA','US') or ac.Actor2CountryCode in ('USA','US')) 
# MAGIC )
# MAGIC select count(*) from cte_duplicates where site_base in ('news.yahoo.com',
# MAGIC 'www.yahoo.com',
# MAGIC 'www.dailymail.co.uk',
# MAGIC 'www.theepochtimes.com',
# MAGIC 'nypost.com',
# MAGIC 'www.foxnews.com',
# MAGIC 'www.washingtonpost.com',
# MAGIC 'www.cbsnews.com',
# MAGIC 'dailycaller.com',
# MAGIC 'www.aol.com',
# MAGIC 'www.nbcnews.com',
# MAGIC 'www.globalsecurity.org',
# MAGIC 'www.arkansasonline.com',
# MAGIC 'www.independent.co.uk',
# MAGIC 'www.bostonglobe.com',
# MAGIC 'www.cnn.com',
# MAGIC 'gazette.com',
# MAGIC 'menafn.com',
# MAGIC 'timesofindia.indiatimes.com',
# MAGIC 'www.forbes.com',
# MAGIC 'www.jpost.com',
# MAGIC 'abcnews.go.com',
# MAGIC 'www.chicagotribune.com',
# MAGIC 'www.nydailynews.com',
# MAGIC 'www.startribune.com',
# MAGIC 'www.breitbart.com:443',
# MAGIC 'www.sandiegouniontribune.com',
# MAGIC 'www.ajc.com',
# MAGIC 'www.hngn.com',
# MAGIC 'townhall.com',
# MAGIC 'www.postandcourier.com',
# MAGIC 'www.mirror.co.uk',
# MAGIC 'www.scmp.com',
# MAGIC 'www.nbcchicago.com',
# MAGIC 'www.firstpost.com',
# MAGIC 'www.wsws.org',
# MAGIC 'www.thecentersquare.com',
# MAGIC 'www.theblaze.com',
# MAGIC 'www.inquirer.com',
# MAGIC 'www.heritage.org',
# MAGIC 'www.newsday.com',
# MAGIC 'www.zerohedge.com',
# MAGIC 'www.miragenews.com',
# MAGIC 'edition.cnn.com',
# MAGIC 'www.columbian.com',
# MAGIC 'www.breitbart.com',
# MAGIC 'www.clevelandjewishnews.com',
# MAGIC 'chicago.suntimes.com',
# MAGIC 'www.businessinsider.com',
# MAGIC 'www.prnewswire.com',
# MAGIC 'redstate.com',
# MAGIC 'www.masslive.com',
# MAGIC 'www.aljazeera.com',
# MAGIC 'nystateofpolitics.com'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from gold.gdelt_events_news_detailed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.gdelt_events_news_detailed where GlobalEventID <> 1

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_teste as (
# MAGIC select distinct sourcebaseurl from gold.gdelt_events_news_detailed
# MAGIC )
# MAGIC select count(*) 
# MAGIC   from cte_teste

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH site_base_cte as (
# MAGIC select distinct GlobalEventID, SPLIT(SPLIT(sourceurl,'://')[1],'/')[0] as site_base
# MAGIC   from gold.gdelt_events_data_management
# MAGIC ),
# MAGIC cte_equal_news as (
# MAGIC select  SOURCEURL, COUNT(*) AS COUNT from gold.gdelt_events_data_management dm join site_base_cte  sb on dm.GlobalEventID = sb.GlobalEventID join gold.gdelt_events_actors ac on dm.GlobalEventID = ac.GlobalEventID WHERE (ac.Actor1CountryCode in ('USA','US') or ac.Actor2CountryCode in ('USA','US')) and site_base in ('news.yahoo.com',
# MAGIC 'www.yahoo.com',
# MAGIC 'www.dailymail.co.uk',
# MAGIC 'www.theepochtimes.com',
# MAGIC 'nypost.com',
# MAGIC 'www.foxnews.com',
# MAGIC 'www.washingtonpost.com',
# MAGIC 'www.cbsnews.com',
# MAGIC 'dailycaller.com',
# MAGIC 'www.aol.com',
# MAGIC 'www.nbcnews.com',
# MAGIC 'www.globalsecurity.org',
# MAGIC 'www.arkansasonline.com',
# MAGIC 'www.independent.co.uk',
# MAGIC 'www.bostonglobe.com',
# MAGIC 'www.cnn.com',
# MAGIC 'www.winnipegfreepress.com',
# MAGIC 'gazette.com',
# MAGIC 'menafn.com',
# MAGIC 'timesofindia.indiatimes.com',
# MAGIC 'www.forbes.com',
# MAGIC 'www.jpost.com',
# MAGIC 'abcnews.go.com',
# MAGIC 'www.chicagotribune.com',
# MAGIC 'www.nydailynews.com',
# MAGIC 'www.startribune.com',
# MAGIC 'www.breitbart.com:443',
# MAGIC 'www.sandiegouniontribune.com',
# MAGIC 'www.ajc.com',
# MAGIC 'www.hngn.com',
# MAGIC 'townhall.com',
# MAGIC 'www.postandcourier.com',
# MAGIC 'www.mirror.co.uk',
# MAGIC 'www.scmp.com',
# MAGIC 'www.nbcchicago.com',
# MAGIC 'www.firstpost.com',
# MAGIC 'www.wsws.org',
# MAGIC 'www.thecentersquare.com',
# MAGIC 'www.theblaze.com',
# MAGIC 'www.inquirer.com',
# MAGIC 'www.heritage.org',
# MAGIC 'www.newsday.com',
# MAGIC 'www.zerohedge.com',
# MAGIC 'www.miragenews.com',
# MAGIC 'edition.cnn.com',
# MAGIC 'www.columbian.com',
# MAGIC 'www.breitbart.com',
# MAGIC 'www.clevelandjewishnews.com',
# MAGIC 'chicago.suntimes.com',
# MAGIC 'www.businessinsider.com',
# MAGIC 'www.prnewswire.com',
# MAGIC 'redstate.com',
# MAGIC 'www.masslive.com',
# MAGIC 'www.aljazeera.com'
# MAGIC )
# MAGIC  GROUP BY SOURCEURL HAVING COUNT >= 2
# MAGIC )
# MAGIC select sum(count) from cte_equal_news

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_news as (
# MAGIC select distinct SPLIT(SPLIT(sourceurl,'://')[1],'/')[0] as site_base, sourceurl
# MAGIC   from gold.gdelt_events_data_management
# MAGIC )
# MAGIC SELECT * from cte_news 
# MAGIC   where site_base in ('news.yahoo.com',
# MAGIC 'www.yahoo.com',
# MAGIC 'www.dailymail.co.uk',
# MAGIC 'www.theepochtimes.com',
# MAGIC 'nypost.com',
# MAGIC 'www.foxnews.com',
# MAGIC 'www.washingtonpost.com',
# MAGIC 'www.cbsnews.com',
# MAGIC 'dailycaller.com',
# MAGIC 'www.aol.com',
# MAGIC 'www.nbcnews.com',
# MAGIC 'www.globalsecurity.org',
# MAGIC 'www.arkansasonline.com',
# MAGIC 'www.independent.co.uk',
# MAGIC 'www.bostonglobe.com',
# MAGIC 'www.cnn.com',
# MAGIC 'www.winnipegfreepress.com',
# MAGIC 'gazette.com',
# MAGIC 'menafn.com',
# MAGIC 'timesofindia.indiatimes.com',
# MAGIC 'www.forbes.com',
# MAGIC 'www.jpost.com',
# MAGIC 'abcnews.go.com',
# MAGIC 'www.chicagotribune.com',
# MAGIC 'www.nydailynews.com',
# MAGIC 'www.startribune.com',
# MAGIC 'www.breitbart.com:443',
# MAGIC 'www.sandiegouniontribune.com',
# MAGIC 'www.ajc.com',
# MAGIC 'www.hngn.com',
# MAGIC 'townhall.com',
# MAGIC 'www.postandcourier.com',
# MAGIC 'www.mirror.co.uk',
# MAGIC 'www.scmp.com',
# MAGIC 'www.nbcchicago.com',
# MAGIC 'www.firstpost.com',
# MAGIC 'www.wsws.org',
# MAGIC 'www.thecentersquare.com',
# MAGIC 'www.theblaze.com',
# MAGIC 'www.inquirer.com',
# MAGIC 'www.heritage.org',
# MAGIC 'www.newsday.com',
# MAGIC 'www.zerohedge.com',
# MAGIC 'www.miragenews.com',
# MAGIC 'edition.cnn.com',
# MAGIC 'www.columbian.com',
# MAGIC 'www.breitbart.com',
# MAGIC 'www.clevelandjewishnews.com',
# MAGIC 'chicago.suntimes.com',
# MAGIC 'www.businessinsider.com',
# MAGIC 'www.prnewswire.com',
# MAGIC 'redstate.com',
# MAGIC 'www.masslive.com',
# MAGIC 'www.aljazeera.com'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Actor1CountryCode from gold.gdelt_events_actors

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SPLIT(SPLIT(sourceurl,'://')[1],'/')[0] as site_base, count(*) as count
# MAGIC   from gold.gdelt_events_data_management dm
# MAGIC   join gold.gdelt_events_actors at on dm.GlobalEventID = at.GlobalEventID
# MAGIC where at.Actor1CountryCode in ('USA','US','USGOV')
# MAGIC group by site_base
# MAGIC order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_news_1 as (
# MAGIC   select distinct SPLIT(SPLIT(sourceurl,'://')[1],'/')[0] as site_base, sourceurl
# MAGIC   from gold.gdelt_events_data_management dm
# MAGIC   join gold.gdelt_events_actors at on dm.GlobalEventID = at.GlobalEventID
# MAGIC   where at.Actor1CountryCode in ('USA','US','USGOV')
# MAGIC ), cte_news_ordered as (
# MAGIC   select site_base
# MAGIC        , sourceurl 
# MAGIC        , row_number() over (PARTITION BY site_base ORDER BY count(*) over (PARTITION BY site_base) desc) as rn 
# MAGIC     from cte_news_1
# MAGIC ), cte_site_base_count as (
# MAGIC   select distinct SPLIT(SPLIT(sourceurl,'://')[1],'/')[0] as site_base, count(*) as count
# MAGIC     from gold.gdelt_events_data_management dm
# MAGIC     join gold.gdelt_events_actors at on dm.GlobalEventID = at.GlobalEventID
# MAGIC   where at.Actor1CountryCode in ('USA','US','USGOV')
# MAGIC   group by site_base
# MAGIC   order by count desc
# MAGIC )
# MAGIC select c.* 
# MAGIC   from cte_news_ordered c
# MAGIC   join  cte_site_base_count cs
# MAGIC   on c.site_base = cs.site_base
# MAGIC   where rn <= 5
# MAGIC   and cs.count >= 416
# MAGIC   order by cs.count desc

from databricks import sql
import pandas as pd
import os

BASE_QUERY = '''
SELECT 
    t1.GlobalEventID,
    t1.Actor1Name,
    t1.Actor1CountryCode,   
    t1.Actor1Type1Code,
    t1.Actor2Name,
    t1.Actor2CountryCode,
    t1.Actor2Type1Code,
    t2.IsRootEvent, 
    t2.EventCode,
    t2.EventBaseCode, 
    t2.EventRootCode, 
    t2.QuadClass, 
    t2.GoldsteinScale, 
    t2.NumMentions, 
    t2.NumSources, 
    t2.NumArticles,
    t2.AvgTone,
    t3.Actor1Geo_Fullname,
    t3.Actor1Geo_Lat,
    t3.Actor1Geo_Long,
    t3.Actor2Geo_Fullname,
    t3.Actor2Geo_Lat,
    t3.Actor2Geo_Long,
    t3.ActionGeo_Fullname,
    t3.ActionGeo_Lat,
    t3.ActionGeo_Long,
    t4.SOURCEURL,
    t5.Day,
    t5.Month,
    t5.Year
FROM 
    `hive_metastore`.`gold`.`gdelt_events_actors` t1
INNER JOIN 
    `hive_metastore`.`gold`.`gdelt_events_actions` t2 ON t1.GlobalEventID = t2.GlobalEventID
INNER JOIN 
    `hive_metastore`.`gold`.`gdelt_events_geography` t3 ON t1.GlobalEventID = t3.GlobalEventID
INNER JOIN 
    `hive_metastore`.`gold`.`gdelt_events_data_management` t4 ON t1.GlobalEventID = t4.GlobalEventID
INNER JOIN 
    `hive_metastore`.`gold`.`gdelt_events_and_dates` t5 ON t1.GlobalEventID = t5.GlobalEventID
INNER JOIN 
    `hive_metastore`.`gold`.`vw_news_detailed` vw1 ON t1.GlobalEventID = vw1.GlobalEventID
WHERE 
    t1.GlobalEventID IS NOT NULL AND
    t1.Actor1Name IS NOT NULL AND
    t1.Actor1CountryCode IS NOT NULL AND
    t1.Actor2Name IS NOT NULL AND
    t1.Actor2CountryCode = '{0}' AND
    t2.IsRootEvent IS NOT NULL AND 
    t2.EventCode = '{1}' AND
    t2.QuadClass IS NOT NULL AND 
    t2.GoldsteinScale IS NOT NULL AND 
    t2.NumMentions IS NOT NULL AND 
    t2.NumSources IS NOT NULL AND 
    t2.NumArticles IS NOT NULL AND
    t2.AvgTone IS NOT NULL AND
    t3.Actor1Geo_Fullname IS NOT NULL AND
    t3.Actor1Geo_Lat IS NOT NULL AND
    t3.Actor1Geo_Long IS NOT NULL AND
    t3.Actor2Geo_Fullname IS NOT NULL AND
    t3.Actor2Geo_Lat IS NOT NULL AND
    t3.Actor2Geo_Long IS NOT NULL AND
    t3.ActionGeo_Fullname IS NOT NULL AND
    t3.ActionGeo_Lat IS NOT NULL AND
    t3.ActionGeo_Long IS NOT NULL AND
    t5.Day IS NOT NULL AND
    t5.Month IS NOT NULL AND
    t5.Year IS NOT NULL
ORDER BY t5.Year desc, t5.Month desc, t5.Day desc
LIMIT 1;
'''


class Databricks:

    def __init__(self):
        self.connection = sql.connect(
            server_hostname=os.environ['server_hostname'],
            http_path=os.environ['http_path'],
            access_token=os.environ['access_token'],
        )

    def get_latest_news(self, actor2, event_code):
        return pd.read_sql(BASE_QUERY.format(actor2, event_code), self.connection)

    def get_news_body(self, global_event_id):
        df = pd.read_sql(f"SELECT * FROM `hive_metastore`.`gold`.`vw_news_detailed` WHERE GlobalEventID = '{global_event_id}' ", self.connection)
        return df.news_body[0]

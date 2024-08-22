# Databricks notebook source
# MAGIC %md
# MAGIC ## CREATE TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.gdelt_events_and_dates (
# MAGIC   GlobalEventID INTEGER COMMENT "Globally unique identifier assigned to each event record that uniquely
# MAGIC identifies it in the master dataset"
# MAGIC   ,DateEventOcurred INTEGER COMMENT "Date the event took place in YYYYMMDD format"
# MAGIC   ,Day INTEGER COMMENT "Day the event took place"
# MAGIC   ,Month INTEGER COMMENT "Month the event took place"
# MAGIC   ,Year INTEGER COMMENT "Alternative formatting of the event date, in YYYY format"
# MAGIC   ,MonthYear INTEGER COMMENT "Alternative formatting of the event date, in YYYYMM format"
# MAGIC   ,FractionDate DOUBLE COMMENT  "Alternative formatting of the event date, computed as YYYY.FFFF, where FFFF is the percentage of the year completed by that day"
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/prd/gold/gdelt_events_and_dates/'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold.gdelt_events_actors (
# MAGIC   GlobalEventID INTEGER COMMENT "Globally unique identifier assigned to each event record that uniquely
# MAGIC identifies it in the master dataset",
# MAGIC   Actor1Code STRING COMMENT "(character or factor) The complete raw CAMEO code for Actor1 (includes geographic, class, ethnic, religious, and type classes)",
# MAGIC   Actor1Name STRING COMMENT "(character) The actual name of the Actor 1.",
# MAGIC   Actor1CountryCode STRING COMMENT "(character or factor) The 3-character CAMEO code for the country affiliation of Actor1",
# MAGIC   Actor1KnownGroupCode STRING COMMENT "(character or factor) If Actor1 is a known IGO/NGO/rebel organization (United Nations, World Bank, al-Qaeda, etc) with its own CAMEO code, this field will contain that code",
# MAGIC   Actor1EthnicCode STRING COMMENT "(character or factor) If the source document specifies the ethnic affiliation of Actor1 and that ethnic group has a CAMEO entry, the CAMEO code is entered here",
# MAGIC   Actor1Religion1Code STRING COMMENT "(character or factor) If the source document specifies the religious affiliation of Actor1 and that religious group has a CAMEO entry, the CAMEO code is entered here",
# MAGIC   Actor1Religion2Code STRING COMMENT "(character or factor) If multiple religious codes are specified for Actor1,this contains the secondary code. Some religion entries automatically use two codes, such as Catholic, which invokes Christianity as Code1 and Catholicism as Code2.",
# MAGIC   Actor1Type1Code STRING COMMENT "(character or factor) The 3-character CAMEO code of the CAMEO 'type' or 'role' of Actor1, if specified. This can be a specific role such as Police Forces, Government, Military, Political Opposition, Rebels, etc, a broad role class such as Education, Elites, Media, Refugees, or organizational classes like Non-Governmental Movement. Special codes such as Moderate and Radical may refer to the operational strategy of a group",
# MAGIC   Actor1Type2Code STRING COMMENT "(character or factor) If multiple type/role codes are specified for Actor1, this returns the second code",
# MAGIC   Actor1Type3Code STRING COMMENT "(character or factor) If multiple type/role codes are specified for Actor1, this returns the third code.",
# MAGIC   Actor2Code STRING COMMENT "(character or factor) The complete raw CAMEO code for Actor2 (includes geographic, class, ethnic, religious, and type classes)",
# MAGIC   Actor2Name STRING COMMENT "(character) The actual name of the Actor 2.",
# MAGIC   Actor2CountryCode STRING COMMENT "(character or factor) The 3-character CAMEO code for the country affiliation of Actor2",
# MAGIC   Actor2KnownGroupCode STRING COMMENT "(character or factor) If Actor2 is a known IGO/NGO/rebel organization (United Nations, World Bank, al-Qaeda, etc) with its own CAMEO code, this field will contain that code",
# MAGIC   Actor2EthnicCode STRING COMMENT "(character or factor) If the source document specifies the ethnic affiliation of Actor2 and that ethnic group has a CAMEO entry, the CAMEO code is entered here",
# MAGIC   Actor2Religion1Code STRING COMMENT "(character or factor) If the source document specifies the religious affiliation of Actor2 and that religious group has a CAMEO entry, the CAMEO code is entered here",
# MAGIC   Actor2Religion2Code STRING COMMENT "(character or factor) If multiple religious codes are specified for Actor2,this contains the secondary code. Some religion entries automatically use two codes, such as Catholic, which invokes Christianity as Code1 and Catholicism as Code2.",
# MAGIC   Actor2Type1Code STRING COMMENT "(character or factor) The 3-character CAMEO code of the CAMEO 'type' or 'role' of Actor2, if specified. This can be a specific role such as Police Forces, Government, Military, Political Opposition, Rebels, etc, a broad role class such as Education, Elites, Media, Refugees, or organizational classes like Non-Governmental Movement. Special codes such as Moderate and Radical may refer to the operational strategy of a group",
# MAGIC   Actor2Type2Code STRING COMMENT "(character or factor) If multiple type/role codes are specified for Actor2, this returns the second code",
# MAGIC   Actor2Type3Code STRING COMMENT "(character or factor) If multiple type/role codes are specified for Actor2, this returns the third code."
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/prd/gold/gdelt_events_actors/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold.gdelt_events_actions (
# MAGIC   GlobalEventID INTEGER COMMENT "Globally unique identifier assigned to each event record that uniquely identifies it in the master dataset",
# MAGIC   IsRootEvent BOOLEAN COMMENT "The system codes every event found in an entire document, using an array of techniques to deference and link information together. A number of previous projects such as the ICEWS initiative have found that events occurring in the lead paragraph of a document tend to be the most “important.” This flag can therefore be used as a proxy for the rough importance of an event to create subsets of the event stream." ,
# MAGIC   EventCode STRING COMMENT "(character or factor) This is the raw CAMEO action code describing the action that Actor1 performed upon Actor2.",
# MAGIC   EventBaseCode STRING COMMENT "(character or factor) CAMEO event codes are defined in a three-level taxonomy. For events at level three in the taxonomy, this yields its level two leaf root node",
# MAGIC   EventRootCode STRING COMMENT "(character or factor) Similar to EventBaseCode, this defines the root-level category the event code falls under. For example, code “0251” (“Appeal for easing of administrative sanctions”) has a root code of “02” (“Appeal”). This makes it possible to aggregate events at various resolutions of specificity. For events at levels two or one, this field will be set to EventCode.",
# MAGIC   QuadClass INTEGER COMMENT "The entire CAMEO event taxonomy is ultimately organized under four primary classifications: Verbal Cooperation, Material Cooperation, Verbal Conflict, and Material Conflict" ,
# MAGIC   GoldsteinScale DOUBLE COMMENT "Each CAMEO event code is assigned a numeric score from -10 to +10 capturing the theoretical potential impact that type of event will have on the stability of a country. This is known as the Goldstein Scale. This field specifies the Goldstein score for each event type",
# MAGIC   NumMentions INTEGER COMMENT "This is the total number of mentions of this event across all source documents. Multiple references to an event within a single document also contribute to this count. This can be used as a method of assessing the “importance” of an event: the more discussion of that event, the more likely it is to be significant.",
# MAGIC   NumSources INTEGER COMMENT "This is the total number of information sources containing one or more mentions of this event. This can be used as a method of assessing the “importance” of an event: the more discussion of that event, the more likely it is to be significant",
# MAGIC   NumArticles INTEGER COMMENT "This is the total number of source documents containing one or more mentions of this event. This can be used as a method of assessing the “importance” of an event: the more discussion of that event, the more likely it is to be significant.",
# MAGIC   AvgTone DOUBLE COMMENT "This is the average “tone” of all documents containing one or more mentions of this event. The score ranges from -100 (extremely negative) to +100 (extremely positive). Common values range between -10 and +10, with 0 indicating neutral. This can be used as a method of filtering the “context” of events as a subtle measure of the importance of an event and as a proxy for the “impact” of that event"
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/prd/gold/gdelt_events_actions/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold.gdelt_events_geography (
# MAGIC   GlobalEventID INTEGER COMMENT "Globally unique identifier assigned to each event record that uniquely identifies it in the master dataset",
# MAGIC   Actor1Geo_Type INTEGER COMMENT "This field specifies the geographic resolution of the match type and holds one of the following values: 1=COUNTRY (match was at the country level), 2=USSTATE (match was to a US state), 3=USCITY (match was to a US city or landmark), 4=WORLDCITY (match was to a city or landmark outside the US), 5=WORLDSTATE (match was to an Administrative Division 1 outside the US – roughly equivalent to a US state)",
# MAGIC   Actor1Geo_Fullname STRING COMMENT "(character) This is the full human-readable name of the matched location. In the case of a country it is simply the country name",
# MAGIC   Actor1Geo_CountryCode STRING COMMENT "(character) This is the 2-character FIPS10-4 country code for the location.",
# MAGIC   Actor1Geo_ADM1Code STRING COMMENT "(character) This is the 2-character FIPS10-4 country code for the location.",
# MAGIC   Actor1Geo_Lat DOUBLE COMMENT "This is the centroid latitude of the landmark for mapping",
# MAGIC   Actor1Geo_Long DOUBLE COMMENT "This is the centroid longitude of the landmark for mapping",
# MAGIC   Actor1Geo_FeatureID STRING COMMENT "This is the GNS or GNIS FeatureID for this location",
# MAGIC   Actor2Geo_Type INTEGER COMMENT "This field specifies the geographic resolution of the match type and holds one of the following values: 1=COUNTRY (match was at the country level), 2=USSTATE (match was to a US state), 3=USCITY (match was to a US city or landmark), 4=WORLDCITY (match was to a city or landmark outside the US), 5=WORLDSTATE (match was to an Administrative Division 1 outside the US – roughly equivalent to a US state)",
# MAGIC   Actor2Geo_Fullname STRING COMMENT "(character) This is the full human-readable name of the matched location. In the case of a country it is simply the country name", 
# MAGIC   Actor2Geo_CountryCode STRING COMMENT "(character) This is the 2-character FIPS10-4 country code for the location.",
# MAGIC   Actor2Geo_ADM1Code STRING COMMENT "(character) This is the 2-character FIPS10-4 country code for the location.",
# MAGIC   Actor2Geo_Lat DOUBLE COMMENT "This is the centroid latitude of the landmark for mapping",
# MAGIC   Actor2Geo_Long DOUBLE COMMENT "This is the centroid longitude of the landmark for mapping",
# MAGIC   Actor2Geo_FeatureID STRING COMMENT "This is the GNS or GNIS FeatureID for this location",
# MAGIC   ActionGeo_Type INTEGER COMMENT "This field specifies the geographic resolution of the match type and holds one of the following values: 1=COUNTRY (match was at the country level), 2=USSTATE (match was to a US state), 3=USCITY (match was to a US city or landmark), 4=WORLDCITY (match was to a city or landmark outside the US), 5=WORLDSTATE (match was to an Administrative Division 1 outside the US – roughly equivalent to a US state)",
# MAGIC   ActionGeo_Fullname STRING COMMENT "(character) This is the full human-readable name of the matched location. In the case of a country it is simply the country name",
# MAGIC   ActionGeo_CountryCode STRING COMMENT "(character) This is the 2-character FIPS10-4 country code for the location.",
# MAGIC   ActionGeo_ADM1Code STRING COMMENT "(character) This is the 2-character FIPS10-4 country code for the location.",
# MAGIC   ActionGeo_Lat DOUBLE COMMENT "This is the centroid latitude of the landmark for mapping",
# MAGIC   ActionGeo_Long DOUBLE COMMENT "This is the centroid longitude of the landmark for mapping",
# MAGIC   ActionGeo_FeatureID STRING COMMENT "This is the GNS or GNIS FeatureID for this location"
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/prd/gold/gdelt_events_geography/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold.gdelt_events_data_management (
# MAGIC   GlobalEventID INTEGER COMMENT "Globally unique identifier assigned to each event record that uniquely
# MAGIC identifies it in the master dataset",
# MAGIC   DATEADDED INTEGER COMMENT "This field stores the date the event was added to the master database",
# MAGIC   SOURCEURL STRING COMMENT "This field is only present in the daily event stream files beginning April
# MAGIC 1, 2013 and lists the URL of the news article the event was found in"
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/prd/gold/gdelt_events_data_management/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.gdelt_events_news_detailed (
# MAGIC   GlobalEventID INTEGER COMMENT "Globally unique identifier assigned to each event record that uniquely
# MAGIC   identifies it in the master dataset",
# MAGIC   SOURCEURL STRING COMMENT "This field is only present in the daily event stream files beginning April
# MAGIC   1, 2013 and lists the URL of the news article the event was found in",
# MAGIC   SOURCEBASEURL STRING COMMENT "This field is the base URL, the site, that the news came from",
# MAGIC   TITLE STRING COMMENT "This field have the title of the news",
# MAGIC   NEWS_BODY STRING COMMENT "This field contains the news body", 
# MAGIC   SUCCESSFUL BOOLEAN COMMENT "This field indicates if the news was successfully imported"
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/prd/gold/gdelt_events_news_detailed/'

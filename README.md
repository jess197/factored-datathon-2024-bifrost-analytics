# FACTORED DATATHON 2024 - BIFROST ANALYTICS
<hr>

## CHALLENGE 
<p> Your mission, should you choose to accept it, is to uncover critical insights from global events that allow for early intervention and strategic planning, ultimately helping businesses and organizations navigate complex and volatile environments.</p>

<p> You will dive deep into the vast and dynamic dataset provided by the Global Database of Events, Language, and Tone 

[GDELT](https://www.gdeltproject.org/).
This year, we're presenting two challenges, designed to test your data skills and creativity.</p>

<hr>

#### NEWS ANALYSIS
<p> Create a summary of news for a specific period picking topics such as Social, Political, and Economical.You should use Natural Language Processing (NLP) techniques to summarize the most relevant news for a topic of your interest in a specific timeframe. General guidelines that will help with the analysis are:

● Avoid using fake news as input, so you should be able to identify them.
</br>
● Minimize Political Polarization, it should be more focused on facts than opinions.
</br>
● Include the geographical and time dimensions within the analysis.
</br>

Your task is to synthesize vast amounts of news data into insightful summaries that capture the essence of current events, helping stakeholders make informed decisions based on reliable and unbiased information.

Goal: Create insightful, unbiased summaries of current events to aid decision-making.
</p>

<hr>

#### RISK ASSESSMENT 
<p> Explore Global Trends and Conduct Risk Assessments across Social, Political, and Economic Domains.You should search and uncover anomalies that enable you to detect early signs of political unrest, armed conflicts, economic crises, or disease outbreaks. The insights enable proactive decision-making and risk mitigation strategies. General guidelines that will help with the analysis are:

● Assessing how Social and Political Risks (News) influence Economic events (Flagged Events).
</br>
● Predict negative sentiment in Economic events based on Social and Political news.
</br>
● Choose one topic—Politics, Social, or Economics—to understand and predict patterns for deteriorating situations in different geographies.
</br>
● Setup alerting mechanisms with possible signs of anomalies.
</br>

Goal: Provide early warning insights and predictions to aid in risk mitigation for businesses and organizations facing social, political, and economic uncertainties.
</p>

<hr>

![Datathon Challenge](docs/img/challenges.png)

<hr>

### 1. DATA ARCHITECTURE

![Project Architecture](docs/img/datathon_architecture_2024.png)

<hr>
<p> The approach that Bifrost Analytics used to provide insights from de Data Sources was creating a Batch Architecture Solution, 
    because the source of GDELT events was just updated daily, so characterizing a batch process. </p>

#### BATCH
<p>
<b>BATCH:</b> We've developed a Python application to handle batch processing of global events data sourced from the GDELT Project. To automate this process efficiently, we utilized Azure Data Factory (ADF), a cloud-based data integration service from Microsoft Azure, which gives us complete control over our data pipelines for seamless automation of ELT and ETL workflows. We set up a Data Lake Gen2 within our Storage Account, enabling hierarchical namespaces to organize the file structure. To optimize storage and performance, we converted the original CSV files into Parquet format and stored them in a container within the Storage Account.

We integrated Databricks with an Azure Storage Account, setting up a Data Lake Gen2 to handle our data storage needs. The Data Lakehouse architecture follows a layered approach to ensure data quality and efficient processing. Inside Databricks, we leverage Delta Lake, an open-source storage layer that brings ACID transactions and scalable metadata handling to Apache Spark - the distributed processing of data, ensuring high performance and scalability. We structured our data into Bronze, Silver and Gold Layers. While maintaining data quality throughout the entire process

This approach enabled us to provide Data Scientist and Data Analyst with access to high-quality, readily available data to drive meaningful decisions, discoveries and valuable insights.
</p> 

#### 1.1 DATA ORIGIN 

<b>A Global Database of Society </b>
<p> Supported by Google Jigsaw, the GDELT Project monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages and identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day, creating a free open platform for computing on the entire world. </p>

### 2. DATA QUALITY

 With [Soda](https://www.soda.io/) we could be able to deliver trust data. During the process we discovered some inconsistences throught this wonderful tool.
 ##### 1. Percentage of Duplicate news URL: 
 <p>
 Thought this warn check we created an incident and discovered that some URL were duplicated since the origin, but in the others fields the data was different, so it wasn't a duplicated value, but some news that included different types of Actors, the GlobalEventID was different, besides others fields. It was possible to discover that 14,8% of gdelt events was with the same link (almost 526k records).
</p>

 ![Percentage of Duplicated URL](docs/img/percentage_duplicated_url.png)

 ##### 2. Schema Check: 
<p>
  Thought this fail check we were able to see if the schema had new columns added or changed the type, and always passed with success.
</p>

 ![Schema_Check](docs/img/schema_check_events.png)

 ##### 3. Duplicated Events: 
 <p>
  Thought this check we were able to see if we had duplicated GlobalEventIds, with means duplicated events, and we hadn't.
</p>

 ![Duplicated Events](docs/img/global_event_id_duplicated.png)


### 3. DATA SCIENCE: 


### 4. DATA ANALYTICS: 

#### Temporal and Geographic Analysis 

![Temporal Geographic Analysis](docs/img/temporal_geographic_analysis.png)

<hr>

#### Actor 1 Analysis

![Actor 1 Analysis](docs/img/actor1_analysis.png)

<hr> 

#### Analysisc of Actions and Impacts

![Actor 1 Analysis](docs/img/analysis_of_actions_and_impacts.png)


#### 4.1 Understanding the Scale of Global Events

##### 1. Global Events Over Time

<p> Historical analysis of global events provides a comprehensive view of their frequency, impact and evolution over time. </p>

##### 2. Geographical Location

<p> The geographic location of events is fundamental to understanding their contexts, regional influences and potential international impacts. </p>

##### 3. Identifying Patterns and Trends

<p> By analyzing temporal and geographic distribution, we can identify patterns and trends, revealing emerging scenarios and strategic opportunities.. </p>


#### 4.2 Understanding the Global Environment

Analyzing global events allows for a better understanding of the ever-changing global environment, providing valuable information for strategic decision-making


##### 1. Risk and Opportunity Identification

<p> By identifying patterns and trends, we can anticipate emerging risks and opportunities, enabling a more effective strategic response. </p>





















### 5. TEAM 🇧🇷
 - Jessica Caroline Costa e Silva - Data Engineer | <b> [LinkedIn](https://www.linkedin.com/in/jessicaccostaesilva/) </b> | <b> [Github](https://github.com/jess197) </b> 
 - Luan José de Almeida Cardoso - Data Engineer | <b> [LinkedIn](https://www.linkedin.com/in/luanjosecar/) </b> | <b> [Github](https://github.com/luanjosecar) </b>
 - Lukas Mendes - Data Scientist | <b> [LinkedIn](https://www.linkedin.com/in/lukasjm/) </b> | <b> [Github](https://github.com/LukasJM) </b>
 - Gabrielle Moura - Data Analyst |<b> [LinkedIn](https://www.linkedin.com/in/gabrielle-moura-a3a782156/) </b> | <b> [Github](https://github.com/gabymoura) </b>



### 6. TECH STACK  
<img src="./docs/img/azure.png" alt="azure" style="vertical-align:top; margin:4px; height:40px; width:40px"><img src="./docs/img/python.png" alt="python" style="vertical-align:top; margin:4px; height:40px; width:40px">










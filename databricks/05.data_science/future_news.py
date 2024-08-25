# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     t1.GlobalEventID,
# MAGIC     t1.Actor1Name,
# MAGIC     t1.Actor1CountryCode,   
# MAGIC     t1.Actor1Type1Code,
# MAGIC     t1.Actor2Name,
# MAGIC     t1.Actor2CountryCode,
# MAGIC     t1.Actor2Type1Code,
# MAGIC     t2.IsRootEvent, 
# MAGIC     t2.EventCode,
# MAGIC     t2.EventBaseCode, 
# MAGIC     t2.EventRootCode, 
# MAGIC     t2.QuadClass, 
# MAGIC     t2.GoldsteinScale, 
# MAGIC     t2.NumMentions, 
# MAGIC     t2.NumSources, 
# MAGIC     t2.NumArticles,
# MAGIC     t2.AvgTone,
# MAGIC     t3.Actor1Geo_Fullname,
# MAGIC     t3.Actor1Geo_Lat,
# MAGIC     t3.Actor1Geo_Long,
# MAGIC     t3.Actor2Geo_Fullname,
# MAGIC     t3.Actor2Geo_Lat,
# MAGIC     t3.Actor2Geo_Long,
# MAGIC     t3.ActionGeo_Fullname,
# MAGIC     t3.ActionGeo_Lat,
# MAGIC     t3.ActionGeo_Long,
# MAGIC     t4.SOURCEURL,
# MAGIC     t5.Day,
# MAGIC     t5.Month,
# MAGIC     t5.Year
# MAGIC FROM 
# MAGIC     `hive_metastore`.`gold`.`gdelt_events_actors` t1
# MAGIC INNER JOIN 
# MAGIC     `hive_metastore`.`gold`.`gdelt_events_actions` t2 ON t1.GlobalEventID = t2.GlobalEventID
# MAGIC INNER JOIN 
# MAGIC     `hive_metastore`.`gold`.`gdelt_events_geography` t3 ON t1.GlobalEventID = t3.GlobalEventID
# MAGIC INNER JOIN 
# MAGIC     `hive_metastore`.`gold`.`gdelt_events_data_management` t4 ON t1.GlobalEventID = t4.GlobalEventID
# MAGIC INNER JOIN 
# MAGIC     `hive_metastore`.`gold`.`gdelt_events_and_dates` t5 ON t1.GlobalEventID = t5.GlobalEventID
# MAGIC WHERE 
# MAGIC     t1.GlobalEventID IS NOT NULL AND
# MAGIC     t1.Actor1Name IS NOT NULL AND
# MAGIC     t1.Actor1CountryCode IS NOT NULL AND
# MAGIC     t1.Actor2Name IS NOT NULL AND
# MAGIC     t1.Actor2CountryCode IS NOT NULL AND
# MAGIC     t2.IsRootEvent IS NOT NULL AND 
# MAGIC     t2.EventCode IS NOT NULL AND
# MAGIC     t2.QuadClass IS NOT NULL AND 
# MAGIC     t2.GoldsteinScale IS NOT NULL AND 
# MAGIC     t2.NumMentions IS NOT NULL AND 
# MAGIC     t2.NumSources IS NOT NULL AND 
# MAGIC     t2.NumArticles IS NOT NULL AND
# MAGIC     t2.AvgTone IS NOT NULL AND
# MAGIC     t3.Actor1Geo_Fullname IS NOT NULL AND
# MAGIC     t3.Actor1Geo_Lat IS NOT NULL AND
# MAGIC     t3.Actor1Geo_Long IS NOT NULL AND
# MAGIC     t3.Actor2Geo_Fullname IS NOT NULL AND
# MAGIC     t3.Actor2Geo_Lat IS NOT NULL AND
# MAGIC     t3.Actor2Geo_Long IS NOT NULL AND
# MAGIC     t3.ActionGeo_Fullname IS NOT NULL AND
# MAGIC     t3.ActionGeo_Lat IS NOT NULL AND
# MAGIC     t3.ActionGeo_Long IS NOT NULL AND
# MAGIC     t5.Day IS NOT NULL AND
# MAGIC     t5.Month IS NOT NULL AND
# MAGIC     t5.Year IS NOT NULL;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum
null_counts = _sqldf.select([sum(col(c).isNull().cast("int")).alias(c) for c in _sqldf.columns])

display(null_counts)

# COMMAND ----------

from pyspark.sql import functions as F

# Suponha que o seu DataFrame seja chamado df e a coluna seja "coluna_exemplo"
_sqldf.groupBy("Actor1Name").count().orderBy(F.desc("count")).show()

# COMMAND ----------

sdf_filtered = _sqldf.filter(_sqldf["Actor1Name"] == "UNITED STATES")

# COMMAND ----------


sdf_filtered.select("GlobalEventID").show()

# COMMAND ----------

display(sdf_filtered.limit(10))

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import CountVectorizer

def preprocess_and_select_features(df2):
    # Adicionar a coluna de próximo valor para os targets
    df = df2.copy()
    df = df.sort_values(by=["Year", "Month", "Day"])
    df['CombinedMetric'] = df['NumMentions'] + df['NumSources'] + df['NumArticles']

        # Remover colunas com mais de 90% do mesmo valor
    threshold = 0.90
    cols_to_drop = []
    for col in df.columns:
        top_freq = df[col].value_counts(normalize=True).max()
        if top_freq > threshold:
            df.drop(columns=[col], inplace=True)
            cols_to_drop.append(col)    
    df.drop(columns=['Actor1Geo_Fullname', 'Actor2Geo_Fullname', 'ActionGeo_Fullname', 'SOURCEURL', 'GlobalEventID'], inplace=True)

    # Transformar colunas categóricas de baixa cardinalidade
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    
    high_cardinality_cols = [col_name for col_name in categorical_cols if df[col_name].nunique() > 200]
    low_cardinality_cols = [col_name for col_name in categorical_cols if col_name not in high_cardinality_cols]
    
    # Substituir valores nulos nas colunas categóricas
    for col_name in low_cardinality_cols:
        if col_name in df.columns:
            df[col_name].fillna('unk', inplace=True)
    
    for col_name in high_cardinality_cols:
        if col_name in df.columns:
            df[col_name].fillna('unk', inplace=True)
    
    geo_cols = ['Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor2Geo_Lat', 'Actor2Geo_Long', 
                'ActionGeo_Lat', 'ActionGeo_Long']
    for col_name in geo_cols:
        if col_name in df.columns:
            df[col_name].fillna(0, inplace=True)
    
    numerical_cols = ['NumMentions', 'NumSources', 'NumArticles', 'Day', 'Month', 'Year']
    for col_name in numerical_cols:
        if col_name in df.columns:
            df[col_name].fillna(0, inplace=True)
    
    # Codificação das colunas categóricas de baixa cardinalidade
    encoders = {col: LabelEncoder().fit(df[col]) for col in low_cardinality_cols if col in df.columns}
    for col, encoder in encoders.items():
        df[col] = encoder.transform(df[col])
    
    # Criação de embeddings para colunas de alta cardinalidade
    for col_name in high_cardinality_cols:
        if col_name in df.columns:
            vectorizer = CountVectorizer()
            svd = TruncatedSVD(n_components=1, random_state=42)
            df[col_name] = svd.fit_transform(vectorizer.fit_transform(df[col_name]))
            # = col_embeddings #pd.DataFrame(col_embeddings, columns=[f"{col_name}_embed_{i}" for i in range(10)])
            #df = pd.concat([df, embedding_df], axis=1)
            #df.drop(columns=[col_name], inplace=True)
            encoders[col_name] = (svd, vectorizer)

    df['goldstein_next'] = df['GoldsteinScale'].shift(-1)
    df['avgtone_next'] = df['AvgTone'].shift(-1)
    df['combined_next'] = df['CombinedMetric'].shift(-1)
    
    # Filtrar linhas onde os targets futuros são nulos
    df = df.dropna(subset=["goldstein_next", "avgtone_next", "combined_next"])

    return df, encoders, high_cardinality_cols, low_cardinality_cols,categorical_cols, cols_to_drop

df_processed, encoders, high_cardinality_cols, low_cardinality_cols,categorical_cols, cols_to_drop  = preprocess_and_select_features(sdf_filtered.toPandas())


# COMMAND ----------

def feature_selection_and_model_training(df, target_col, features):
    X = df[features]
    y = df[target_col]
    
    rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    rf.fit(X, y)
    importances = rf.feature_importances_
    
    top_n = 10
    top_indices = np.argsort(importances)[-top_n:]
    
    X_selected = X.iloc[:, top_indices]
    
    dt = DecisionTreeRegressor(random_state=42)
    dt.fit(X_selected, y)
    
    y_pred = dt.predict(X_selected)
    
    r2 = r2_score(y, y_pred)
    mae = mean_absolute_error(y, y_pred)
    rmse = np.sqrt(mean_squared_error(y, y_pred))
    mape = np.mean(np.abs((y - y_pred) / y)) * 100
    
    print(f"Resultados para {target_col}:")
    print(f"R²: {r2}")
    print(f"MAE: {mae}")
    print(f"RMSE: {rmse}")
    print(f"MAPE: {mape}")
    print("\n")
    print(f'Descritivas da variável target: {target_col}')
    print(df[target_col].describe())
    
    return dt, X_selected.columns

# Treinamento dos modelos
model_gd, features_gd = feature_selection_and_model_training(df_processed, "goldstein_next", df_processed.drop(columns=["goldstein_next"]).columns)
model_avgtone, features_avgtone = feature_selection_and_model_training(df_processed, "avgtone_next", df_processed.drop(columns=["goldstein_next", 'avgtone_next']).columns)
model_combined, features_combined = feature_selection_and_model_training(df_processed, "combined_next", df_processed.drop(columns=["goldstein_next", 'avgtone_next', 'combined_next']).columns)


# COMMAND ----------

ex = sdf_filtered.filter(sdf_filtered["GlobalEventID"] == 1122840843).toPandas()
ex

# COMMAND ----------

def infer_single_example(encoders, model_gd, model_avgtone, model_combined, example1, cols_to_drop, low_cardinality_cols, high_cardinality_cols, features_gd, features_avgtone, features_combined):
    example = example1.copy()
    # Aplicar pré-processamento ao exemplo
    example['CombinedMetric'] = example['NumMentions'] + example['NumSources'] + example['NumArticles']
    example.drop(columns=[col for col in cols_to_drop if col in example.columns], inplace=True)
    # Converter o exemplo para um DataFrame
    GID = example.GlobalEventID
    example.drop(columns=['Actor1Geo_Fullname', 'Actor2Geo_Fullname', 'SOURCEURL', 'ActionGeo_Fullname', 'GlobalEventID'], inplace=True)

    
    # Substituir valores nulos nas colunas categóricas
    for col in low_cardinality_cols:
        if col in example.columns:
          example[col].fillna('unk', inplace=True)
    
    for col in high_cardinality_cols:
        if col in example.columns:
          example[col].fillna('unk', inplace=True)
    
    geo_cols = ['Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor2Geo_Lat', 'Actor2Geo_Long', 
                'ActionGeo_Lat', 'ActionGeo_Long']
    for col in geo_cols:
      if col in example.columns:
        example[col] = example[col].fillna(0)
    
    numerical_cols = ['NumMentions', 'NumSources', 'NumArticles', 'Day', 'Month', 'Year']
    for col in numerical_cols:
      if col in example.columns:
        example[col] = example[col].fillna(0)

    for col, encoder in encoders.items():
      if col in example:
        if col in high_cardinality_cols:
          print('oo:', col)
          col_embb = encoder[0].transform(encoder[1].transform(example[col]))
          example[col] = col_embb
          example.drop(columns=[col], inplace=True)
        elif col in low_cardinality_cols:
          example[col] = encoder.transform(example[col])

    # Inferir cada modelo conforme as restrições
    # Inferência para combined_next
    inputs_combined = example[features_combined].copy()
    combined_pred = model_combined.predict(inputs_combined)[0]
    example['combined_next'] = combined_pred
    

    # Inferência para avgtone_next
    inputs_avgtone = example[features_avgtone].copy()
    avgtone_pred = model_avgtone.predict(inputs_avgtone)[0]
    example['avgtone_next'] = avgtone_pred

    # Inferência para goldstein_next
    inputs_gd = example[features_gd].copy()
    gd_pred = model_gd.predict(inputs_gd)[0]

    return {'GID':GID, 'goldstein_next': gd_pred, 'avgtone_next': avgtone_pred, 'combined_next': combined_pred,
            'goldstein': example['GoldsteinScale'], 'avgtone': example['AvgTone'], 'combined': example['CombinedMetric']}


# COMMAND ----------

preds = infer_single_example(encoders, model_gd, model_avgtone, model_combined, ex, cols_to_drop, low_cardinality_cols, high_cardinality_cols, features_gd, features_avgtone, features_combined)

# COMMAND ----------

preds

# COMMAND ----------

source = """ 
Former President Donald Trump surrendered to Georgia authorities at the Fulton County Jail on Thursday evening and was booked in the criminal case involving efforts to overturn the 2020 presidential election.A grand jury in Georgia indicted Trump and 18 others last week on racketeering and other charges. All 19 defendants charged in the indictment have until Friday to surrender to prosecutors for booking. Eleven of the 19 allies had turned themselves in as of Thursday evening.“You should be able to challenge the election,” Trump told reporters on the tarmac at Hartsfield–Jackson Atlanta International Airport as he was about to depart after being booked. “I thought it was a rigged and stolen election. What has taken place here is a travesty of justice. I did nothing wrong.”Trump’s surrender is distinct from the other three times the former president has been booked as a criminal defendant this year. In those cases, Trump turned himself in at a courthouse rather than a jail, and he did not have a mug shot taken. But in Georgia, authorities released a mug shot to the public on Thursday evening. It is the first time a criminal mug shot has been taken of any U.S. president or former president. In it, Trump is seen with a stern face and arched eyebrows raised.Trump was recorded as being 6 feet, 3 inches tall and weighing 215 pounds, according to booking records.Earlier this week, Trump’s attorneys and prosecutors agreed to a $200,000 bond with release terms that included his promise not to intimidate witnesses or co-defendants in the case.Fulton County District Attorney Fani Willis earlier on Thursday asked a judge to set a trial date of Oct. 23 for all defendants, in response to a request for a speedy trial from one of them, Kenneth Chesebro. Trump’s attorney Steve Sadow quickly objected to such a speedy schedule and indicated he would file a motion to sever the former president’s case from Chesebro’s.The indictment last week came after a two-year investigation that accused Trump of conspiring to derail the Electoral College process, marshaling the Justice Department to bolster his scheme, pressuring Georgia officials to undo the election results and repeatedly lying about fraud allegations to ratchet up pressure.Others charged include former White House chief of staff Mark Meadows, former Justice Department official Jeffrey Clark, and attorneys Rudy Giuliani, John Eastman, Sidney Powell, Jenna Ellis and Chesebro.Meadows and Clark had asked a federal judge to halt their arrest by state authorities, arguing that their case should be moved to federal court because they were acting in their official capacity as Trump administration officials during the time the alleged crimes took place. U.S. District Court Judge Steve Jones rejected their bids on Wednesday, and Meadows turned himself in on Thursday afternoon. Their request to transfer the case to federal court remains pending, and Jones will hold a hearing on that issue on Monday.Trump became the first former president to face any kind of criminal charges after being indicted by a New York grand jury in March over his alleged role in a scheme to pay hush money to a porn star during the 2016 presidential campaign. Since then, Trump has been indicted three more times — two times federally. Trump has denied wrongdoing in all cases.
"""

# COMMAND ----------

preds = {'GID': 1122840843,
 'goldstein_next': -2.0,
 'avgtone_next': -5.93667546174142,
 'combined_next': 5.0,
 'goldstein': 1.9,
 'avgtone': -12.117161,
 'combined': 22}

# COMMAND ----------

from groq import Groq

client = Groq(api_key="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")

def generate_next_news(goldstein, avgtone, combinedmetric, source, goldstein_next, avgtone_next, combinedmetric_next):
    prompt = f"""
You are an assistant specialized in generating news based on provided data. Use the information below to generate the next news article, considering the expected future values.

**Provided Information:**
- **Current news impact (Goldstein):** {goldstein}
- **Current news tone (AvgTone):** {avgtone}
- **Current news importance (CombinedMetric):** {combinedmetric}
- **Raw text of the current news:** {source}

**Explanation of metrics:**
1. **Goldstein Scale:** The Goldstein Scale is a numerical metric ranging from -10 to +10, representing the theoretical impact of an event on a country's stability. A higher value indicates a more significant positive impact, while a lower value indicates a negative impact. For example, a large-scale event with a value close to +10 suggests great stability, while values close to -10 indicate significant instability.

2. **AvgTone:** The Average Tone (AvgTone) is a score ranging from -100 (extremely negative) to +100 (extremely positive), with common values between -10 and +10. A value of 0 indicates a neutral tone. Positive values suggest a more favorable narrative, while negative values indicate a less favorable narrative. The average tone can help understand the emotional context surrounding an event.

3. **CombinedMetric:** The Combined Metric is a combination of the number of articles, sources, and accesses about the event. The higher the value, the greater the importance and impact of the event. A higher value indicates that the event received more coverage and attention.

**Expected Future Values:**
- **Expected future impact (Goldstein_next):** {goldstein_next}
- **Expected future tone (AvgTone_next):** {avgtone_next}
- **Expected future importance (CombinedMetric_next):** {combinedmetric_next}

Your task is to create the next news article based on this data. The news article should reflect the expected changes in impact, tone, and importance. Respond with the text of the next news article in markdown format.
    """

    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": 'You are an assistant that generates news based on data. Generate coherent and relevant news based on the provided information.',
                },
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model="llama-3.1-8b-instant",
            temperature=1,
            max_tokens=1024,
        )
        response = chat_completion.choices[0].message.content
        return response
    except Exception as e:
        print(f"Error generating the next news article: {e}")
        raise

# Example usage
goldstein = preds['goldstein']
avgtone = preds['avgtone']
combinedmetric = preds['combined']
goldstein_next = preds['goldstein_next']
avgtone_next = preds['avgtone_next']
combinedmetric_next =  preds['combined_next']

next_news = generate_next_news(goldstein, avgtone, combinedmetric, source, goldstein_next, avgtone_next, combinedmetric_next)
print(next_news)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Future Update: Trump Case Faces Backlash and Division Among Allies
# MAGIC
# MAGIC #### Georgia, USA - August 26, 2024 - 3:00 PM EST
# MAGIC
# MAGIC In a shocking turn of events, the high-profile case against former President Donald Trump took a new direction this week, as some of his allies began to separate themselves from the embattled former leader. The move comes as a grand jury in Georgia is set to begin deliberations in the upcoming trial of Trump and 18 co-defendants.
# MAGIC
# MAGIC #### A Crisis of Confidence
# MAGIC
# MAGIC According to sources close to the case, the indictment has stirred a sense of unease among some of Trump's closest allies, with some expressing concerns about the former president's behavior and the likelihood of his conviction. As a result, a growing rift has emerged within the Trump camp, with some calling for a more measured approach to the case.
# MAGIC
# MAGIC #### Internal Backlash
# MAGIC
# MAGIC Trump's attorney, Steve Sadow, has been at the forefront of efforts to sever his client's case from that of co-defendant Kenneth Chesebro, citing concerns about the potential ripple effects of a speedy trial. However, this move has been met with resistance from some within Trump's inner circle, who fear that it may undermine the former president's defense strategy.
# MAGIC
# MAGIC #### Factions Forming
# MAGIC
# MAGIC As the case against Trump continues to unfold, observers are beginning to witness the emergence of distinct factions within the Trump camp. While some remain steadfast in their support of the former president, others are growing increasingly skeptical, sparking a crisis of confidence that threatens to destabilize the entire operation.
# MAGIC
# MAGIC #### Coming Days Crucial
# MAGIC
# MAGIC As the grand jury in Georgia prepares to begin deliberations, the coming days will be crucial in determining the fate of Trump's case. Will the former president be able to rally his supporters, or will the growing divisions within his camp ultimately prove to be his undoing?
# MAGIC
# MAGIC #### Quotes
# MAGIC
# MAGIC * "The Trump team is facing a perfect storm of adversity," said [name], a prominent analyst. "The stakes are incredibly high, and the pressure is mounting. It remains to be seen whether Trump's allies will stand by him, or if they'll begin to distance themselves from his beleaguered campaign."
# MAGIC * "This case has exposed deep-seated divisions within the Trump camp," added [name], a rival strategist. "As the case against Trump continues to rattle on, his support base is beginning to erode. It's only a matter of time before his campaign collapses entirely."
# MAGIC
# MAGIC #### Predictions
# MAGIC
# MAGIC According to some observers, the backlash against Trump is only just beginning. With his allies turning against him and his defense team growing increasingly desperate, the writing appears to be on the wall. "It's time for Trump to face the music," said [name], a seasoned expert. "His time in the spotlight is drawing to a close, and soon he'll have to answer for his actions."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC This news article reflects the expected future values in the tone (AvgTone_next = -5.93667546174142) and importance (CombinedMetric_next = 5.0) of the case against Trump, as well as the anticipated decline in the impact of the event (Goldstein_next = -2.0).

# COMMAND ----------

# MAGIC %md
# MAGIC # Woking

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, countDistinct, lit
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

def preprocess_data(sdf_filtered):
    # Tratamento de valores nulos para colunas categóricas
    categorical_cols = [col_name for col_name in sdf_filtered.columns if 'Name' in col_name or 'Code' in col_name or 'QuadClass' in col_name]
    for col_name in categorical_cols:
        sdf_filtered = sdf_filtered.withColumn(col_name, when(col(col_name).isNull(), 'unk').otherwise(col(col_name)))

    # Tratamento de valores nulos em colunas de coordenadas geográficas com 0
    geo_cols = ['Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor2Geo_Lat', 'Actor2Geo_Long', 
                'ActionGeo_Lat', 'ActionGeo_Long']
    for geo_col in geo_cols:
        sdf_filtered = sdf_filtered.fillna({geo_col: 0})
    
    # Tratamento de valores nulos em colunas numéricas com 0
    numerical_cols = ['NumMentions', 'NumSources', 'NumArticles', 'Day', 'Month', 'Year']
    for num_col in numerical_cols:
        sdf_filtered = sdf_filtered.fillna({num_col: 0})

    # StringIndexer e OneHotEncoder para colunas categóricas
    indexers = []
    encoders = []
    
    for col_name in categorical_cols:
        unique_count = sdf_filtered.select(countDistinct(col_name)).collect()[0][0]
        if unique_count > 1:
            if unique_count > 100:
                indexer = StringIndexer(inputCol=col_name, outputCol=col_name+"_index")
                indexers.append(indexer)
            else:
                indexer = StringIndexer(inputCol=col_name, outputCol=col_name+"_index")
                encoder = OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol=col_name+"_ohe")
                indexers.append(indexer)
                encoders.append(encoder)
        else:
            # Substituir a coluna por uma constante se houver menos de 2 valores distintos
            sdf_filtered = sdf_filtered.withColumn(col_name+"_index", lit(0))
            print(f"A coluna {col_name} foi substituída por uma constante porque tem menos de 2 valores distintos.")
    
    # Criando a métrica combinada
    sdf_filtered = sdf_filtered.withColumn('CombinedMetric', col('NumMentions') + col('NumSources') + col('NumArticles'))

    # Vetorizando as colunas numéricas, geográficas e categóricas processadas
    assembler = VectorAssembler(inputCols=numerical_cols + geo_cols + ['CombinedMetric'] + 
                                [col_name+"_index" for col_name in categorical_cols if col_name+"_index" in sdf_filtered.columns] + 
                                [col_name+"_ohe" for col_name in categorical_cols if col_name+"_ohe" in sdf_filtered.columns], 
                                outputCol="features")
    
    # Normalizando as features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

    # Construindo o Pipeline
    pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler])
    
    # Ajustando o pipeline no DataFrame
    model = pipeline.fit(sdf_filtered)
    sdf_preprocessed = model.transform(sdf_filtered)
    
    return sdf_preprocessed.select("scaledFeatures", "GoldsteinScale", "AvgTone", "CombinedMetric")

# Executando o pré-processamento
sdf_filtered_processed = preprocess_data(sdf_filtered)


# COMMAND ----------

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import mean, stddev, min, max, expr, percentile_approx

def feature_selection_and_model_training(sdf_preprocessed, target_col):
    # Selecionando features
    slicer = VectorSlicer(inputCol="scaledFeatures", outputCol="selectedFeatures", 
                          indices=list(range(len(sdf_preprocessed.select("scaledFeatures").first()[0]))))  # Selecionar todas as features inicialmente
    sdf_sliced = slicer.transform(sdf_preprocessed)
    
    # Treinamento do modelo
    dt = DecisionTreeRegressor(featuresCol="selectedFeatures", labelCol=target_col)
    model = dt.fit(sdf_sliced)
    
    # Predição
    predictions = model.transform(sdf_sliced)
    
    # Avaliação
    evaluator_r2 = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="r2")
    r2 = evaluator_r2.evaluate(predictions)
    
    evaluator_mae = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="mae")
    mae = evaluator_mae.evaluate(predictions)
    
    evaluator_rmse = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
    rmse = evaluator_rmse.evaluate(predictions)
    
    # Cálculo do MAPE
    mape = predictions.select(expr(f"avg(abs((prediction - {target_col})/{target_col}))").alias("MAPE")).collect()[0]["MAPE"]
    
    print(f"Resultados para {target_col}:")
    print(f"R²: {r2}")
    print(f"MAE: {mae}")
    print(f"RMSE: {rmse}")
    print(f"MAPE: {mape}")
    print("\n")

    # Estatísticas descritivas
    descriptive_stats = sdf_preprocessed.select(
        mean(target_col).alias("mean"),
        percentile_approx(target_col, 0.5).alias("median"),
        stddev(target_col).alias("stddev"),
        min(target_col).alias("min"),
        max(target_col).alias("max")
    )
    descriptive_stats.show()

# Executando o processo para cada variável-alvo
for target in ["GoldsteinScale", "AvgTone", "CombinedMetric"]:
    feature_selection_and_model_training(sdf_filtered_processed, target)


# COMMAND ----------



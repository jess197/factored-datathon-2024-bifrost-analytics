from groq import Groq
import pickle
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
import os

class NewsGenerator:

    def __init__(self):
        self.client = Groq(api_key=os.environ["groq_apy_key"])
        with open(os.environ['model_base_folder'] + '/encoders.pkl', 'rb') as f:
            self.encoders = pickle.load(f)
        with open(os.environ['model_base_folder'] + '/model_gd.pkl', 'rb') as f:
            self.model_gd = pickle.load(f)
        with open(os.environ['model_base_folder'] + '/model_avgtone.pkl', 'rb') as f:
            self.model_avgtone = pickle.load(f)
        with open(os.environ['model_base_folder'] + '/model_combined.pkl', 'rb') as f:
            self.model_combined = pickle.load(f)
        with open(os.environ['model_base_folder'] + '/columns_to_filter.pkl', 'rb') as f:
            cols_to_filter = pickle.load(f)
            self.cols_to_drop = cols_to_filter['cols_to_drop']
            self.low_cardinality_cols = cols_to_filter['low_cardinality_cols']
            self.high_cardinality_cols = cols_to_filter['high_cardinality_cols']
        with open(os.environ['model_base_folder'] + '/features_gd.pkl', 'rb') as f:
            self.features_gd = pickle.load(f)
        with open(os.environ['model_base_folder'] + '/features_avgtone.pkl', 'rb') as f:
            self.features_avgtone = pickle.load(f)
        with open(os.environ['model_base_folder'] + '/features_combined.pkl', 'rb') as f:
            self.features_combined = pickle.load(f)

    def __generate_next_news(self, goldstein, avgtone, combinedmetric, source, goldstein_next, avgtone_next, combinedmetric_next):
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
            chat_completion = self.client.chat.completions.create(
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

    def __infer_single_example(self, encoders, model_gd, model_avgtone, model_combined, example1, cols_to_drop, low_cardinality_cols, high_cardinality_cols, features_gd, features_avgtone, features_combined):
        example = example1.copy()

        example['CombinedMetric'] = example['NumMentions'] + example['NumSources'] + example['NumArticles']
        example.drop(columns=[col for col in cols_to_drop if col in example.columns], inplace=True)

        GID = example.GlobalEventID
        example.drop(columns=['Actor1Geo_Fullname', 'Actor2Geo_Fullname', 'SOURCEURL', 'ActionGeo_Fullname', 'GlobalEventID'], inplace=True)

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

        inputs_combined = example[features_combined].copy()
        combined_pred = model_combined.predict(inputs_combined)[0]
        example['combined_next'] = combined_pred

        inputs_avgtone = example[features_avgtone].copy()
        avgtone_pred = model_avgtone.predict(inputs_avgtone)[0]
        example['avgtone_next'] = avgtone_pred

        inputs_gd = example[features_gd].copy()
        gd_pred = model_gd.predict(inputs_gd)[0]

        return {'GID':GID, 'goldstein_next': gd_pred, 'avgtone_next': avgtone_pred, 'combined_next': combined_pred,
                'goldstein': example['GoldsteinScale'], 'avgtone': example['AvgTone'], 'combined': example['CombinedMetric']}

    def get_next_news(self, df_news, news_body):
        preds = self.__infer_single_example(self.encoders, self.model_gd, self.model_avgtone, self.model_combined, df_news, self.cols_to_drop, self.low_cardinality_cols, self.high_cardinality_cols, self.features_gd, self.features_avgtone, self.features_combined)

        goldstein = preds['goldstein']
        avgtone = preds['avgtone']
        combinedmetric = preds['combined']
        goldstein_next = preds['goldstein_next']
        avgtone_next = preds['avgtone_next']
        combinedmetric_next = preds['combined_next']
        return self.__generate_next_news(goldstein, avgtone, combinedmetric, news_body, goldstein_next, avgtone_next, combinedmetric_next)

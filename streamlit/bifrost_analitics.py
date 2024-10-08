import streamlit as st
import os

from data.databricks import Databricks
from data.news_generator import NewsGenerator

actor2_dict = {
    'UNITED STATES' : 'USA',
    'ISRAEL' : 'ISR',
    'UKRAINE' : 'UKR',
    'RUSSIA' : 'RUS',
    'CHINA' : 'CHN',
    'PALESTINE' : 'PSE'
}

event_type_dict = {
    'Make a visit' : '042',
    'Host a visit' : '043',
    'Consult, not specified below' : '040',
    'Make statement, not specificed below' : '010',
    'Praise or endorse' : '051'
}

st.image(os.environ['model_base_folder'] + '/bifrost-analytics-logo.png')

st.write("Please select actor and event type to generate the news. Notice that the Actor 1 is always United States.")

st.write("* Please notice that the first attempt of generating the news may take a while, because of the Databrick's Cluster inicialization")

actor2 = st.selectbox(
    "Select the Actor 2",
    ('UNITED STATES', 'ISRAEL', 'UKRAINE', 'RUSSIA', 'CHINA', 'PALESTINE'),
)

event_type = st.selectbox(
    "Select the Event Type",
    ('Make a visit', 'Host a visit', 'Consult, not specified below', 'Make statement, not specificed below', 'Praise or endorse'),
)

if st.button("Generate"):
    st.write("Generating news for:", actor2, " and ", event_type)

    dtbricks = Databricks()
    news_generator = NewsGenerator()
    
    df = dtbricks.get_latest_news(actor2_dict[actor2], event_type_dict[event_type])
    news_body = dtbricks.get_news_body(df['GlobalEventID'][0])
    next_news = news_generator.get_next_news(df, news_body)

    col1, col2 = st.columns(2)

    with col1:
        st.header("Original news:")
        st.write(news_body)

    with col2:
        st.header("Generated(T+1) news:")
        st.write(next_news)
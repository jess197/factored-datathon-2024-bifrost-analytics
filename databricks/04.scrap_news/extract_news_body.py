# Databricks notebook source
# MAGIC %md
# MAGIC # Libs

# COMMAND ----------

import requests
from bs4 import BeautifulSoup


from pyspark.sql.functions import col
from pyspark.sql import Row
from pyspark.sql.types import *

from queue import Queue
from threading import Thread
import time

# COMMAND ----------

# MAGIC %md
# MAGIC # Scrapper's Class

# COMMAND ----------

class NYStateOfPolitics():
  def load_news(self, news_link):
      response = requests.get(news_link)
      self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="text parbase")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class YahooNews():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="caas-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class Yahoo():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="caas-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class DailyMail():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    for line in self.news_content.find_all("p", class_="mol-para-with-font"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class TheEpochTimes():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="post_content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class TheEpochTimes():
  def load_news(self, news_link):
      response = requests.get(news_link)
      self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="post_content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class Foxnews():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string.replace(' | Fox News','')
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts.replace('CLICK TO GET THE FOX NEWS APP','')

# COMMAND ----------

class WashingtonPost():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="grid-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class CBSNews():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string.strip()
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("section", class_="content__body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class DailyCaller():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string.replace('	','').replace(' | ','').replace('  The Daily Caller','').strip()
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article-content mb-2 pb-2 tracking-tight")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class AOL():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="caas-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class NBCNews():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article-body__content")
    if(len(news_text)  == 0):
      news_text = self.news_content.find_all("div", class_="showblog-body__content")

    for line in news_text[0].find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class GlobalSecurity():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find("div", {"id": "content"})

    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class StarTribune():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find("div", {"data-testid": "article-body"})

    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class NYDailyNews():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="body-copy")[0]

    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class ChicagoTribune():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article-body")[0]

    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class ABCNews():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find("div", {"data-testid":"prism-article-body"})

    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class JPost():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("section", class_="article-inner-content")[0]

    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.strip()

    return news_full_texts

# COMMAND ----------

class Forbes():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("article")[0]

    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.strip()

    return news_full_texts

# COMMAND ----------

class BreitBart():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="entry-content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class ArkansasOnline():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article__body")[0]
    for line in news_text.find_all("p"):
      if(line.string):
        news_full_texts += line.string

    return news_full_texts

# COMMAND ----------

class IndependentUK():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="sc-jbiisr-0 ebhxqi sc-jbiisr-2 loNmgs", id="main")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts.replace('Our mission is to deliver unbiased, fact-based reporting that holds power to account and exposes the truth.Whether $5 or $50, every contribution counts.Support us to deliver journalism without an agenda.Louise ThomasEditor','')


# COMMAND ----------

class BostonGlobe():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("article", id="article-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class CNN():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article__content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class Gazette():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", id="article-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class MenaFn():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="entry-summary entry-summary3")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class TimesOfIndia():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="_s30J clearfix")[0]

    text = news_text.get_text()
    if(text):
      news_full_texts += text

    return news_full_texts

# COMMAND ----------

class SandiegoUnion():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class BreitBart():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="entry-content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class AJC():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("article")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class HNGN():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article-text")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class TownHall():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("section", class_="post-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class PostnCourier():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find("div", {"id":"article-body"})
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class PostnCourier():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find("div", {"id":"article-body"})
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class SCMP():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("section")[0]
    for line in news_text.find_all("div"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class NbcChicago():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')
    
  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article-content--wrap")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class FirstPost():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="art-content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class WSWS():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find("article", {"id":"article"})
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class CenterSquare():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find("div", {"id":"article-body"})
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class TheBlaze():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="body-description")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class TheBlaze():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="body-description")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class Heritage():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article__body-copy")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class NewsDay():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="contentAccess")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class ZeroHedge():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="NodeContent_body__HBEFs NodeBody_container__eeFKv")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class MirageNews():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="entry-content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class EditionCNN():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article__content-container")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class Columbian():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("section", class_="article-content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class ClevelandJewish():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find("div", {"id":"article-body"})
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class ChicagoSuntimes():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="Page-articleBody")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class BusinessInsider():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="content-lock-content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class PRNewsWire():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("section", class_="release-body container")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class RedState():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("section", class_="post-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class MassLive():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="entry-content")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class NYPost():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    if(self.news_content.title):
      return self.news_content.title.string
    return None

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="single__content entry-content m-bottom")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class MirrorCo():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="article-body")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text

    return news_full_texts

# COMMAND ----------

class Inquirer():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find("div", {"id":"article-body"})
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

class AlJazeera():
  def load_news(self, news_link):
    response = requests.get(news_link)
    self.news_content = BeautifulSoup(response.content, 'html.parser')

  def get_title(self):
    return self.news_content.title.string

  def get_text(self):
    news_full_texts = ""
    news_text = self.news_content.find_all("div", class_="wysiwyg")[0]
    for line in news_text.find_all("p"):
      text = line.get_text()
      if(text):
        news_full_texts += text.replace('	','').strip()

    return news_full_texts

# COMMAND ----------

# MAGIC %md
# MAGIC # Scrapper Factory

# COMMAND ----------

def get_scrap_class(site):
  match site:
    case 'news.yahoo.com':
        return YahooNews()
    case 'www.yahoo.com':
        return Yahoo()
    case 'www.dailymail.co.uk':
        return DailyMail()
    case 'www.theepochtimes.com':
        return TheEpochTimes()
    case 'nypost.com':
        return NYPost()
    case 'www.foxnews.com':
        return Foxnews()
    case 'www.washingtonpost.com':
        return WashingtonPost()
    case 'www.cbsnews.com':
        return CBSNews()
    case 'dailycaller.com':
        return DailyCaller()
    case 'www.aol.com':
        return AOL()
    case 'www.nbcnews.com':
        return NBCNews()
    case 'www.globalsecurity.org':
        return GlobalSecurity()
    case 'www.arkansasonline.com':
        return ArkansasOnline()
    case 'www.independent.co.uk':
        return IndependentUK()
    case 'www.bostonglobe.com':
        return BostonGlobe()
    case 'www.cnn.com':
        return CNN()
    case 'gazette.com':
        return Gazette()
    case 'menafn.com':
        return MenaFn()
    case 'timesofindia.indiatimes.com':
        return TimesOfIndia()
    case 'www.forbes.com':
        return Forbes()
    case 'www.jpost.com':
        return JPost()
    case 'abcnews.go.com':
        return ABCNews()
    case 'www.chicagotribune.com':
        return ChicagoTribune()
    case 'www.nydailynews.com':
        return NYDailyNews
    case 'www.startribune.com':
        return StarTribune()
    case 'www.breitbart.com:433':
        return BreitBart()
    case 'www.breitbart.com':
        return BreitBart()
    case 'www.sandiegouniontribune.com':
        return SandiegoUnion()
    case 'www.ajc.com':
        return AJC()
    case 'www.hngn.com':
        return HNGN()
    case 'townhall.com':
        return TownHall()
    case 'www.postandcourier.com':
        return PostnCourier()
    case 'www.mirror.co.uk':
        return MirrorCo()
    case 'www.scmp.com':
        return SCMP()
    case 'www.nbcchicago.com':
        return NbcChicago()
    case 'www.firstpost.com':
        return FirstPost()
    case 'www.wsws.org':
        return WSWS()
    case 'www.thecentersquare.com':
        return CenterSquare()
    case 'www.theblaze.com':
        return TheBlaze()
    case 'www.inquirer.com':
        return Inquirer()
    case 'www.heritage.org':
        return Heritage()
    case 'www.newsday.com':
        return NewsDay()
    case 'www.zerohedge.com':
        return ZeroHedge()
    case 'www.miragenews.com':
        return MirageNews()
    case 'edition.cnn.com':
        return EditionCNN()
    case 'www.columbian.com':
        return Columbian()
    case 'www.clevelandjewishnews.com':
        return ClevelandJewish()
    case 'chicago.suntimes.com':
        return ChicagoSuntimes()
    case 'www.businessinsider.com':
        return BusinessInsider()
    case 'www.prnewswire.com':
        return PRNewsWire()
    case 'redstate.com':
        return RedState()
    case 'www.masslive.com':
        return MassLive()
    case 'nystateofpolitics.com':
        return NYStateOfPolitics()
    case 'www.aljazeera.com':
        return AlJazeera()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Global Events News View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_global_events_news
# MAGIC (GlobalEventID, site_base,sourceurl) AS (
# MAGIC WITH cte_news AS (
# MAGIC SELECT DISTINCT 1 as GlobalEventID
# MAGIC               , SPLIT(SPLIT(dm.sourceurl,'://')[1],'/')[0] as site_base
# MAGIC               , dm.sourceurl
# MAGIC   FROM gold.gdelt_events_data_management dm 
# MAGIC   JOIN gold.gdelt_events_actors at on dm.GlobalEventID = at.GlobalEventID
# MAGIC   LEFT JOIN gold.gdelt_events_news_detailed nd on dm.sourceurl = nd.sourcebaseurl 
# MAGIC   WHERE (at.Actor1CountryCode in ('USA','US') or at.Actor2CountryCode in ('USA','US')) 
# MAGIC     AND nd.sourcebaseurl IS NULL
# MAGIC )
# MAGIC SELECT GlobalEventID
# MAGIC      , site_base
# MAGIC      , sourceurl
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
# MAGIC   )
# MAGIC )

# COMMAND ----------

df_news = (
    table('vw_global_events_news')
    .select(
      col('GlobalEventID')
     ,col('site_base')
     ,col('sourceurl')
    )
) 

# COMMAND ----------

news_queue = Queue()

for news in df_news.collect():
    news_queue.put(news)

print(news_queue.qsize())

# COMMAND ----------

news_schema = [
    "GlobalEventID", 
    "SOURCEBASEURL", 
    "SOURCEURL", 
    "TITLE", 
    "NEWS_BODY", 
    "SUCCESSFUL"
]

def get_news():

    while not news_queue.empty():
        news = news_queue.get()
        news_title = ''
        news_text = ''
        try:
            scrapper = get_scrap_class(news.site_base)
            scrapper.load_news(news.sourceurl)
            news_title = scrapper.get_title()
            news_text = scrapper.get_text()

            df = spark.createDataFrame(
                [
                    (
                        news.GlobalEventID, 
                        news.site_base,
                        news.sourceurl,
                        str(news_title),
                        str(news_text),
                        True
                    )
                ],
                schema = news_schema
            )

            df.write.insertInto("gold.gdelt_events_news_detailed")
        except Exception as ex:
            try:
                df_error = spark.createDataFrame(
                    [
                        Row(
                            news.GlobalEventID, 
                            news.site_base,
                            news.sourceurl,
                            str(news_title),
                            str(news_text),
                            False
                        )
                    ],
                    schema = news_schema
                )
                df_error.write.insertInto("gold.gdelt_events_news_detailed")
            except Exception as ex:
                print(ex)

# COMMAND ----------

threads_list = []
max_concurrent_calls = 50

for i in range(0,max_concurrent_calls):
    thread = Thread(target=lambda: get_news())
    threads_list.append(thread)
    time.sleep(1)
    thread.start()

for thread in threads_list:
    thread.join()

print('-------------------------------')
print('queue size:', news_queue.qsize())
print('-------------------------------')

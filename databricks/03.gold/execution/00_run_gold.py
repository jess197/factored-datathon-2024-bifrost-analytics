# Databricks notebook source
def run_notebook(notebook_name):
    dbutils.notebook.run(notebook_name, 0)

# COMMAND ----------

# Lista dos notebooks de modelos de predições que serão executados
notebook_list = ['gdelt_events_actions_gold_exec','gdelt_events_actors_gold_exec','gdelt_events_data_management_gold_exec','gdelt_events_dates_gold_exec','gdelt_events_geography_gold_exec']

# COMMAND ----------

### Execução dos cadernos de cada modelo de predição

from threading import Thread
from queue import Queue
 
q = Queue()
 
worker_count = len(notebook_list) # quantidade de notebooks a serem executados simultaneamente
 
for notebook in notebook_list:
    q.put(notebook)
 
def run_tasks(function, q):
    while not q.empty():
        value = q.get()
        run_notebook(value)
        q.task_done()
        
for i in range(worker_count):
    t=Thread(target=run_tasks, args=(run_notebook, q))
    t.daemon = True
    t.start()
 
q.join()

# Databricks notebook source
#define widgets -- NEEDS TO DEFINE IT ONCE
#dbutils.widgets.text("environment", "","")
#dbutils.widgets.text("system_name", "","")

#dynamic variables (pass it from ADF)
environment = dbutils.widgets.get("environment")
system_name = dbutils.widgets.get("system_name")

# COMMAND ----------

# MAGIC %run ../../bi_config/pbi_common

# COMMAND ----------

import os

notebook = os.path.basename(getNotebookPath())

try:
  #process_curated(environment,system_name)        # Generate all files for given system name
  #process_curated("dev","d365")                  # Generate all files for given system name
  process_curated(environment,system_name,"p_automl_fact_revenue_forecast")  # Generate a file for given system name and table
  
except Exception as error: 
  print(error)
  log_error("{} {}".format(notebook, error)) #log error in sentry
  #raise dbutils.notebook.exit(error) #raise the exception
  raise error #raise the exception

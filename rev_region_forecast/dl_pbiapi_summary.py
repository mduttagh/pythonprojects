# Databricks notebook source
#define widgets - NEED TO DEFINE IT ONCE
# dbutils.widgets.text("environment", "","")
# dbutils.widgets.text("system_name", "","")
# dbutils.widgets.text("data_load_type", "","")

# To remove unnecessary dbutils
# dbutils.widgets.removeAll()

#dynamic variables (pass it from ADF)
environment = dbutils.widgets.get("environment")
system_name = dbutils.widgets.get("system_name")
data_load_type = dbutils.widgets.get("data_load_type") # Full/Incremental


# COMMAND ----------

# DBTITLE 1,Common library
# MAGIC %run ../../bi_config/pbi_common

# COMMAND ----------

# DBTITLE 1,Download raw data
# ============================ get raw data for opportunity entity======================= #
import os
from datetime import date
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

print("Downloading raw data")

#static variables
g_bi_config_parameters_path = "/mnt/"+ environment + "/gold/g_bi_config_parameters"
full_load_date = datetime(1970, 3, 1)
okta_end_poiont = "auth/getapitoken"
end_point_tag = "summary"
#reading config table
df_bi_configuration  = spark.read.format("delta").load(g_bi_config_parameters_path)
df_d365_bi_configuration = df_bi_configuration.filter(df_bi_configuration.SystemName == "bimodelapi")
df_bi_configuration  = df_bi_configuration.filter(df_bi_configuration.SystemName == "bimodelapi")  

#initializing config parameter values
service_account_name =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "service_account_name")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

bronze_folder_path =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "bronze_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

silver_folder_path =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "silver_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

gold_folder_path =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "gold_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

source_path = df_bi_configuration.filter(df_bi_configuration.ParameterName == "source_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

conf_threshold_value =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "threshold")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

powerbi_username =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "powerbi_username")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

ocp_apim_subscription_key =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "ocp_apim_subscription_key")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

curated_folder_path   =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "curated_folder_path")\
                                                     .select("ParameterValue")\
                                                     .collect()[0][0]
#initialize storage account 
storage_name = df_bi_configuration.filter(df_bi_configuration.ParameterName == "storage_name")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]
if environment == "prod":
  pbi_api_password = dbutils.secrets.get(scope = "kv-bi-prod-01-secrets", key = "pbi-biuser-ideocom-key")
  storage_key  = dbutils.secrets.get(scope="kv-bi-prod-01-secrets", key="databricks-{}-storage-key".format(environment))
else:
  pbi_api_password = dbutils.secrets.get(scope = "kv-bi-devqa-01-secrets", key = "pbi-biuser-ideocom-key")
  storage_key  = dbutils.secrets.get(scope="kv-bi-devqa-01-secrets", key="databricks-{}-storage-key".format(environment))
  
spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storage_name),storage_key)

source_system_id = system_name + "/" + end_point_tag

#setting delta tables path
g_automljobruninfo_path = gold_folder_path + "/automljobruninfo"  # Story # 3181
  
#reading job run info
df_job_info = spark.read.format("delta").load(g_automljobruninfo_path) # Story # 3181

#initialize batch id and batch start date time variables
batch_id = df_job_info.agg({"batch_id" : "max"}).collect()[0][0]
batch_start_datetime = df_job_info.agg({"batch_start_datetime" : "max"}).collect()[0][0]
last_updated_datetime = df_job_info.agg({"last_updated_datetime" : "max"}).collect()[0][0]

notebook = os.path.basename(getNotebookPath())

try:   
  #api call for okta authorization
  subscription_key = ""
  query_params = {"username": powerbi_username ,"password": pbi_api_password}
  subscription_key = pbi_post('{}'.format(okta_end_poiont),ocp_apim_subscription_key,query_params)
  current_date = date.today().strftime("(%Y,%m,%d)")
  from_date = "(2017,01,01)"
  #to_date = "(9999,12,31)"
  opp_from_date = "(2018,07,01)"
  #cur_month_eod = date.today()
  #cur_month_eod = date(cur_month_eod.year + (cur_month_eod.month == 12),(cur_month_eod.month + 1 if cur_month_eod.month < 12 else 1), 1) - timedelta(1)
  #cur_month_eod = cur_month_eod.strftime("(%Y,%m,%d)")
  eom_date = date.today() + relativedelta(months=+11)
  eom_date = date(eom_date.year + (eom_date.month == 12),(eom_date.month + 1 if eom_date.month < 12 else 1), 1) - timedelta(1)
  eom_date = eom_date.strftime("(%Y,%m,%d)")
  #Call summary/gl
  entity_name =  "gl"
  entity_responses = []
  if data_load_type == 'Full':
    # ts 2021-06-02 : changed todate filter from "(9999,12,31)" to current date + 11 months 
    query_params = {"DrillDownOptions" : "Managing Sub Region","PostingLayerNames" : "Current, Custom layer 1", "ExcludeSubTypeCodes" : "2020, 2050, 2055, 2060",\
                    "FromDate": from_date, "ToDate" : eom_date, "ReportCurrencyCode" : "USD"}
  else:
    query_params = {"DrillDownOptions" : "Managing Sub Region","PostingLayerNames" : "Current, Custom layer 1", "ExcludeSubTypeCodes" : "2020, 2050, 2055, 2060", \
                    "FromDate": from_date, "ToDate" : eom_date,  "ReportCurrencyCode" : "USD"}

  responses = pbi_post('{}/{}'.format(end_point_tag,entity_name), ocp_apim_subscription_key,query_params,subscription_key)
  for res in responses:
    if res.status_code not in [200, 201, 204]:
      raise Exception(res.json().get('error'))
    
    entity_responses.append(res.json())  
  
  #storing data into json file
  gl_filepath = write_to_json(get_filepath(source_path, source_system_id, entity_name, batch_id), entity_responses)
  
  # Call summary/pipeline
  entity_name = "pipeline"
  entity_responses = []
  if data_load_type == 'Full':
    query_params = {"DrillDownOptions" : "Contrib Sub Region, Relative Snapshot Month Offset, Relative Month Offset, Pipeline Type",\
                    "FromDate": from_date, "ToDate" : eom_date, "FromSnapshotDate":from_date,  "ToSnapshotDate" : current_date}
  else:
    query_params = {"DrillDownOptions" : "Contrib Sub Region, Relative Snapshot Month Offset, Relative Month Offset, Pipeline Type", \
                    "FromDate": from_date, "ToDate" : eom_date, "FromSnapshotDate":from_date,  "ToSnapshotDate" : current_date}

  responses = pbi_post('{}/{}'.format(end_point_tag,entity_name), ocp_apim_subscription_key, query_params,subscription_key)
  for res in responses:
    if res.status_code not in [200, 201, 204]:
      raise Exception(res.json().get('error'))
    
    entity_responses.append(res.json())
      
  #storing data into json file
  pipeline_filepath = write_to_json(get_filepath(source_path, source_system_id, entity_name, batch_id), entity_responses)

  #Call summary/opportunities 1
  entity_name = "opportunities"
  entity_responses = []
  if data_load_type == 'Full':
    # ts 2021-06-02 : changed todate filter from "(9999,12,31)" to current date + 11 months 
    query_params = {"DrillDownOptions" : "Managing Sub Region", "OpportunityStatus" : "Open","FromDate": opp_from_date, "ToDate" : eom_date}
  else:
    # ts 2021-06-02 : changed todate filter from "(9999,12,31)" to current date + 11 months 
    query_params = {"DrillDownOptions" : "Managing Sub Region", "OpportunityStatus" : "Open","FromDate": opp_from_date, "ToDate" : eom_date}

  responses = pbi_post('{}/{}'.format(end_point_tag,entity_name),ocp_apim_subscription_key, query_params,subscription_key)
  for res in responses:
    if res.status_code not in [200, 201, 204]:
      raise Exception(res.json().get('error'))
    
    entity_responses.append(res.json())

  
  #storing data into json file
  opportunities_filepath = write_to_json(get_filepath(source_path, source_system_id, entity_name, batch_id), entity_responses)
  print(opportunities_filepath)
  
  # Call summary/projects
  entity_name = "projects"
  entity_responses = []
  if data_load_type == 'Full':
    # ts 2021-06-02 : changed todate filter from "(9999,12,31)" to current date + 11 months 
    query_params = {"DrillDownOptions" : "Managing Sub Region", "ProjectStatus" : "In Planning, Pending, Okay to Start, Active, Delivered","FromDate": from_date, "ToDate" : eom_date}
  else:
    # ts 2021-06-02 : changed todate filter from "(9999,12,31)" to current date + 11 months
    query_params = {"DrillDownOptions" : "Managing Sub Region", "ProjectStatus" : "In Planning, Pending, Okay to Start, Active, Delivered","FromDate": from_date, "ToDate" : eom_date}
  
  responses = pbi_post('{}/{}'.format(end_point_tag,entity_name), ocp_apim_subscription_key, query_params,subscription_key)
  for res in responses:
    if res.status_code not in [200, 201, 204]:
      raise Exception(res.json().get('error'))
    
    entity_responses.append(res.json())
    
  #storing data into json file
  projects_filepath = write_to_json(get_filepath(source_path, source_system_id, entity_name, batch_id), entity_responses)
  
  #Call summary/headcount
  entity_name = "headcount"
  entity_responses = []
  if data_load_type == 'Full':
    # ts 2021-06-02 : changed todate filter from "(9999,12,31)" to current date + 11 months
    query_params = {"DrillDownOptions" : "Contrib Sub Region, Journey Level, Billable", "FromDate": from_date, "ToDate" : eom_date}
  else:
    # ts 2021-06-02 : changed todate filter from "(9999,12,31)" to current date + 11 months
    query_params = {"DrillDownOptions" : "Contrib Sub Region, Journey Level, Billable", "FromDate": from_date, "ToDate" : eom_date}

  responses = pbi_post('{}/{}'.format(end_point_tag,entity_name), ocp_apim_subscription_key, query_params,subscription_key)
  for res in responses:
    if res.status_code not in [200, 201, 204]:
      raise Exception(res.json().get('error'))
    
    entity_responses.append(res.json())
    
  #storing data into json file
  headcount_filepath = write_to_json(get_filepath(source_path, source_system_id, entity_name, batch_id), entity_responses)
  
  # Call summary/project allocations
  entity_name = "project allocations"
  entity_responses = []
  if data_load_type == 'Full':
    # ts 2021-06-02 : changed todate filter from current month eod to current date + 11 months
    query_params = {"DrillDownOptions" : "Contrib Sub Region, Journey Level", "FromDate": from_date, "ToDate" : eom_date}
  else:
    # ts 2021-06-02 : changed todate filter from current month eod to current date + 11 months
    query_params = {"DrillDownOptions" : "Contrib Sub Region, Journey Level", "FromDate": from_date, "ToDate" : eom_date}

  responses = pbi_post('{}/{}'.format(end_point_tag,entity_name), ocp_apim_subscription_key, query_params,subscription_key)
  for res in responses:
    if res.status_code not in [200, 201, 204]:
      raise Exception(res.json().get('error'))
    
    entity_responses.append(res.json())
    
  #storing data into json file
  project_allocations_filepath = write_to_json(get_filepath(source_path, source_system_id, entity_name, batch_id), entity_responses)
  
  #Call summary/pipeline trend
  entity_name = "pipeline trend"
  entity_responses = []
  if data_load_type == 'Full':
    query_params = {"DrillDownOptions" : "Contrib Sub Region", "FromSnapshotDate":from_date, "ToSnapshotDate" : current_date}
  else:
    query_params = {"DrillDownOptions" : "Contrib Sub Region", "FromSnapshotDate":from_date, "ToSnapshotDate" : current_date}

  responses = pbi_post('{}/{}'.format(end_point_tag,entity_name),ocp_apim_subscription_key,query_params,subscription_key)
  for res in responses:
    if res.status_code not in [200, 201, 204]:
      raise Exception(res.json().get('error'))
      
    entity_responses.append(res.json())
    
  #storing data into json file
  pipeline_trend_filepath = write_to_json(get_filepath(source_path, source_system_id, entity_name, batch_id), entity_responses)

except Exception as error:
  print(error)
  log_error("{} {}".format(notebook, error)) #log error in sentry
  #raise dbutils.notebook.exit(error) #raise the exception
  raise error #raise the exception

# COMMAND ----------

# DBTITLE 1,Converting to pandas dataframe
from pyspark.sql.functions import explode, col,when,udf,split,regexp_replace,lit
from pyspark.sql.types import ArrayType, IntegerType,StringType,TimestampType,DecimalType,DateType
import pandas as pd
#initializing dataframe from API data 
print("Converting to pandas...")

try:
  
  #Revenue
  #gl_filepath = "/mnt/dev/raw/enterprise/bimodelapi/summary/gl/2021/05/05/gl_20210505131204.json"
  df_gl = spark.read.option("multiLine","true").json(gl_filepath)

  #set varivable for Audit Control 
  gl_min_records = df_gl.agg({"TotalRows" : "min"}).collect()[0][0]
  gl_max_records = df_gl.agg({"TotalRows" : "max"}).collect()[0][0]

  df_gl = df_gl.withColumn("data",explode(col('data'))).select("data.*")
  df_gl = df_gl.select("Date[End of Month]", "Managing Studio Location[Managing Sub Region Code]", "[Revenue]")

  df_gl = df_gl.withColumnRenamed("Date[End of Month]","End_of_Month")\
               .withColumnRenamed("Managing Studio Location[Managing Sub Region Code]","Managing Sub Region Code")\
               .withColumnRenamed("[Revenue]","Revenue")

  df_gl = df_gl.withColumn("End_of_Month",  df_gl["End_of_Month"].cast(DateType()))
  df_gl = df_gl.withColumn("Managing Sub Region Code",  df_gl["Managing Sub Region Code"].cast(StringType()))
  df_gl = df_gl.withColumn("Revenue",regexp_replace("Revenue", ",", ''))
  df_gl = df_gl.withColumn("Revenue",  df_gl["Revenue"].cast(DecimalType(32,6)))

  #set varivable for Audit Control 
  gl_record_count = df_gl.agg({"*" : "count"}).collect()[0][0]
  #Audit Control 
  check_audit_contorls("gl",gl_record_count,gl_min_records,gl_max_records)

  #Opp
  #opportunities_filepath = "/mnt/dev/raw/enterprise/bimodelapi/summary/opportunities/2021/05/05/opportunities_20210505131204.json"
  df_opportunities = spark.read.option("multiLine","true").json(opportunities_filepath)
  #set varivable for Audit Control 
  opp_min_records = df_opportunities.agg({"TotalRows" : "min"}).collect()[0][0]
  opp_max_records = df_opportunities.agg({"TotalRows" : "max"}).collect()[0][0]

  df_opportunities = df_opportunities.withColumn("data",explode(col('data'))).select("data.*")
  df_opportunities = df_opportunities.select("Date[End of Month]", "Managing Studio Location[Managing Sub Region Code]", \
                                             "[Opportunity_Period_Count]","[Opportunity_Count]","[Current_Opp_Period_Value]", \
                                             "[Opportunity_Value]", "[Win_Rate]","[Current_Opp_Period_Count]","[Opportunity_Period_Value]")

  df_opportunities = df_opportunities.withColumnRenamed("Date[End of Month]","End_of_Month")\
                                     .withColumnRenamed("Managing Studio Location[Managing Sub Region Code]","Managing Sub Region Code")\
                                     .withColumnRenamed("[Opportunity_Period_Count]","Opportunity Period Count")\
                                     .withColumnRenamed("[Opportunity_Count]","Opportunity Count")\
                                     .withColumnRenamed("[Current_Opp_Period_Value]","Current Opp Period Value")\
                                     .withColumnRenamed("[Opportunity_Value]","Opportunity Value")\
                                     .withColumnRenamed("[Win_Rate]","Win Rate")\
                                     .withColumnRenamed("[Current_Opp_Period_Count]","Current Opp Period Count")\
                                     .withColumnRenamed("[Opportunity_Period_Value]","Opportunity Period Value")
  
  df_opportunities = df_opportunities.withColumn("End_of_Month",  df_opportunities["End_of_Month"].cast(DateType()))
  df_opportunities = df_opportunities.withColumn("Managing Sub Region Code",  df_opportunities["Managing Sub Region Code"].cast(StringType()))
  df_opportunities = df_opportunities.withColumn("Opportunity Period Count",regexp_replace("Opportunity Period Count", ",", ''))
  df_opportunities = df_opportunities.withColumn("Opportunity Period Count",  df_opportunities["Opportunity Period Count"].cast(IntegerType()))
  df_opportunities = df_opportunities.withColumn("Opportunity Count",regexp_replace("Opportunity Count", ",", ''))
  df_opportunities = df_opportunities.withColumn("Opportunity Count",  df_opportunities["Opportunity Count"].cast(IntegerType()))
  df_opportunities = df_opportunities.withColumn("Current Opp Period Value",regexp_replace("Current Opp Period Value", ",", ''))
  df_opportunities = df_opportunities.withColumn("Current Opp Period Value",  df_opportunities["Current Opp Period Value"].cast(DecimalType(32,6)))
  df_opportunities = df_opportunities.withColumn("Opportunity Value",regexp_replace("Opportunity Value", ",", ''))
  df_opportunities = df_opportunities.withColumn("Opportunity Value",  df_opportunities["Opportunity Value"].cast(DecimalType(32,6)))
  df_opportunities = df_opportunities.withColumn("Current Opp Period Count",regexp_replace("Current Opp Period Count", ",", ''))
  df_opportunities = df_opportunities.withColumn("Current Opp Period Count",  df_opportunities["Current Opp Period Count"].cast(IntegerType()))
  df_opportunities = df_opportunities.withColumn("Opportunity Period Value",regexp_replace("Opportunity Period Value", ",", ''))
  df_opportunities = df_opportunities.withColumn("Opportunity Period Value",  df_opportunities["Opportunity Period Value"].cast(DecimalType(32,6)))
  df_opportunities = df_opportunities.withColumn("Win Rate",regexp_replace("Win Rate", "%", ''))
  df_opportunities = df_opportunities.withColumn("Win Rate",  df_opportunities["Win Rate"].cast(DecimalType(8,4)) * lit(0.01))

  #set varivable for Audit Control 
  opp_record_count = df_opportunities.agg({"*" : "count"}).collect()[0][0]
  #Audit Control 
  check_audit_contorls("opportunities",opp_record_count,opp_min_records,opp_max_records)


  #project
  #projects_filepath = "/mnt/dev/raw/enterprise/bimodelapi/summary/projects/2021/05/05/projects_20210505131204.json"
  df_projects = spark.read.option("multiLine","true").json(projects_filepath)
  #set varivable for Audit Control 
  projects_min_records = df_projects.agg({"TotalRows" : "min"}).collect()[0][0]
  projects_max_records = df_projects.agg({"TotalRows" : "max"}).collect()[0][0]
  
  df_projects = df_projects.withColumn("data",explode(col('data'))).select("data.*")
  df_projects = df_projects.select("Date[End of Month]", "Managing Studio Location[Managing Sub Region Code]", \
                                             "[Project Period Count]","[Project Count]","[Project Period Price]", \
                                             "[Project Price]", "[Conversions]")

  df_projects = df_projects.withColumnRenamed("Date[End of Month]","End_of_Month")\
                           .withColumnRenamed("Managing Studio Location[Managing Sub Region Code]","Managing Sub Region Code")\
                           .withColumnRenamed("[Project Period Count]","Project Period Count")\
                           .withColumnRenamed("[Project Count]","Project Count")\
                           .withColumnRenamed("[Project Period Price]","Project Period Price")\
                           .withColumnRenamed("[Project Price]","Project Price")\
                           .withColumnRenamed("[Conversions]","Conversions")

  df_projects = df_projects.withColumn("End_of_Month",  df_projects["End_of_Month"].cast(DateType()))
  df_projects = df_projects.withColumn("Managing Sub Region Code",  df_projects["Managing Sub Region Code"].cast(StringType()))
  df_projects = df_projects.withColumn("Project Period Count",regexp_replace("Project Period Count", ",", ''))
  df_projects = df_projects.withColumn("Project Period Count",  df_projects["Project Period Count"].cast(IntegerType()))
  df_projects = df_projects.withColumn("Project Count",regexp_replace("Project Count", ",", ''))
  df_projects = df_projects.withColumn("Project Count",  df_projects["Project Count"].cast(IntegerType()))
  df_projects = df_projects.withColumn("Project Period Price",regexp_replace("Project Period Price", ",", ''))
  df_projects = df_projects.withColumn("Project Period Price",  df_projects["Project Period Price"].cast(DecimalType(32,6)))
  df_projects = df_projects.withColumn("Project Price",regexp_replace("Project Price", ",", ''))
  df_projects = df_projects.withColumn("Project Price",  df_projects["Project Price"].cast(DecimalType(32,6)))
  df_projects = df_projects.withColumn("Conversions",regexp_replace("Conversions", ",", ''))
  df_projects = df_projects.withColumn("Conversions",  df_projects["Conversions"].cast(DecimalType(32,6)))
  
  #set varivable for Audit Control 
  projects_record_count = df_projects.agg({"*" : "count"}).collect()[0][0]
  #Audit Control 
  check_audit_contorls("projects",projects_record_count,projects_min_records,projects_max_records)

  #Talent
  #headcount_filepath = "/mnt/dev/raw/enterprise/bimodelapi/summary/headcount/2021/05/14/headcount_20210514193743.json"
  df_headcount = spark.read.option("multiLine","true").json(headcount_filepath)
  
  #set varivable for Audit Control 
  headcount_min_records = df_headcount.agg({"TotalRows" : "min"}).collect()[0][0]
  headcount_max_records = df_headcount.agg({"TotalRows" : "max"}).collect()[0][0]
  
  df_headcount = df_headcount.withColumn("data",explode(col('data'))).select("data.*")
  df_headcount = df_headcount.select("Date[End of Month]", "Contributing Studio Location[Contrib Sub Region Code]", \
                                     "Journey Level[Journey Level]","Billable[Billable]","[Headcount Value]","[Headcount Contingent]")

  df_headcount = df_headcount.withColumnRenamed("Date[End of Month]","End_of_Month")\
                           .withColumnRenamed("Contributing Studio Location[Contrib Sub Region Code]","Contrib Sub Region Code")\
                           .withColumnRenamed("Journey Level[Journey Level]","Journey Level")\
                           .withColumnRenamed("Billable[Billable]","Billable")\
                           .withColumnRenamed("[Headcount Value]","Headcount")\
                           .withColumnRenamed("[Headcount Contingent]","Headcount Contingent")

  df_headcount = df_headcount.withColumn("End_of_Month",  df_headcount["End_of_Month"].cast(DateType()))
  df_headcount = df_headcount.withColumn("Contrib Sub Region Code",  df_headcount["Contrib Sub Region Code"].cast(StringType()))
  df_headcount = df_headcount.withColumn("Journey Level",  df_headcount["Journey Level"].cast(StringType()))
  df_headcount = df_headcount.withColumn("Billable",  df_headcount["Billable"].cast(StringType()))
  df_headcount = df_headcount.withColumn("Headcount",regexp_replace("Headcount", ",", ''))
  df_headcount = df_headcount.withColumn("Headcount",  df_headcount["Headcount"].cast(DecimalType(32,6)))
  df_headcount = df_headcount.withColumn("Headcount Contingent",regexp_replace("Headcount Contingent", ",", ''))
  df_headcount = df_headcount.withColumn("Headcount Contingent",  df_headcount["Headcount Contingent"].cast(DecimalType(32,6)))
  
  #set varivable for Audit Control 
  headcount_record_count = df_headcount.agg({"*" : "count"}).collect()[0][0]
  #Audit Control 
  check_audit_contorls("headcount",headcount_record_count,headcount_min_records,headcount_max_records)
  
  #project actual
  #project_allocations_filepath = "/mnt/dev/raw/enterprise/bimodelapi/summary/project allocations/2021/05/05/project allocations_20210505131204.json"
  df_project_allocations = spark.read.option("multiLine","true").json(project_allocations_filepath)
  #set varivable for Audit Control 
  project_allocations_min_records = df_project_allocations.agg({"TotalRows" : "min"}).collect()[0][0]
  project_allocations_max_records = df_project_allocations.agg({"TotalRows" : "max"}).collect()[0][0]

  df_project_allocations = df_project_allocations.withColumn("data",explode(col('data'))).select("data.*")
  df_project_allocations= df_project_allocations.select("Date[End of Month]", "Contributing Studio Location[Contrib Sub Region Code]",\
                                     "Journey Level[Journey Level]","[Utilization Billable]","[Nominal Hours]")

  df_project_allocations= df_project_allocations.withColumnRenamed("Date[End of Month]","End_of_Month")\
                           .withColumnRenamed("Contributing Studio Location[Contrib Sub Region Code]","Contrib Sub Region Code")\
                           .withColumnRenamed("Journey Level[Journey Level]","Journey Level")\
                           .withColumnRenamed("[Utilization Billable]","Utilization Billable")\
                           .withColumnRenamed("[Nominal Hours]","Nominal_Hours")

  df_project_allocations= df_project_allocations.withColumn("End_of_Month",  df_project_allocations["End_of_Month"].cast(DateType()))
  df_project_allocations= df_project_allocations.withColumn("Contrib Sub Region Code",  df_project_allocations["Contrib Sub Region Code"].cast(StringType()))
  df_project_allocations= df_project_allocations.withColumn("Journey Level",  df_project_allocations["Journey Level"].cast(StringType()))
  df_project_allocations = df_project_allocations.withColumn("Utilization Billable",regexp_replace("Utilization Billable", "%", ''))
  df_project_allocations = df_project_allocations.withColumn("Utilization Billable",  df_project_allocations["Utilization Billable"].cast(DecimalType(8,4)) * lit(0.01))
  df_project_allocations = df_project_allocations.withColumn("Nominal_Hours",regexp_replace("Nominal_Hours", ",", ''))
  df_project_allocations = df_project_allocations.withColumn("Nominal_Hours", df_project_allocations["Nominal_Hours"].cast(DecimalType(32,6)))


  #set varivable for Audit Control 
  project_allocations_record_count = df_project_allocations.agg({"*" : "count"}).collect()[0][0]
  #Audit Control 
  check_audit_contorls("project_allocations",project_allocations_record_count,project_allocations_min_records,project_allocations_max_records)
  
  #pipeine
  #pipeline_filepath = "/mnt/dev/raw/enterprise/bimodelapi/summary/pipeline/2021/05/05/pipeline_20210505131204.json"
  #pipeline_filepath =  "/mnt/dev/raw/enterprise/bimodelapi/summary/pipeline/2021/04/23/pipeline_20210423090034.json"   # Null pipeline type 
  df_pipeline = spark.read.option("multiLine","true").json(pipeline_filepath)
  #set varivable for Audit Control 
  pipeline_min_records = df_pipeline.agg({"TotalRows" : "min"}).collect()[0][0]
  pipeline_max_records = df_pipeline.agg({"TotalRows" : "max"}).collect()[0][0]
  
  df_pipeline = df_pipeline.withColumn("data",explode(col('data'))).select("data.*")

  df_pipeline = df_pipeline.select("Snapshot Date[Snapshot Date]", "Snapshot Date[Relative Snapshot Month Offset]", "Date[End of Month]", "Date[Relative Month Offset]",\
                                   "Contributing Studio Location[Contrib Sub Region Code]", "Pipeline Type[Pipeline Type]", "[Pipeline Value]","[Pipeline at 100 Percent]","[Yield]")

  df_pipeline = df_pipeline.withColumnRenamed("Snapshot Date[Snapshot Date]","Snapshot_Date")\
                           .withColumnRenamed("Snapshot Date[Relative Snapshot Month Offset]","Relative Snapshot Month Offset")\
                           .withColumnRenamed("Date[End of Month]","End_of_Month")\
                           .withColumnRenamed("Date[Relative Month Offset]","Relative Month Offset")\
                           .withColumnRenamed("Contributing Studio Location[Contrib Sub Region Code]","Contrib Sub Region Code")\
                           .withColumnRenamed("Pipeline Type[Pipeline Type]","Pipeline Type")\
                           .withColumnRenamed("[Pipeline Value]","Pipeline")\
                           .withColumnRenamed("[Pipeline at 100 Percent]","Pipeline at 100 Percent")\
                           .withColumnRenamed("[Yield]","Yield")

  df_pipeline = df_pipeline.withColumn("Snapshot_Date",  df_pipeline["Snapshot_Date"].cast(DateType()))
  df_pipeline = df_pipeline.withColumn("Relative Snapshot Month Offset",  df_pipeline["Relative Snapshot Month Offset"].cast(IntegerType()))
  df_pipeline = df_pipeline.withColumn("End_of_Month",  df_pipeline["End_of_Month"].cast(DateType()))
  df_pipeline = df_pipeline.withColumn("Relative Month Offset",  df_pipeline["Relative Month Offset"].cast(IntegerType()))
  df_pipeline = df_pipeline.withColumn("Contrib Sub Region Code",  df_pipeline["Contrib Sub Region Code"].cast(StringType()))

  df_pipeline = df_pipeline.withColumn("Pipeline",regexp_replace("Pipeline", ",", ''))
  df_pipeline = df_pipeline.withColumn("Pipeline",  df_pipeline["Pipeline"].cast(DecimalType(32,6)))

  df_pipeline = df_pipeline.withColumn("Pipeline at 100 Percent",regexp_replace("Pipeline at 100 Percent", ",", ''))
  df_pipeline = df_pipeline.withColumn("Pipeline at 100 Percent",  df_pipeline["Pipeline at 100 Percent"].cast(DecimalType(32,6)))

  df_pipeline = df_pipeline.withColumn("Yield",regexp_replace("Yield", "%", ''))
  df_pipeline = df_pipeline.withColumn("Yield",  df_pipeline["Yield"].cast(DecimalType(8,4)) * lit(0.01))
  
  #set varivable for Audit Control 
  pipeline_record_count = df_pipeline.agg({"*" : "count"}).collect()[0][0]
  pipeline_type_null_count = df_pipeline.filter(col("Pipeline Type").isNull()).agg({"Snapshot_Date" : "count"}).collect()[0][0]
  #Audit Control 
  check_audit_contorls("pipeline",pipeline_record_count,pipeline_min_records,pipeline_max_records,pipeline_type_null_count)

  #pipeine trend
  #pipeline_trend_filepath = "/mnt/dev/raw/enterprise/bimodelapi/summary/pipeline trend/2021/05/05/pipeline trend_20210505131204.json"
  df_pipeline_trend = spark.read.option("multiLine","true").json(pipeline_trend_filepath)
  #set varivable for Audit Control 
  pipeline_trend_min_records = df_pipeline_trend.agg({"TotalRows" : "min"}).collect()[0][0]
  pipeline_trend_max_records = df_pipeline_trend.agg({"TotalRows" : "max"}).collect()[0][0]
  
  df_pipeline_trend = df_pipeline_trend.withColumn("data",explode(col('data'))).select("data.*")
  df_pipeline_trend = df_pipeline_trend.select("Snapshot Date[Snapshot Date]", "Snapshot Date[Snapshot End of Month]",\
                                   "Contributing Studio Location[Contrib Sub Region Code]", "[Pipeline Trend Value]")

  df_pipeline_trend = df_pipeline_trend.withColumnRenamed("Snapshot Date[Snapshot Date]","Snapshot_Date")\
                                       .withColumnRenamed("Snapshot Date[Snapshot End of Month]","End_of_Month")\
                                       .withColumnRenamed("Contributing Studio Location[Contrib Sub Region Code]","Contrib Sub Region Code")\
                                       .withColumnRenamed("[Pipeline Trend Value]","Pipeline Trend")

  df_pipeline_trend = df_pipeline_trend.withColumn("Snapshot_Date",  df_pipeline_trend["Snapshot_Date"].cast(DateType()))
  df_pipeline_trend = df_pipeline_trend.withColumn("End_of_Month",  df_pipeline_trend["End_of_Month"].cast(DateType()))
  df_pipeline_trend = df_pipeline_trend.withColumn("Contrib Sub Region Code",  df_pipeline_trend["Contrib Sub Region Code"].cast(StringType()))
  df_pipeline_trend = df_pipeline_trend.withColumn("Pipeline Trend",regexp_replace("Pipeline Trend", ",", ''))
  df_pipeline_trend = df_pipeline_trend.withColumn("Pipeline Trend",  df_pipeline_trend["Pipeline Trend"].cast(DecimalType(32,6)))

  #set varivable for Audit Control 
  pipeline_trend_record_count = df_pipeline_trend.agg({"*" : "count"}).collect()[0][0]
  #Audit Control 
  check_audit_contorls("pipeline_trend",pipeline_trend_record_count,pipeline_trend_min_records,pipeline_trend_max_records)

#   #Convert pyspark dataframe to pandas dataframe.
#   revhist = df_gl.toPandas()
#   opphist = df_opportunities.toPandas()
#   projhist = df_projects.toPandas()
#   talenthist = df_headcount.toPandas()
#   projectactualshist = df_project_allocations.toPandas()
#   pipehist = df_pipeline.toPandas()
#   pipetrend = df_pipeline_trend.toPandas()

  target_file_name = "OppHistory.csv"
  generate_automl_input_files(df_opportunities, environment, storage_name, target_file_name)
  
  target_file_name = "PipelineHistory.csv"
  generate_automl_input_files(df_pipeline, environment, storage_name, target_file_name)
  
  target_file_name = "PipelineTrend.csv"
  generate_automl_input_files(df_pipeline_trend, environment, storage_name, target_file_name)

  target_file_name = "ProjectActualsHistory.csv"
  generate_automl_input_files(df_project_allocations, environment, storage_name, target_file_name)

  target_file_name = "ProjectHistory.csv"
  generate_automl_input_files(df_projects, environment, storage_name, target_file_name)

  target_file_name = "RevenueHistory.csv"
  generate_automl_input_files(df_gl, environment, storage_name, target_file_name)  
  
  target_file_name = "TalentHistory.csv"
  generate_automl_input_files(df_headcount, environment, storage_name, target_file_name)
  
  
except Exception as error:
  print(error)
  log_error("{} {}".format(notebook, error)) #log error in sentry
  #raise dbutils.notebook.exit(error) #raise the exception
  raise error #raise the exception

# COMMAND ----------


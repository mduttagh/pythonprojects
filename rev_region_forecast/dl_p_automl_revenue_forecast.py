# Databricks notebook source
#define widgets - NEED TO DEFINE IT ONCE
#dbutils.widgets.text("environment", "","")
#dbutils.widgets.text("system_name", "","")

#dynamic variables (pass it from ADF)
environment = dbutils.widgets.get("environment")
system_name = dbutils.widgets.get("system_name")

# COMMAND ----------

# MAGIC %run ../../bi_config/pbi_common

# COMMAND ----------

#============================= Global Variable Declaration ===================================#
from pyspark.sql.functions import col,concat,lit,current_date, lag, lead, first, last, desc, hash, date_format,coalesce,broadcast
from datetime import date, timedelta, datetime
from pyspark.sql import *
from delta.tables import *
from pyspark.sql.types import TimestampType, LongType, StringType
import os

notebook = os.path.basename(getNotebookPath())

try:

  #setting default valid to
  g_bi_config_parameters_path = "/mnt/"+ environment + "/gold/g_bi_config_parameters"
  default_valid_to = datetime(9999, 12, 31)
  table_name = "fact_revenue_forecast"

  #lookup tables
  p_currency_table_name = "currency"
  
  #reading config table
  df_bi_configuration  = spark.read.format("delta").load(g_bi_config_parameters_path)
  df_bi_configuration  = df_bi_configuration.filter(df_bi_configuration.SystemName == "bimodelapi")

  #initializing config parameter values
  gold_folder_path      =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "gold_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  platinum_folder_path  =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "platinum_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  #setting delta tables path
  g_table_name_path = gold_folder_path + "/g_automl_{}".format(table_name)
  p_table_name_path = platinum_folder_path + "/p_automl_{}".format(table_name)

  #setting lookup tables path
  p_currency_table_path = platinum_folder_path + "/p_d365_{}".format(p_currency_table_name)
  
  #initialize gold dataframe
  g_df_table_name = spark.read.format("delta").load(g_table_name_path).repartition(160)
  
  #adding datekeys
  g_df_table_name = g_df_table_name.withColumn("ValidFromDateKey",date_format('ValidFrom', 'yyyyMMdd').cast("Integer"))
  g_df_table_name = g_df_table_name.withColumn("ValidToDateKey",date_format(coalesce(g_df_table_name.ValidTo,lit(default_valid_to)),\
                                                                            'yyyyMMdd').cast("Integer"))  
  g_df_table_name = g_df_table_name.withColumn("CreatedDateKey",date_format('IBICreatedDate', 'yyyyMMdd').cast("Integer"))
  g_df_table_name = g_df_table_name.withColumn("UpdatedDateKey",date_format('IBIUpdatedDate', 'yyyyMMdd').cast("Integer"))
  g_df_table_name = g_df_table_name.withColumn("BatchStartDateKey",date_format('IBIBatchStartDateTime', 'yyyyMMdd').cast("Integer"))
  #additional datekeys
  g_df_table_name = g_df_table_name.withColumn("ForecastDateKey",date_format('Forecast_Date', 'yyyyMMdd').cast("Integer"))
  g_df_table_name = g_df_table_name.withColumn("SnapshotDateKey",date_format('Snapshot_Date', 'yyyyMMdd').cast("Integer"))
  g_df_table_name = g_df_table_name.withColumn("EndofMonthDateKey",date_format('End_of_Month', 'yyyyMMdd').cast("Integer"))
  g_df_table_name = g_df_table_name.withColumn("RunDateKey",date_format('Run_Date', 'yyyyMMdd').cast("Integer"))
  
  #filtering out valid records for platinum
  g_df_table_name = g_df_table_name \
                  .where(g_df_table_name.ValidFromDateKey <= g_df_table_name.ValidToDateKey)

  #filtering out deleted records for platinum
  g_df_table_name = g_df_table_name \
                  .where(g_df_table_name.IsDeleted == 'false')
  
  
  #initializing lookup tables
  p_df_currency = spark.read.format("delta").load(p_currency_table_path)

  #lookup into currency dimension 
  p_df_currency_lookup = g_df_table_name\
                           .alias("rev")\
                           .join(broadcast(p_df_currency.alias("cu")),\
                                 g_df_table_name.Currency_Code == p_df_currency.CurrencyCode,"inner")\
                           .where('rev.ValidToDateKey BETWEEN cu.ValidFromDateKey AND cu.ValidToDateKey')\
                           .selectExpr("rev.RevenueForecastKey","cu.CurrencyKey")
  
  #adding addtional fields for confirmed dimension lookup
  g_df_table_name = g_df_table_name\
                      .alias("rev")\
                      .join(broadcast(p_df_currency_lookup.alias("cur")),\
                                           col("rev.RevenueForecastKey") == col("cur.RevenueForecastKey"),"left")\
                      .selectExpr("rev.*","coalesce(cur.CurrencyKey,-1) as CurrencyKey")

  
  g_df_table_name = g_df_table_name.na.fill("")

  g_table_name_schema = g_df_table_name.select("RevenueForecastKey","ValidFrom","ValidTo", "ValidFromDateKey","ValidToDateKey", "CreatedDateKey", \
                                               "UpdatedDateKey", "BatchStartDateKey", "ForecastDateKey", "SnapshotDateKey", "EndOfMonthDateKey", "RunDateKey","CurrencyKey",\
                                               "Forecast_Date","Snapshot_Date","End_of_Month","Sub_Region_Code", "Relative_Month_Offset", "Relative_EOM_Snp_Month_Offset", \
                                               "Relative_Snapshot_Month_Offset", "Snapshot_Day_of_Month", "Snp_Seq_No", "Currency_Code","Predicted_Revenue", \
                                               "Revenue","Project_Period_Count","Project_Count","Project_Period_Price","Project_Price","Conversions",\
                                               "Opportunity_Period_Count", "Opportunity_Count","Current_Opp_Period_Value","Opportunity_Value","Win_Rate",\
                                               "Nominal_Hours","Utilization_Billable","Headcount_Billable","Headcount_Non_Billable","Headcount_Contingent_Billable",\
                                               "Headcount_Contingent_Non_Billable","Headcount_Contingent_Unknown","EURONEXT_100","FTSE_100","Nikkei_225","SP_500",\
                                               "SSE_Composite_Index","Pipeline_Trend","Pipeline_Active_Unrecognized","Pipeline_Opportunity","Pipeline_Opportunity_ML",\
                                               "Pipeline_Recognized","Pipeline_at_100_Percent_Active_Unrecognized","Pipeline_at_100_Percent_Opportunity",\
                                               "Pipeline_at_100_Percent_Opportunity_ML","Pipeline_at_100_Percent_Recognized","Yield_Active_Unrecognized",\
                                               "Yield_Opportunity_ML","Yield_Recognized","Run_ID","Run_Date","Explained_Variance","Mean_Absolute_Error",\
                                               "Mean_Absolute_Percentage_Error","Median_Absolute_Error","Normalized_Mean_Absolute_Error","Normalized_Median_Absolute_Error",\
                                               "Normalized_Root_Mean_Squared_Error","Normalized_Root_Mean_Squared_Log_Error", "R2_Score","Root_Mean_Squared_Error",\
                                               "Root_Mean_Squared_Log_Error","Spearman_Correlation","Current_Opp_Period_Count", "Opportunity_Period_Value",\
                                               "IBIBatchID","IBICreatedBy","IBIUpdatedBy","IBICreatedDate",\
                                               "IBIUpdatedDate","IBIChangeReason","IBIOperationType","IBISourceFormat","IBISourceName","IBISourcePath","IBISourceSystemID",\
                                               "IBIDataCategory","IBILineageLevel","IBIIngestionMethod","IBIBatchStartDateTime","IBIDataClassification","IBIProgramName",\
                                               "IBIProgramPath","IBIValidationStatus","IBIHashCode","IsLatest","IsDeleted").schema

  g_df_table_name = g_df_table_name.select(g_table_name_schema.fieldNames())

  #loading modified records into bronze for further processing
  g_df_table_name.coalesce(16).write.format("delta").mode("overwrite").save(p_table_name_path)
  
except Exception as error: 
  log_error("{} {}".format(notebook, error)) #log error in sentry
  #raise dbutils.notebook.exit(error) #raise the exception
  raise error #raise the exception

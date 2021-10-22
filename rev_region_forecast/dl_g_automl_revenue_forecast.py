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
from pyspark.sql.functions import col,concat,lit,current_date, lag, lead, first, last, desc, hash,row_number,udf, trunc
from datetime import date, timedelta, datetime
from pyspark.sql import *
from delta.tables import *
from pyspark.sql.types import TimestampType, LongType, StringType
#from ibi_packages import functions as fn
import os

notebook = os.path.basename(getNotebookPath())

try: 
  #static variables
  g_bi_config_parameters_path = "/mnt/"+ environment + "/gold/g_bi_config_parameters"
  default_valid_from = datetime.strptime('01031901', '%d%m%Y').date()
  table_name = "fact_revenue_forecast"

  #reading config table
  df_bi_configuration  = spark.read.format("delta").load(g_bi_config_parameters_path)
  df_bi_configuration  = df_bi_configuration.filter(df_bi_configuration.SystemName == "bimodelapi")

  #initializing config parameter values
  service_account_name = df_bi_configuration.filter(df_bi_configuration.ParameterName == "service_account_name")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  source_path         =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "source_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  bronze_folder_path  =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "bronze_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  silver_folder_path  =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "silver_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  gold_folder_path    =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "gold_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  source_system_id = system_name

  #setting delta tables path
  g_automljobruninfo_path = gold_folder_path + "/automljobruninfo"  # Story # 3181
  b_table_name_path = bronze_folder_path + "/b_automl_{}".format(table_name)
  s_table_name_path = silver_folder_path + "/s_automl_{}".format(table_name) 
  g_table_name_path = gold_folder_path + "/g_automl_{}".format(table_name) 

  #extracting jobruninfo table
  df_job_info = spark.read.format("delta").load(g_automljobruninfo_path)  # Story # 3181 
  batch_id = df_job_info.agg({"batch_id" : "max"}).collect()[0][0] #for audit fields
  batch_start_datetime = df_job_info.agg({"batch_start_datetime" : "max"}).collect()[0][0] #for audit fields 
  batch_date = batch_start_datetime.date() #as valid from

  #audit fields
  ibi_batch_id = batch_id
  ibi_created_by =  getNotebookUser() #service_account_name
  ibi_updated_by =  getNotebookUser() #service_account_name
  ibi_created_date = getUtcTimeNow() #this should be always UTC for TS 
  ibi_updated_date = getUtcTimeNow() #this should be always UTC for TS 
  ibi_source_format = "delta table" #static
  ibi_source_name = "s_automl_{}".format(table_name) #table name will be dynamically replaced
  ibi_source_path = "delta_silver_{}_01.s_automl_{}".format(environment,table_name)  #table name & environment will be dyamically replaced
  ibi_source_system_id =  source_system_id  #from config table
  ibi_data_category = "Master"  #static
  ibi_lineage_level = 1 #static
  ibi_ingestion_method = "python notebook"  #static
  ibi_data_classification = "Private"  #static
  ibi_program_path = getNotebookPath() 
  ibi_program_name = os.path.basename(ibi_program_path)
  ibi_validation_status = "success" #static

#====================================== Gold Merge ========================================#
 #reading delta tables
  s_df_table_name = spark.read.format("delta").load(s_table_name_path)
  g_df_table_name = spark.read.format("delta").load(g_table_name_path)
  
  #initializing gold table for SCD 2 merge
  g_delta_table_name = DeltaTable.forPath(spark, g_table_name_path)

  s_df_table_name = s_df_table_name \
                  .where(s_df_table_name.IBIBatchID >= ibi_batch_id)

  #find maximum 
  max_table_name_key = g_df_table_name.agg({"RevenueForecastKey" : "max"}).collect()[0][0]

  if max_table_name_key == None: 
     max_table_name_key = 0
    
  #set dataframe for inserted records
  df_inserted_table_name =  s_df_table_name\
                          .alias("silver")\
                          .join(g_df_table_name.alias("gold"),\
                               (s_df_table_name.Forecast_Date==g_df_table_name.Forecast_Date) & \
                               (s_df_table_name.Snapshot_Date==g_df_table_name.Snapshot_Date) & \
                               (s_df_table_name.End_of_Month==g_df_table_name.End_of_Month) & \
                               (s_df_table_name.Sub_Region_Code==g_df_table_name.Sub_Region_Code) &\
                               (s_df_table_name.Snp_Seq_No==g_df_table_name.Snp_Seq_No) ,"left_anti")\
                          .where(s_df_table_name.IBIOperationType != "Delete")\
                          .selectExpr("NULL as MergeKey1","NULL as MergeKey2","NULL as MergeKey3","NULL as MergeKey4", "NULL as MergeKey5",\
                                      "silver.*", "silver.IBIUpdatedDate as s_IBIUpdatedDate", "silver.IBIUpdatedBy as s_IBIUpdatedBy")

  df_inserted_table_name = df_inserted_table_name.withColumn("ValidFrom",trunc("End_of_Month", "month"))
  df_inserted_table_name = df_inserted_table_name.withColumn("ValidTo",df_inserted_table_name.End_of_Month)

  
  df_inserted_table_name = df_inserted_table_name.withColumn("IBIChangeReason",lit("New record has been created in source system."))
  df_inserted_table_name = df_inserted_table_name.withColumn("IBIOperationType",lit("Insert"))
  df_inserted_table_name = df_inserted_table_name.withColumn("IsDeleted",lit("false"))
  df_inserted_table_name = df_inserted_table_name.withColumn("IsLatest",lit("true"))
  
  #set dataframe for modified records 
  df_modified_table_name =  s_df_table_name\
                          .alias("silver")\
                          .join(df_inserted_table_name.alias("inserted"),\
                               (s_df_table_name.Forecast_Date==df_inserted_table_name.Forecast_Date) & \
                               (s_df_table_name.Snapshot_Date==df_inserted_table_name.Snapshot_Date) & \
                               (s_df_table_name.End_of_Month==df_inserted_table_name.End_of_Month) & \
                               (s_df_table_name.Sub_Region_Code==df_inserted_table_name.Sub_Region_Code) & \
                               (s_df_table_name.Snp_Seq_No==df_inserted_table_name.Snp_Seq_No),"left_anti")\
                          .where(s_df_table_name.IBIOperationType != "Delete")\
                          .selectExpr("silver.Forecast_Date as MergeKey1","silver.Snapshot_Date as MergeKey2","silver.End_of_Month as MergeKey3",\
                                      "silver.Sub_Region_Code as MergeKey4", "silver.Snp_Seq_No as MergeKey5", \
                                      "silver.*", "silver.IBIUpdatedDate as s_IBIUpdatedDate", "silver.IBIUpdatedBy as s_IBIUpdatedBy")

  df_modified_table_name = df_modified_table_name.withColumn("ValidFrom", trunc("End_of_Month", "month"))
  df_modified_table_name = df_modified_table_name.withColumn("ValidTo",df_modified_table_name.End_of_Month)
  df_modified_table_name = df_modified_table_name.withColumn("IBIChangeReason",lit("Record has been updated in source system."))
  df_modified_table_name = df_modified_table_name.withColumn("IBIOperationType",lit("Insert"))
  df_modified_table_name = df_modified_table_name.withColumn("IsDeleted",lit("false"))
  df_modified_table_name = df_modified_table_name.withColumn("IsLatest",lit("true"))

  #set dataframe for deleted records 
  df_deleted_table_name =  s_df_table_name\
                          .alias("silver")\
                          .where(s_df_table_name.IBIOperationType == "Delete")\
                          .selectExpr("silver.Forecast_Date as MergeKey1","silver.Snapshot_Date as MergeKey2","silver.End_of_Month as MergeKey3",\
                                      "silver.Sub_Region_Code as MergeKey4", "silver.Snp_Seq_No as MergeKey5", \
                                      "silver.*", "silver.IBIUpdatedDate as s_IBIUpdatedDate", "silver.IBIUpdatedBy as s_IBIUpdatedBy")

  df_deleted_table_name = df_deleted_table_name.withColumn("ValidFrom", trunc("End_of_Month", "month"))
  
  df_deleted_table_name = df_deleted_table_name.withColumn("ValidTo",df_deleted_table_name.End_of_Month)
  df_deleted_table_name = df_deleted_table_name.withColumn("IBIChangeReason",lit("Record got deleted from source system."))
  df_deleted_table_name = df_deleted_table_name.withColumn("IBIOperationType",lit("Delete"))
  df_deleted_table_name = df_deleted_table_name.withColumn("IsDeleted",lit("true"))
  df_deleted_table_name = df_deleted_table_name.withColumn("IsLatest",lit("false"))
  
  
  #check if hashcode already exists for the latest valid record, do not insert it (exclude it)
  df_inserted_table_name = df_inserted_table_name\
                           .alias("inserted")\
                           .join(g_df_table_name.alias("gold"),\
                                (df_inserted_table_name.Forecast_Date==g_df_table_name.Forecast_Date) & \
                                (df_inserted_table_name.Snapshot_Date==g_df_table_name.Snapshot_Date) & \
                                (df_inserted_table_name.End_of_Month==g_df_table_name.End_of_Month) & \
                                (df_inserted_table_name.Sub_Region_Code==g_df_table_name.Sub_Region_Code) & \
                                (df_inserted_table_name.Snp_Seq_No==g_df_table_name.Snp_Seq_No),"left_anti")\
                           .selectExpr("inserted.*")

  #combine all three dataframes
  df_modified_table_name = df_inserted_table_name\
                         .union(df_modified_table_name)\
                         .union(df_deleted_table_name)

  df_modified_table_name = df_modified_table_name\
                         .withColumn("RevenueForecastKey",row_number().over(Window.orderBy('Forecast_Date','Snapshot_Date','End_of_Month','Sub_Region_Code','Snp_Seq_No', 'ValidFrom')) + max_table_name_key)

  #generating schema 
  table_name_schema = df_modified_table_name.select("RevenueForecastKey","ValidFrom","ValidTo","Forecast_Date","Snapshot_Date","End_of_Month",\
                                                 "Sub_Region_Code","Relative_Month_Offset","Relative_EOM_Snp_Month_Offset","Relative_Snapshot_Month_Offset", \
                                                 "Snapshot_Day_of_Month","Snp_Seq_No","Currency_Code","Predicted_Revenue", \
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
                                                 "Root_Mean_Squared_Log_Error","Spearman_Correlation", "Current_Opp_Period_Count", "Opportunity_Period_Value",\
                                                 "IBIBatchID","IBIBatchStartDateTime","IBIChangeReason","IBIOperationType","IBIHashCode",\
                                                 "IBIChangeReason","IBIOperationType","IBICreatedBy","IBICreatedDate","s_IBIUpdatedBy",\
                                                 "s_IBIUpdatedDate","IsDeleted","IsLatest","MergeKey1", "MergeKey2", "MergeKey3", "MergeKey4", "MergeKey5").schema
  
  #appending schemae
  df_table_name = df_modified_table_name.select(table_name_schema.fieldNames())

  #audit fields
  df_table_name = df_table_name.withColumn("IBISourceFormat",lit(ibi_source_format))
  df_table_name = df_table_name.withColumn("IBISourceName",lit(ibi_source_name))
  df_table_name = df_table_name.withColumn("IBISourcePath",lit(ibi_source_path))
  df_table_name = df_table_name.withColumn("IBISourceSystemID",lit(ibi_source_system_id))
  df_table_name = df_table_name.withColumn("IBIDataCategory",lit(ibi_data_category))
  df_table_name = df_table_name.withColumn("IBILineageLevel",lit(ibi_lineage_level).cast("integer"))
  df_table_name = df_table_name.withColumn("IBIIngestionMethod",lit(ibi_ingestion_method))
  df_table_name = df_table_name.withColumn("IBIDataClassification",lit(ibi_data_classification))
  df_table_name = df_table_name.withColumn("IBIProgramName",lit(ibi_program_name))
  df_table_name = df_table_name.withColumn("IBIProgramPath",lit(ibi_program_path))
  df_table_name = df_table_name.withColumn("IBIValidationStatus",lit(ibi_validation_status))

  g_delta_table_name.alias("gold").merge(
    df_table_name.alias("silver"),
    "gold.Forecast_Date = silver.MergeKey1 AND gold.Snapshot_Date = silver.MergeKey2 AND gold.End_of_Month = silver.MergeKey3 AND gold.Sub_Region_Code = silver.MergeKey4 AND gold.Snp_Seq_No = silver.MergeKey5")\
    .whenMatchedUpdate(
     condition = "silver.IsDeleted == true",  
     set = {
      #audit fields
      "IBIBatchID":  "silver.IBIBatchID",
      "IBIBatchStartDateTime": "silver.IBIBatchStartDateTime",
      "IBIUpdatedBy": "silver.s_IBIUpdatedBy",
      "IBIUpdatedDate":"silver.s_IBIUpdatedDate",
      "IBIChangeReason": "silver.IBIChangeReason",
      "IBIOperationType": "silver.IBIOperationType",
      "IsLatest": "silver.IsLatest",
      "IsDeleted": "silver.IsDeleted"
     }
  ).whenMatchedUpdate(
      #updating previous record valid to for modified records
      condition = "(silver.IBIHashCode <> gold.IBIHashCode or gold.IsDeleted == true) and silver.IsDeleted == false",  
      set = {
        "ValidFrom": "silver.ValidFrom",
        "ValidTo": "silver.ValidTo",
        #actual source fields
        "ValidFrom": "silver.ValidFrom",
        "ValidTo": "silver.ValidTo",      
        "Forecast_Date": "silver.Forecast_Date",
        "Snapshot_Date": "silver.Snapshot_Date",
        "End_of_Month": "silver.End_of_Month",
        "Sub_Region_Code": "silver.Sub_Region_Code",
        "Relative_EOM_Snp_Month_Offset": "silver.Relative_EOM_Snp_Month_Offset",
        "Relative_Month_Offset": "silver.Relative_Month_Offset",
        "Relative_Snapshot_Month_Offset": "silver.Relative_Snapshot_Month_Offset",
        "Snapshot_Day_of_Month": "silver.Snapshot_Day_of_Month",
        "Snp_Seq_No": "silver.Snp_Seq_No",
        "Currency_Code": "silver.Currency_Code",
        "Predicted_Revenue": "silver.Predicted_Revenue",
        "Revenue": "silver.Revenue",
        "Project_Period_Count": "silver.Project_Period_Count",
        "Project_Count": "silver.Project_Count",
        "Project_Period_Price": "silver.Project_Period_Price",
        "Project_Price": "silver.Project_Price",
        "Conversions": "silver.Conversions",
        "Opportunity_Period_Count": "silver.Opportunity_Period_Count",
        "Opportunity_Count": "silver.Opportunity_Count",
        "Current_Opp_Period_Value": "silver.Current_Opp_Period_Value",
        "Opportunity_Value": "silver.Opportunity_Value",
        "Win_Rate": "silver.Win_Rate",
        "Nominal_Hours": "silver.Nominal_Hours",
        "Utilization_Billable": "silver.Utilization_Billable",
        "Headcount_Billable": "silver.Headcount_Billable",
        "Headcount_Non_Billable": "silver.Headcount_Non_Billable",
        "Headcount_Contingent_Billable": "silver.Headcount_Contingent_Billable",
        "Headcount_Contingent_Non_Billable": "silver.Headcount_Contingent_Non_Billable",
        "Headcount_Contingent_Unknown": "silver.Headcount_Contingent_Unknown",
        "EURONEXT_100": "silver.EURONEXT_100",
        "FTSE_100": "silver.FTSE_100",
        "Nikkei_225": "silver.Nikkei_225",
        "SP_500": "silver.SP_500",
        "SSE_Composite_Index": "silver.SSE_Composite_Index",
        "Pipeline_Trend": "silver.Pipeline_Trend",
        "Pipeline_Active_Unrecognized": "silver.Pipeline_Active_Unrecognized",
        "Pipeline_Opportunity": "silver.Pipeline_Opportunity",
        "Pipeline_Opportunity_ML": "silver.Pipeline_Opportunity_ML",
        "Pipeline_Recognized": "silver.Pipeline_Recognized",
        "Pipeline_at_100_Percent_Active_Unrecognized": "silver.Pipeline_at_100_Percent_Active_Unrecognized",
        "Pipeline_at_100_Percent_Opportunity": "silver.Pipeline_at_100_Percent_Opportunity",
        "Pipeline_at_100_Percent_Opportunity_ML": "silver.Pipeline_at_100_Percent_Opportunity_ML",
        "Pipeline_at_100_Percent_Recognized": "silver.Pipeline_at_100_Percent_Recognized",
        "Yield_Active_Unrecognized": "silver.Yield_Active_Unrecognized",
        "Yield_Opportunity_ML": "silver.Yield_Opportunity_ML",
        "Yield_Recognized": "silver.Yield_Recognized",
        "Run_ID": "silver.Run_ID",
        "Run_Date": "silver.Run_Date",
        "Explained_Variance" : "silver.Explained_Variance",
        "Mean_Absolute_Error": "silver.Mean_Absolute_Error",
        "Mean_Absolute_Percentage_Error": "silver.Mean_Absolute_Percentage_Error",
        "Median_Absolute_Error": "silver.Median_Absolute_Error",
        "Normalized_Mean_Absolute_Error": "silver.Normalized_Mean_Absolute_Error",
        "Normalized_Median_Absolute_Error": "silver.Normalized_Median_Absolute_Error",
        "Normalized_Root_Mean_Squared_Error": "silver.Normalized_Root_Mean_Squared_Error",
        "Normalized_Root_Mean_Squared_Log_Error": "silver.Normalized_Root_Mean_Squared_Log_Error",
        "R2_Score": "silver.R2_Score" ,
        "Root_Mean_Squared_Error": "silver.Root_Mean_Squared_Error", 
        "Root_Mean_Squared_Log_Error": "silver.Root_Mean_Squared_Log_Error",
        "Spearman_Correlation": "silver.Spearman_Correlation",        
        "Current_Opp_Period_Count": "silver.Current_Opp_Period_Count",
        "Opportunity_Period_Value": "silver.Opportunity_Period_Value",
        #audit fields
        "IBIBatchID": "silver.IBIBatchID",
        "IBICreatedBy": "silver.IBICreatedBy",
        "IBIUpdatedBy": "silver.s_IBIUpdatedBy",
        "IBICreatedDate": "silver.IBICreatedDate",
        "IBIUpdatedDate":"silver.s_IBIUpdatedDate",
        "IBIChangeReason": "silver.IBIChangeReason",
        "IBIOperationType": "silver.IBIOperationType",
        "IBISourceFormat": "silver.IBISourceFormat",
        "IBISourceName": "silver.IBISourceName",
        "IBISourcePath": "silver.IBISourcePath",
        "IBISourceSystemID": "silver.IBISourceSystemID",
        "IBIDataCategory": "silver.IBIDataCategory",
        "IBILineageLevel": "silver.IBILineageLevel",
        "IBIIngestionMethod": "silver.IBIIngestionMethod",
        "IBIBatchStartDateTime": "silver.IBIBatchStartDateTime",
        "IBIDataClassification": "silver.IBIDataClassification",
        "IBIProgramName": "silver.IBIProgramName",
        "IBIProgramPath": "silver.IBIProgramPath",
        "IBIValidationStatus": "silver.IBIValidationStatus",
        "IBIHashCode":  "silver.IBIHashCode",
        "IsLatest": "silver.IsLatest",
        "IsDeleted":  "silver.IsDeleted"
      }
    ).whenNotMatchedInsert(
    values = {
        "RevenueForecastKey": "silver.RevenueForecastKey",
        "ValidFrom": "silver.ValidFrom",
        "ValidTo": "silver.ValidTo",      
        "Forecast_Date": "silver.Forecast_Date",
        "Snapshot_Date": "silver.Snapshot_Date",
        "End_of_Month": "silver.End_of_Month",
        "Sub_Region_Code": "silver.Sub_Region_Code",
        "Relative_EOM_Snp_Month_Offset": "silver.Relative_EOM_Snp_Month_Offset",
        "Relative_Month_Offset": "silver.Relative_Month_Offset",
        "Relative_Snapshot_Month_Offset": "silver.Relative_Snapshot_Month_Offset",
        "Snapshot_Day_of_Month": "silver.Snapshot_Day_of_Month",
        "Snp_Seq_No": "silver.Snp_Seq_No",
        "Currency_Code": "silver.Currency_Code",
        "Predicted_Revenue": "silver.Predicted_Revenue",
        "Revenue": "silver.Revenue",
        "Project_Period_Count": "silver.Project_Period_Count",
        "Project_Count": "silver.Project_Count",
        "Project_Period_Price": "silver.Project_Period_Price",
        "Project_Price": "silver.Project_Price",
        "Conversions": "silver.Conversions",
        "Opportunity_Period_Count": "silver.Opportunity_Period_Count",
        "Opportunity_Count": "silver.Opportunity_Count",
        "Current_Opp_Period_Value": "silver.Current_Opp_Period_Value",
        "Opportunity_Value": "silver.Opportunity_Value",
        "Win_Rate": "silver.Win_Rate",
        "Nominal_Hours": "silver.Nominal_Hours",
        "Utilization_Billable": "silver.Utilization_Billable",
        "Headcount_Billable": "silver.Headcount_Billable",
        "Headcount_Non_Billable": "silver.Headcount_Non_Billable",
        "Headcount_Contingent_Billable": "silver.Headcount_Contingent_Billable",
        "Headcount_Contingent_Non_Billable": "silver.Headcount_Contingent_Non_Billable",
        "Headcount_Contingent_Unknown": "silver.Headcount_Contingent_Unknown",
        "EURONEXT_100": "silver.EURONEXT_100",
        "FTSE_100": "silver.FTSE_100",
        "Nikkei_225": "silver.Nikkei_225",
        "SP_500": "silver.SP_500",
        "SSE_Composite_Index": "silver.SSE_Composite_Index",
        "Pipeline_Trend": "silver.Pipeline_Trend",
        "Pipeline_Active_Unrecognized": "silver.Pipeline_Active_Unrecognized",
        "Pipeline_Opportunity": "silver.Pipeline_Opportunity",
        "Pipeline_Opportunity_ML": "silver.Pipeline_Opportunity_ML",
        "Pipeline_Recognized": "silver.Pipeline_Recognized",
        "Pipeline_at_100_Percent_Active_Unrecognized": "silver.Pipeline_at_100_Percent_Active_Unrecognized",
        "Pipeline_at_100_Percent_Opportunity": "silver.Pipeline_at_100_Percent_Opportunity",
        "Pipeline_at_100_Percent_Opportunity_ML": "silver.Pipeline_at_100_Percent_Opportunity_ML",
        "Pipeline_at_100_Percent_Recognized": "silver.Pipeline_at_100_Percent_Recognized",
        "Yield_Active_Unrecognized": "silver.Yield_Active_Unrecognized",
        "Yield_Opportunity_ML": "silver.Yield_Opportunity_ML",
        "Yield_Recognized": "silver.Yield_Recognized",
        "Run_ID": "silver.Run_ID",
        "Run_Date": "silver.Run_Date",
        "Explained_Variance" : "silver.Explained_Variance",
        "Mean_Absolute_Error": "silver.Mean_Absolute_Error",
        "Mean_Absolute_Percentage_Error": "silver.Mean_Absolute_Percentage_Error",
        "Median_Absolute_Error": "silver.Median_Absolute_Error",
        "Normalized_Mean_Absolute_Error": "silver.Normalized_Mean_Absolute_Error",
        "Normalized_Median_Absolute_Error": "silver.Normalized_Median_Absolute_Error",
        "Normalized_Root_Mean_Squared_Error": "silver.Normalized_Root_Mean_Squared_Error",
        "Normalized_Root_Mean_Squared_Log_Error": "silver.Normalized_Root_Mean_Squared_Log_Error",
        "R2_Score": "silver.R2_Score" ,
        "Root_Mean_Squared_Error": "silver.Root_Mean_Squared_Error", 
        "Root_Mean_Squared_Log_Error": "silver.Root_Mean_Squared_Log_Error",
        "Spearman_Correlation": "silver.Spearman_Correlation",  
        "Current_Opp_Period_Count": "silver.Current_Opp_Period_Count",
        "Opportunity_Period_Value": "silver.Opportunity_Period_Value",      
        #audit fields
        "IBIBatchID": "silver.IBIBatchID",
        "IBICreatedBy": "silver.IBICreatedBy",
        "IBIUpdatedBy": "silver.s_IBIUpdatedBy",
        "IBICreatedDate": "silver.IBICreatedDate",
        "IBIUpdatedDate":"silver.s_IBIUpdatedDate",
        "IBIChangeReason": "silver.IBIChangeReason",
        "IBIOperationType": "silver.IBIOperationType",
        "IBISourceFormat": "silver.IBISourceFormat",
        "IBISourceName": "silver.IBISourceName",
        "IBISourcePath": "silver.IBISourcePath",
        "IBISourceSystemID": "silver.IBISourceSystemID",
        "IBIDataCategory": "silver.IBIDataCategory",
        "IBILineageLevel": "silver.IBILineageLevel",
        "IBIIngestionMethod": "silver.IBIIngestionMethod",
        "IBIBatchStartDateTime": "silver.IBIBatchStartDateTime",
        "IBIDataClassification": "silver.IBIDataClassification",
        "IBIProgramName": "silver.IBIProgramName",
        "IBIProgramPath": "silver.IBIProgramPath",
        "IBIValidationStatus": "silver.IBIValidationStatus",
        "IBIHashCode":  "silver.IBIHashCode",
        "IsLatest": "silver.IsLatest",
        "IsDeleted":  "silver.IsDeleted"
    }
  ).execute()
  
except Exception as error: 
  print(error)
  log_error("{} {}".format(notebook, error)) #log error in sentry
  #raise dbutils.notebook.exit(error) #raise the exception
  raise error #raise the exception
  

# COMMAND ----------


# Databricks notebook source
#define widgets - NEED TO DEFINE IT ONCE
#dbutils.widgets.text("environment", "","")
#dbutils.widgets.text("system_name", "","")
#dbutils.widgets.text("data_load_type", "","")
#dynamic variables (pass it from ADF)
environment = dbutils.widgets.get("environment")
system_name = dbutils.widgets.get("system_name")
data_load_type = dbutils.widgets.get("data_load_type") # Full/Incremental

# COMMAND ----------

# MAGIC %run ../../bi_config/pbi_common

# COMMAND ----------

#============================= Global Variable Declaration ===================================#
from datetime import datetime
from datetime import date, timedelta, datetime
from pyspark.sql import *
from delta.tables import *
from pyspark.sql.types import TimestampType, LongType,StructType, StructField, DateType, StringType, DecimalType, IntegerType
from pyspark.sql.functions import col,concat,lit,current_date, when, to_date, unix_timestamp, from_unixtime, regexp_replace
import os
#from ibi_packages import functions as fn
notebook = os.path.basename(getNotebookPath())

try:

  #static variables
  g_bi_config_parameters_path = "/mnt/"+ environment + "/gold/g_bi_config_parameters"
  table_name = "fact_revenue_forecast"

  #reading config table
  df_bi_configuration  = spark.read.format("delta").load(g_bi_config_parameters_path)
  df_bi_configuration  = df_bi_configuration.filter((df_bi_configuration.SystemName == "bimodelapi"))

  #initializing config parameter values
  service_account_name =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "service_account_name")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  source_path          =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "source_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  bronze_folder_path   =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "bronze_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  silver_folder_path   =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "silver_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  gold_folder_path     =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "gold_folder_path")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  conf_threshold_value =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "threshold")\
                                               .select("ParameterValue")\
                                               .collect()[0][0]

  source_system_id = system_name
  
  #setting delta tables path
  g_automljobruninfo_path = gold_folder_path + "/automljobruninfo"  # Story # 3181
  b_table_name_path = bronze_folder_path + "/b_automl_{}".format(table_name) 
  s_table_name_path = silver_folder_path + "/s_automl_{}".format(table_name)
  g_table_name_path = gold_folder_path + "/g_automl_{}".format(table_name)
  
  #reading job run info
  df_job_info = spark.read.format("delta").load(g_automljobruninfo_path)   # Story # 3181  

  #initialize batch id and batch start date time variables
  batch_id = df_job_info.agg({"batch_id" : "max"}).collect()[0][0]
  batch_start_datetime = df_job_info.agg({"batch_start_datetime" : "max"}).collect()[0][0]

  #audit fields
  ibi_batch_id = batch_id
  ibi_created_by =  getNotebookUser() #service_account_name
  ibi_updated_by =  getNotebookUser() #service_account_name
  ibi_created_date = getUtcTimeNow() #this should be always UTC for TS 
  ibi_updated_date = getUtcTimeNow() #this should be always UTC for TS 
  ibi_source_format = "csv" #static
  ibi_source_name = "revenue_forecast" #static
  ibi_source_path = source_path #coming from config table
  ibi_source_system_id =  source_system_id  #from config table
  ibi_data_category = "Master"  #static
  ibi_lineage_level = 1 #static
  ibi_ingestion_method = "python notebook"  #static
  ibi_data_classification = "Private"  #static
  ibi_program_path = getNotebookPath()
  ibi_program_name = os.path.basename(ibi_program_path)
  ibi_validation_status = "success" #static
  ibi_data_load = data_load_type #dynamic from ADF
  
  if ibi_data_load != "Full":
    filename = "revenue_forecast_{}{}".format(datetime.strftime(batch_start_datetime,'%Y-%m-%d'),".csv")
  else:
    filename = "revenue_forecast_*.csv"

  input_data_path =  "/mnt/{}/automl_rev_region_forecast/outputs/forecast/{}".format(environment,filename)
  
  print(input_data_path)
  #reading data from csv file
  # TS Hot Fix -2021-09-10 11AM EST -> Move EURONEXT_100, FTSE_100 , Nikkei_225, SP_500, SSE_Composite_Index columns after Revenue. 
  csv_schema  = StructType(
                  [StructField("Forecast_Date", StringType(), True),
                  StructField("Snapshot_Date", StringType(), True),
                  StructField("End_of_Month", StringType(), True),
                  StructField("Sub_Region_Code", StringType(), True),
                  StructField("Relative_Month_Offset", StringType(), True),
                  StructField("Relative_Snapshot_Month_Offset", StringType(), True),
                  StructField("Relative_EOM_Snp_Month_Offset", StringType(), True),
                  StructField("Snapshot_Day_of_Month", StringType(), True),
                  StructField("Snp_Seq_No", StringType(), True),
                  StructField("Currency_Code", StringType(), True),
                  StructField("Predicted_Revenue", StringType(), True),
                  StructField("Revenue", StringType(), True),
                  StructField("EURONEXT_100", StringType(), True),
                  StructField("FTSE_100", StringType(), True),
                  StructField("Nikkei_225", StringType(), True),
                  StructField("SP_500", StringType(), True),
                  StructField("SSE_Composite_Index", StringType(), True),                   
                  StructField("Project_Period_Count", StringType(), True),
                  StructField("Project_Count", StringType(), True),
                  StructField("Project_Period_Price", StringType(), True),
                  StructField("Project_Price", StringType(), True),
                  StructField("Conversions", StringType(), True),
                  StructField("Opportunity_Period_Count", StringType(), True),
                  StructField("Opportunity_Count", StringType(), True),
                  StructField("Current_Opp_Period_Value", StringType(), True),
                  StructField("Opportunity_Value", StringType(), True),
                  StructField("Win_Rate", StringType(), True),
                  StructField("Nominal_Hours", StringType(), True),
                  StructField("Utilization_Billable", StringType(), True),
                  StructField("Headcount_Billable", StringType(), True),
                  StructField("Headcount_Non-Billable", StringType(), True),
                  StructField("Headcount_Contingent_Billable", StringType(), True),
                  StructField("Headcount_Contingent_Non-Billable", StringType(), True),
                  StructField("Headcount_Contingent_Unknown", StringType(), True),
                  StructField("Pipeline_Trend", StringType(), True),
                  StructField("Pipeline_Active_Unrecognized", StringType(), True),
                  StructField("Pipeline_Opportunity", StringType(), True),
                  StructField("Pipeline_Opportunity_ML", StringType(), True),
                  StructField("Pipeline_Recognized", StringType(), True),
                  StructField("Pipeline_at_100_Percent_Active_Unrecognized", StringType(), True),
                  StructField("Pipeline_at_100_Percent_Opportunity", StringType(), True),
                  StructField("Pipeline_at_100_Percent_Opportunity_ML", StringType(), True),
                  StructField("Pipeline_at_100_Percent_Recognized", StringType(), True),
                  StructField("Yield_Active_Unrecognized", StringType(), True),
                  StructField("Yield_Opportunity_ML", StringType(), True),
                  StructField("Yield_Recognized", StringType(), True),
                  StructField("Run_Date", StringType(), True),
                  StructField("Run_ID", StringType(), True),
                  StructField("Explained_Variance", StringType(), True),
                  StructField("Mean_Absolute_Error", StringType(), True),
                  StructField("Mean_Absolute_Percentage_Error", StringType(), True),
                  StructField("Median_Absolute_Error", StringType(), True),
                  StructField("Normalized_Mean_Absolute_Error", StringType(), True),
                  StructField("Normalized_Median_Absolute_Error", StringType(), True),
                  StructField("Normalized_Root_Mean_Squared_Error", StringType(), True),
                  StructField("Normalized_Root_Mean_Squared_Log_Error", StringType(), True),
                  StructField("R2_Score", StringType(), True),
                  StructField("Root_Mean_Squared_Error", StringType(), True),
                  StructField("Root_Mean_Squared_Log_Error", StringType(), True),
                  StructField("Spearman_Correlation", StringType(), True),
                  StructField("origin", StringType(), True),
                  StructField("horizon_origin", StringType(), True),
                  StructField("Current_Opp_Period_Count", StringType(), True),
                  StructField("Opportunity_Period_Value", StringType(), True)])
  
  df_table_name = spark.read.format("csv").load(input_data_path ,header = True, quote = '"',multiLine = True, sep = "," , escape = '"', schema = csv_schema)
  #df_table_name = spark.read.format("csv").load(input_data_path ,header = True, quote = '"',multiLine = True, sep = "," , escape = '"')
  func =  udf (lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
 
  display(df_table_name)
  df_table_name = df_table_name.withColumnRenamed("Headcount_Non-Billable","Headcount_Non_Billable")\
                               .withColumnRenamed("Headcount_Contingent_Non-Billable","Headcount_Contingent_Non_Billable")
  
  df_table_name = df_table_name.withColumn('Forecast_Date',func(col('Forecast_Date')))
  df_table_name = df_table_name.withColumn('Snapshot_Date',func(col('Snapshot_Date')))
  df_table_name = df_table_name.withColumn('End_of_Month', func(col('End_of_Month')))
  df_table_name = df_table_name.withColumn('Run_Date', func(col('Run_Date')))
  df_table_name = df_table_name.withColumn('Relative_EOM_Snp_Month_Offset',col('Relative_EOM_Snp_Month_Offset').cast("int"))
  df_table_name = df_table_name.withColumn('Relative_Month_Offset',col('Relative_Month_Offset').cast("int"))
  df_table_name = df_table_name.withColumn('Relative_Snapshot_Month_Offset',col('Relative_Snapshot_Month_Offset').cast("int"))
  df_table_name = df_table_name.withColumn('Snapshot_Day_of_Month',col('Snapshot_Day_of_Month').cast("int"))
  df_table_name = df_table_name.withColumn('Snp_Seq_No',col('Snp_Seq_No').cast("int"))
  df_table_name = df_table_name.withColumn('Predicted_Revenue',col('Predicted_Revenue').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Revenue',col('Revenue').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Project_Period_Count',col('Project_Period_Count').cast("int"))
  df_table_name = df_table_name.withColumn('Project_Count',col('Project_Count').cast("int"))
  df_table_name = df_table_name.withColumn('Project_Period_Price',col('Project_Period_Price').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Project_Price',col('Project_Price').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Conversions',col('Conversions').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Opportunity_Period_Count',col('Opportunity_Period_Count').cast("int"))
  df_table_name = df_table_name.withColumn('Opportunity_Count',col('Opportunity_Count').cast("int"))
  df_table_name = df_table_name.withColumn('Current_Opp_Period_Value',col('Current_Opp_Period_Value').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Opportunity_Value',col('Opportunity_Value').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Win_Rate',col('Win_Rate').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Nominal_Hours',col('Nominal_Hours').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Utilization_Billable',col('Utilization_Billable').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Headcount_Billable',col('Headcount_Billable').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Headcount_Non_Billable',col('Headcount_Non_Billable').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Headcount_Contingent_Billable',col('Headcount_Contingent_Billable').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Headcount_Contingent_Non_Billable',col('Headcount_Contingent_Non_Billable').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Headcount_Contingent_Unknown',col('Headcount_Contingent_Unknown').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('EURONEXT_100',col('EURONEXT_100').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('FTSE_100',col('FTSE_100').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Nikkei_225',col('Nikkei_225').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('SP_500',col('SP_500').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('SSE_Composite_Index',col('SSE_Composite_Index').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Pipeline_Trend',col('Pipeline_Trend').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Pipeline_Active_Unrecognized',col('Pipeline_Active_Unrecognized').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Pipeline_Opportunity',col('Pipeline_Opportunity').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Pipeline_Opportunity_ML',col('Pipeline_Opportunity_ML').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Pipeline_Recognized',col('Pipeline_Recognized').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Pipeline_at_100_Percent_Active_Unrecognized',col('Pipeline_at_100_Percent_Active_Unrecognized').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Pipeline_at_100_Percent_Opportunity',col('Pipeline_at_100_Percent_Opportunity').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Pipeline_at_100_Percent_Opportunity_ML',col('Pipeline_at_100_Percent_Opportunity_ML').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Pipeline_at_100_Percent_Recognized',col('Pipeline_at_100_Percent_Recognized').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Yield_Active_Unrecognized',col('Yield_Active_Unrecognized').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Yield_Opportunity_ML',col('Yield_Opportunity_ML').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Yield_Recognized',col('Yield_Recognized').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Explained_Variance',col('Explained_Variance').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Mean_Absolute_Error',col('Mean_Absolute_Error').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Mean_Absolute_Percentage_Error',col('Mean_Absolute_Percentage_Error').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Median_Absolute_Error',col('Median_Absolute_Error').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Normalized_Mean_Absolute_Error',col('Normalized_Mean_Absolute_Error').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Normalized_Median_Absolute_Error',col('Normalized_Median_Absolute_Error').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Normalized_Root_Mean_Squared_Error',col('Normalized_Root_Mean_Squared_Error').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Normalized_Root_Mean_Squared_Log_Error',col('Normalized_Root_Mean_Squared_Log_Error').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('R2_Score',col('R2_Score').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Root_Mean_Squared_Error',col('Root_Mean_Squared_Error').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Root_Mean_Squared_Log_Error',col('Root_Mean_Squared_Log_Error').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Spearman_Correlation',col('Spearman_Correlation').cast(DecimalType(32,6)))
  df_table_name = df_table_name.withColumn('Current_Opp_Period_Count',col('Current_Opp_Period_Count').cast("int"))
  df_table_name = df_table_name.withColumn('Opportunity_Period_Value',col('Opportunity_Period_Value').cast(DecimalType(32,6)))
  
  
  #display(df_table_name)
  #get dataframe columns : make sure df.columns generate source fields list only. do not get columns after adding any metadata field 
  df_table_name_cols = df_table_name.columns 
  timestamp_col = {item[0]:"1900-01-01 00:00:00.000000" for item in df_table_name.dtypes if item[1].startswith('timestamp')}
  num_col = {item[0]:"0" for item in df_table_name.dtypes if item[1].startswith('int') or item[1].startswith('decimal')}
  df_table_name = df_table_name.fillna('')\
                               .fillna(timestamp_col)\
                               .fillna(num_col)
    
  df_table_name = df_table_name.withColumn("IBIHashCode", generate_hash(lit(concat(*df_table_name_cols))))
  df_table_name = df_table_name.withColumn("IBIBatchID", lit(batch_id))
  df_table_name = df_table_name.withColumn("IBIBatchStartDateTime",lit(batch_start_datetime))
  
  #initializing bronze & silver dataframes
  b_df_table_name = spark.read.format("delta").load(b_table_name_path)
  s_df_table_name = spark.read.format("delta").load(s_table_name_path)
  
  #do not consider already deleted records while merging with silver delta table
  s_df_table_name = s_df_table_name \
                  .where(s_df_table_name.IBIOperationType != "Delete")

#   if ibi_data_load != "Full": 
#       #checking for modified records 
#       df_table_name = df_table_name\
#                .join(s_df_table_name,df_table_name.IBIHashCode==s_df_table_name.IBIHashCode,"left_anti");

  #loading modified records into bronze for further processing
  drop_col = ("origin","horizon_origin")
  df_table_name = df_table_name.drop(*drop_col)
  df_table_name.write.format("delta").mode("overwrite").save(b_table_name_path)
#======================================Load data to Silver  ===========================================#
  #calculating threshhold
  b_forecast_date = b_df_table_name.agg({"Forecast_Date" : "max"}).collect()[0][0] 
  if ibi_data_load != "Full": 
    b_count = b_df_table_name.filter(b_df_table_name.Forecast_Date == lit(b_forecast_date)).agg({"IBIHashCode" : "count"}).collect()[0][0]
    s_count = s_df_table_name.filter(s_df_table_name.Forecast_Date == lit(b_forecast_date)).agg({"IBIHashCode" : "count"}).collect()[0][0]
  else:
    b_count = b_df_table_name.agg({"IBIHashCode" : "count"}).collect()[0][0]
    s_count = s_df_table_name.agg({"IBIHashCode" : "count"}).collect()[0][0]
  
  threshold = (b_count-s_count)/(s_count or not s_count) #confirm threshhold formula with Mukesh
  
  print("b_count/b_count/b_count....{}/{}/{}".format(str(b_count),str(s_count),str(threshold)))
  #print(s_table_name_path)
  #initializing silver table for merge
  s_delta_table_name = DeltaTable.forPath(spark, s_table_name_path) 

  #initializing delta table schema
  table_name_schema = b_df_table_name\
                      .select("*").schema; 
  
  #filtering based on batchid
  b_df_table_name = b_df_table_name\
                  .filter((b_df_table_name.IBIBatchID == ibi_batch_id))\
                  .select(table_name_schema.fieldNames());
  
  #setting delered flag false for modified records
  b_df_table_name = b_df_table_name.withColumn("IsDeleted",lit("false"))

  #finding the deleted records
  #if data load type is Full & threshhold requirement meets then perform delete operaton
  #if ibi_data_load == "Full"  and threshold > float(conf_threshold_value): 
  if  threshold > float(conf_threshold_value):
      #if records exists in silver and but not exists in bronze table (mark them as deleted)
      if ibi_data_load != "Full" :  # Incremental data filter only today's forecast date
        s_df_deleted_records = s_df_table_name\
                               .join(b_df_table_name,(s_df_table_name.Forecast_Date==b_df_table_name.Forecast_Date) & \
                               (s_df_table_name.Snapshot_Date==b_df_table_name.Snapshot_Date) & \
                               (s_df_table_name.End_of_Month==b_df_table_name.End_of_Month) & \
                               (s_df_table_name.Sub_Region_Code==b_df_table_name.Sub_Region_Code) &\
                               (s_df_table_name.Snp_Seq_No==b_df_table_name.Snp_Seq_No),"left_anti")\
                               .where((s_df_table_name.IBIOperationType != "Delete") & \
                                      (s_df_table_name.Forecast_Date    == lit(b_forecast_date)))\
                               .select(table_name_schema.fieldNames());
      else:
        s_df_deleted_records = s_df_table_name\
                       .join(b_df_table_name,(s_df_table_name.Forecast_Date==b_df_table_name.Forecast_Date) & \
                       (s_df_table_name.Snapshot_Date==b_df_table_name.Snapshot_Date) & \
                       (s_df_table_name.End_of_Month==b_df_table_name.End_of_Month) & \
                       (s_df_table_name.Sub_Region_Code==b_df_table_name.Sub_Region_Code) &\
                       (s_df_table_name.Snp_Seq_No==b_df_table_name.Snp_Seq_No),"left_anti")\
                       .where(s_df_table_name.IBIOperationType != "Delete")\
                       .select(table_name_schema.fieldNames());

      #marking as deleted
      s_df_deleted_records = s_df_deleted_records.withColumn("IsDeleted",lit("true"))
      
      #combining deleted records to perform complete merge
      b_df_table_name = b_df_table_name.union(s_df_deleted_records)
  
  #silver audit fields
  b_df_table_name = b_df_table_name.withColumn("IBICreatedBy",lit(ibi_created_by))
  b_df_table_name = b_df_table_name.withColumn("IBIUpdatedBy",lit(ibi_updated_by))
  b_df_table_name = b_df_table_name.withColumn("IBICreatedDate",lit(ibi_created_date).cast(TimestampType()))
  b_df_table_name = b_df_table_name.withColumn("IBIUpdatedDate",lit(ibi_updated_date).cast(TimestampType()))
  b_df_table_name = b_df_table_name.withColumn("IBISourceFormat",lit(ibi_source_format))
  b_df_table_name = b_df_table_name.withColumn("IBISourceName",lit(ibi_source_name))
  b_df_table_name = b_df_table_name.withColumn("IBISourcePath",lit(ibi_source_path))
  b_df_table_name = b_df_table_name.withColumn("IBISourceSystemID",lit(ibi_source_system_id))
  b_df_table_name = b_df_table_name.withColumn("IBIDataCategory",lit(ibi_data_category))
  b_df_table_name = b_df_table_name.withColumn("IBILineageLevel",lit(ibi_lineage_level).cast("integer"))
  b_df_table_name = b_df_table_name.withColumn("IBIIngestionMethod",lit(ibi_ingestion_method))
  b_df_table_name = b_df_table_name.withColumn("IBIDataClassification",lit(ibi_data_classification))
  b_df_table_name = b_df_table_name.withColumn("IBIProgramName",lit(ibi_program_name))
  b_df_table_name = b_df_table_name.withColumn("IBIProgramPath",lit(ibi_program_path))
  b_df_table_name = b_df_table_name.withColumn("IBIValidationStatus",lit(ibi_validation_status))

  s_delta_table_name.alias("silver").merge(
  b_df_table_name.alias("bronze"),
    "silver.Forecast_Date = bronze.Forecast_Date AND silver.Snapshot_Date = bronze.Snapshot_Date AND silver.End_of_Month = bronze.End_of_Month AND silver.Sub_Region_Code = bronze.Sub_Region_Code AND silver.Snp_Seq_No = bronze.Snp_Seq_No")\
  .whenMatchedUpdate(
    condition = "bronze.IsDeleted == true",
    set = {
      "IBIBatchID": lit(batch_id), #"bronze.IBIBatchID",
      "IBIBatchStartDateTime": lit(batch_start_datetime), #"bronze.IBIBatchStartDateTime",
      "IBIUpdatedBy": "bronze.IBIUpdatedBy",
      "IBIUpdatedDate":"bronze.IBIUpdatedDate",
      "IBIChangeReason": lit("Record got deleted from source system."),
      "IBIOperationType": lit("Delete")
    }
  ).whenMatchedUpdate(
    #keys matched but hashcode not matched
    condition = "silver.IBIHashCode <> bronze.IBIHashCode or silver.IBIOperationType == 'Delete'" ,  
    set = {
      "Forecast_Date": "bronze.Forecast_Date",
      "Snapshot_Date": "bronze.Snapshot_Date",
      "End_of_Month": "bronze.End_of_Month",
      "Sub_Region_Code": "bronze.Sub_Region_Code",
      "Relative_EOM_Snp_Month_Offset": "bronze.Relative_EOM_Snp_Month_Offset",
      "Relative_Month_Offset": "bronze.Relative_Month_Offset",
      "Relative_Snapshot_Month_Offset": "bronze.Relative_Snapshot_Month_Offset",
      "Snapshot_Day_of_Month": "bronze.Snapshot_Day_of_Month",
      "Snp_Seq_No": "bronze.Snp_Seq_No",
      "Currency_Code": "bronze.Currency_Code",
      "Predicted_Revenue": "bronze.Predicted_Revenue",
      "Revenue": "bronze.Revenue",
      "Project_Period_Count": "bronze.Project_Period_Count",
      "Project_Count": "bronze.Project_Count",
      "Project_Period_Price": "bronze.Project_Period_Price",
      "Project_Price": "bronze.Project_Price",
      "Conversions": "bronze.Conversions",
      "Opportunity_Period_Count": "bronze.Opportunity_Period_Count",
      "Opportunity_Count": "bronze.Opportunity_Count",
      "Current_Opp_Period_Value": "bronze.Current_Opp_Period_Value",
      "Opportunity_Value": "bronze.Opportunity_Value",
      "Win_Rate": "bronze.Win_Rate",
      "Nominal_Hours": "bronze.Nominal_Hours",
      "Utilization_Billable": "bronze.Utilization_Billable",
      "Headcount_Billable": "bronze.Headcount_Billable",
      "Headcount_Non_Billable": "bronze.Headcount_Non_Billable",
      "Headcount_Contingent_Billable": "bronze.Headcount_Contingent_Billable",
      "Headcount_Contingent_Non_Billable": "bronze.Headcount_Contingent_Non_Billable",
      "Headcount_Contingent_Unknown": "bronze.Headcount_Contingent_Unknown",
      "EURONEXT_100": "bronze.EURONEXT_100",
      "FTSE_100": "bronze.FTSE_100",
      "Nikkei_225": "bronze.Nikkei_225",
      "SP_500": "bronze.SP_500",
      "SSE_Composite_Index": "bronze.SSE_Composite_Index",
      "Pipeline_Trend": "bronze.Pipeline_Trend",
      "Pipeline_Active_Unrecognized": "bronze.Pipeline_Active_Unrecognized",
      "Pipeline_Opportunity": "bronze.Pipeline_Opportunity",
      "Pipeline_Opportunity_ML": "bronze.Pipeline_Opportunity_ML",
      "Pipeline_Recognized": "bronze.Pipeline_Recognized",
      "Pipeline_at_100_Percent_Active_Unrecognized": "bronze.Pipeline_at_100_Percent_Active_Unrecognized",
      "Pipeline_at_100_Percent_Opportunity": "bronze.Pipeline_at_100_Percent_Opportunity",
      "Pipeline_at_100_Percent_Opportunity_ML": "bronze.Pipeline_at_100_Percent_Opportunity_ML",
      "Pipeline_at_100_Percent_Recognized": "bronze.Pipeline_at_100_Percent_Recognized",
      "Yield_Active_Unrecognized": "bronze.Yield_Active_Unrecognized",
      "Yield_Opportunity_ML": "bronze.Yield_Opportunity_ML",
      "Yield_Recognized": "bronze.Yield_Recognized",
      "Run_ID": "bronze.Run_ID",
      "Run_Date": "bronze.Run_Date",
      "Explained_Variance" : "bronze.Explained_Variance",
      "Mean_Absolute_Error": "bronze.Mean_Absolute_Error",
      "Mean_Absolute_Percentage_Error": "bronze.Mean_Absolute_Percentage_Error",
      "Median_Absolute_Error": "bronze.Median_Absolute_Error",
      "Normalized_Mean_Absolute_Error": "bronze.Normalized_Mean_Absolute_Error",
      "Normalized_Median_Absolute_Error": "bronze.Normalized_Median_Absolute_Error",
      "Normalized_Root_Mean_Squared_Error": "bronze.Normalized_Root_Mean_Squared_Error",
      "Normalized_Root_Mean_Squared_Log_Error": "bronze.Normalized_Root_Mean_Squared_Log_Error",
      "R2_Score": "bronze.R2_Score" ,
      "Root_Mean_Squared_Error": "bronze.Root_Mean_Squared_Error", 
      "Root_Mean_Squared_Log_Error": "bronze.Root_Mean_Squared_Log_Error",
      "Spearman_Correlation": "bronze.Spearman_Correlation",
      "Current_Opp_Period_Count": "bronze.Current_Opp_Period_Count",
      "Opportunity_Period_Value": "bronze.Opportunity_Period_Value",
      #audit fields
      "IBIBatchID": "bronze.IBIBatchID",
      #"IBICreatedBy": "bronze.IBICreatedBy",
      "IBIUpdatedBy": "bronze.IBIUpdatedBy",
      #"IBICreatedDate": "bronze.IBICreatedDate",
      "IBIUpdatedDate":"bronze.IBIUpdatedDate",
      "IBIChangeReason": lit("Record has been updated in source system."),
      "IBIOperationType": lit("Update"),
      "IBISourceFormat": "bronze.IBISourceFormat",
      "IBISourceName": "bronze.IBISourceName",
      "IBISourcePath": "bronze.IBISourcePath",
      "IBISourceSystemID": "bronze.IBISourceSystemID",
      "IBIDataCategory": "bronze.IBIDataCategory",
      "IBILineageLevel": "bronze.IBILineageLevel",
      "IBIIngestionMethod": "bronze.IBIIngestionMethod",
      "IBIBatchStartDateTime": "bronze.IBIBatchStartDateTime",
      "IBIDataClassification": "bronze.IBIDataClassification",
      "IBIProgramName": "bronze.IBIProgramName",
      "IBIProgramPath": "bronze.IBIProgramPath",
      "IBIValidationStatus": "bronze.IBIValidationStatus",
      "IBIHashCode":  "bronze.IBIHashCode"
          }
    ).whenNotMatchedInsert(
      #condition = "silver.hashcode <> bronze.hashcode",  
      values = {
        "Forecast_Date": "bronze.Forecast_Date",
        "Snapshot_Date": "bronze.Snapshot_Date",
        "End_of_Month": "bronze.End_of_Month",
        "Sub_Region_Code": "bronze.Sub_Region_Code",
        "Relative_EOM_Snp_Month_Offset": "bronze.Relative_EOM_Snp_Month_Offset",
        "Relative_Month_Offset": "bronze.Relative_Month_Offset",
        "Relative_Snapshot_Month_Offset": "bronze.Relative_Snapshot_Month_Offset",
        "Snapshot_Day_of_Month": "bronze.Snapshot_Day_of_Month",
        "Snp_Seq_No": "bronze.Snp_Seq_No",
        "Currency_Code": "bronze.Currency_Code",
        "Predicted_Revenue": "bronze.Predicted_Revenue",
        "Revenue": "bronze.Revenue",
        "Project_Period_Count": "bronze.Project_Period_Count",
        "Project_Count": "bronze.Project_Count",
        "Project_Period_Price": "bronze.Project_Period_Price",
        "Project_Price": "bronze.Project_Price",
        "Conversions": "bronze.Conversions",
        "Opportunity_Period_Count": "bronze.Opportunity_Period_Count",
        "Opportunity_Count": "bronze.Opportunity_Count",
        "Current_Opp_Period_Value": "bronze.Current_Opp_Period_Value",
        "Opportunity_Value": "bronze.Opportunity_Value",
        "Win_Rate": "bronze.Win_Rate",
        "Nominal_Hours": "bronze.Nominal_Hours",
        "Utilization_Billable": "bronze.Utilization_Billable",
        "Headcount_Billable": "bronze.Headcount_Billable",
        "Headcount_Non_Billable": "bronze.Headcount_Non_Billable",
        "Headcount_Contingent_Billable": "bronze.Headcount_Contingent_Billable",
        "Headcount_Contingent_Non_Billable": "bronze.Headcount_Contingent_Non_Billable",
        "Headcount_Contingent_Unknown": "bronze.Headcount_Contingent_Unknown",
        "EURONEXT_100": "bronze.EURONEXT_100",
        "FTSE_100": "bronze.FTSE_100",
        "Nikkei_225": "bronze.Nikkei_225",
        "SP_500": "bronze.SP_500",
        "SSE_Composite_Index": "bronze.SSE_Composite_Index",
        "Pipeline_Trend": "bronze.Pipeline_Trend",
        "Pipeline_Active_Unrecognized": "bronze.Pipeline_Active_Unrecognized",
        "Pipeline_Opportunity": "bronze.Pipeline_Opportunity",
        "Pipeline_Opportunity_ML": "bronze.Pipeline_Opportunity_ML",
        "Pipeline_Recognized": "bronze.Pipeline_Recognized",
        "Pipeline_at_100_Percent_Active_Unrecognized": "bronze.Pipeline_at_100_Percent_Active_Unrecognized",
        "Pipeline_at_100_Percent_Opportunity": "bronze.Pipeline_at_100_Percent_Opportunity",
        "Pipeline_at_100_Percent_Opportunity_ML": "bronze.Pipeline_at_100_Percent_Opportunity_ML",
        "Pipeline_at_100_Percent_Recognized": "bronze.Pipeline_at_100_Percent_Recognized",
        "Yield_Active_Unrecognized": "bronze.Yield_Active_Unrecognized",
        "Yield_Opportunity_ML": "bronze.Yield_Opportunity_ML",
        "Yield_Recognized": "bronze.Yield_Recognized",
        "Run_ID": "bronze.Run_ID",
        "Run_Date": "bronze.Run_Date",
        "Explained_Variance" : "bronze.Explained_Variance",
        "Mean_Absolute_Error": "bronze.Mean_Absolute_Error",
        "Mean_Absolute_Percentage_Error": "bronze.Mean_Absolute_Percentage_Error",
        "Median_Absolute_Error": "bronze.Median_Absolute_Error",
        "Normalized_Mean_Absolute_Error": "bronze.Normalized_Mean_Absolute_Error",
        "Normalized_Median_Absolute_Error": "bronze.Normalized_Median_Absolute_Error",
        "Normalized_Root_Mean_Squared_Error": "bronze.Normalized_Root_Mean_Squared_Error",
        "Normalized_Root_Mean_Squared_Log_Error": "bronze.Normalized_Root_Mean_Squared_Log_Error",
        "R2_Score": "bronze.R2_Score" ,
        "Root_Mean_Squared_Error": "bronze.Root_Mean_Squared_Error", 
        "Root_Mean_Squared_Log_Error": "bronze.Root_Mean_Squared_Log_Error",
        "Spearman_Correlation": "bronze.Spearman_Correlation",
        "Current_Opp_Period_Count": "bronze.Current_Opp_Period_Count",
        "Opportunity_Period_Value": "bronze.Opportunity_Period_Value",
        #audit fields
        "IBIBatchID": "bronze.IBIBatchID",
        "IBICreatedBy": "bronze.IBICreatedBy",
        "IBIUpdatedBy": "bronze.IBIUpdatedBy",
        "IBICreatedDate": "bronze.IBICreatedDate",
        "IBIUpdatedDate":"bronze.IBIUpdatedDate",
        "IBIChangeReason": lit("New record has been created in source system."),
        "IBIOperationType": lit("Insert"),
        "IBISourceFormat": "bronze.IBISourceFormat",
        "IBISourceName": "bronze.IBISourceName",
        "IBISourcePath": "bronze.IBISourcePath",
        "IBISourceSystemID": "bronze.IBISourceSystemID",
        "IBIDataCategory": "bronze.IBIDataCategory",
        "IBILineageLevel": "bronze.IBILineageLevel",
        "IBIIngestionMethod": "bronze.IBIIngestionMethod",
        "IBIBatchStartDateTime": "bronze.IBIBatchStartDateTime",
        "IBIDataClassification": "bronze.IBIDataClassification",
        "IBIProgramName": "bronze.IBIProgramName",
        "IBIProgramPath": "bronze.IBIProgramPath",
        "IBIValidationStatus": "bronze.IBIValidationStatus",
        "IBIHashCode":  "bronze.IBIHashCode"
      }
    ).execute()
  
except Exception as error: 
  print(error)
  log_error("{} {}".format(notebook, error)) #log error in sentry
  #raise dbutils.notebook.exit(error) #raise the exception
  raise error #raise the exception
 
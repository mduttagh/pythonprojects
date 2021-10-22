# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta_gold_prod_01.automljobruninfo;
# MAGIC CREATE TABLE delta_gold_prod_01.automljobruninfo
# MAGIC (job_name string,
# MAGIC  batch_id bigint,
# MAGIC  batch_start_datetime timestamp,
# MAGIC  start_datetime timestamp, 
# MAGIC  end_datetime timestamp,
# MAGIC  last_updated_datetime timestamp,
# MAGIC  status string
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (job_name)
# MAGIC LOCATION "/mnt/prod/gold/automljobruninfo"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE delta_bronze_dev_01.b_automl_fact_revenue_forecast;
# MAGIC -- TRUNCATE TABLE delta_silver_dev_01.s_automl_fact_revenue_forecast;
# MAGIC -- TRUNCATE TABLE delta_gold_dev_01.g_automl_fact_revenue_forecast;
# MAGIC -- TRUNCATE TABLE delta_platinum_dev_01.p_automl_fact_revenue_forecast;

# COMMAND ----------

# MAGIC %sql --utilization type dimension   # null for old data.
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS delta_bronze_dev_01.b_automl_fact_revenue_forecast;
# MAGIC -- DROP TABLE IF EXISTS delta_silver_dev_01.s_automl_fact_revenue_forecast;
# MAGIC -- DROP TABLE IF EXISTS delta_gold_dev_01.g_automl_fact_revenue_forecast;
# MAGIC -- DROP TABLE IF EXISTS delta_platinum_dev_01.p_automl_fact_revenue_forecast;
# MAGIC 
# MAGIC CREATE TABLE delta_bronze_dev_01.b_automl_fact_revenue_forecast
# MAGIC (
# MAGIC     Forecast_Date DATE,
# MAGIC     Snapshot_Date DATE,
# MAGIC     End_of_Month DATE,
# MAGIC     Sub_Region_Code string,
# MAGIC     Relative_Month_Offset int,
# MAGIC     Relative_Offset int,
# MAGIC     Relative_Snapshot_Month_Offset int,
# MAGIC     Snapshot_Day_of_Month int,
# MAGIC     Snp_Seq_No int,
# MAGIC     Currency_Code string,
# MAGIC     Predicted_Revenue numeric(32,6),
# MAGIC     Revenue numeric(32,6),
# MAGIC     Project_Period_Count int, 
# MAGIC     Project_Count int ,
# MAGIC     Project_Period_Price numeric(32,6),
# MAGIC     Project_Price numeric(32,6),
# MAGIC     Conversions numeric(32,6),
# MAGIC     Opportunity_Period_Count int, 
# MAGIC     Opportunity_Count int, 
# MAGIC     Current_Opp_Period_Value numeric(32,6),
# MAGIC     Opportunity_Value numeric(32,6),
# MAGIC     Win_Rate numeric(32,6),
# MAGIC     Nominal_Hours numeric(32,6),
# MAGIC     Utilization_Billable numeric(32,6),
# MAGIC     Headcount_Billable numeric(32,6),
# MAGIC     Headcount_Non_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Non_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Unknown numeric(32,6),
# MAGIC     EURONEXT_100 numeric(32,6),
# MAGIC     FTSE_100 numeric(32,6),
# MAGIC     Nikkei_225 numeric(32,6),
# MAGIC     SP_500 numeric(32,6),
# MAGIC     SSE_Composite_Index numeric(32,6),
# MAGIC     Pipeline_Trend numeric(32,6),
# MAGIC     Pipeline_Active_Unrecognized numeric(32,6),
# MAGIC     Pipeline_Opportunity numeric(32,6),
# MAGIC     Pipeline_Opportunity_ML numeric(32,6),
# MAGIC     Pipeline_Recognized numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Active_Unrecognized numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Opportunity numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Opportunity_ML numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Recognized numeric(32,6),
# MAGIC     Yield_Active_Unrecognized numeric(32,6),
# MAGIC     Yield_Opportunity_ML numeric(32,6),
# MAGIC     Yield_Recognized numeric(32,6),
# MAGIC     Run_Date DATE,
# MAGIC     Run_ID string,
# MAGIC     IBIBatchID bigint,
# MAGIC     IBIBatchStartDateTime timestamp,
# MAGIC     IBIHashCode string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/dev/bronze/b_automl_fact_revenue_forecast";
# MAGIC 
# MAGIC CREATE TABLE delta_silver_dev_01.s_automl_fact_revenue_forecast
# MAGIC (
# MAGIC     Forecast_Date DATE,
# MAGIC     Snapshot_Date DATE,
# MAGIC     End_of_Month DATE,
# MAGIC     Sub_Region_Code string,
# MAGIC     Relative_Month_Offset int,
# MAGIC     Relative_Offset int,
# MAGIC     Relative_Snapshot_Month_Offset int,
# MAGIC     Snapshot_Day_of_Month int,
# MAGIC     Snp_Seq_No int,
# MAGIC     Currency_Code string,
# MAGIC     Predicted_Revenue numeric(32,6),
# MAGIC     Revenue numeric(32,6),
# MAGIC     Project_Period_Count int, 
# MAGIC     Project_Count int ,
# MAGIC     Project_Period_Price numeric(32,6),
# MAGIC     Project_Price numeric(32,6),
# MAGIC     Conversions numeric(32,6),
# MAGIC     Opportunity_Period_Count int, 
# MAGIC     Opportunity_Count int, 
# MAGIC     Current_Opp_Period_Value numeric(32,6),
# MAGIC     Opportunity_Value numeric(32,6),
# MAGIC     Win_Rate numeric(32,6),
# MAGIC     Nominal_Hours numeric(32,6),
# MAGIC     Utilization_Billable numeric(32,6),
# MAGIC     Headcount_Billable numeric(32,6),
# MAGIC     Headcount_Non_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Non_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Unknown numeric(32,6),
# MAGIC     EURONEXT_100 numeric(32,6),
# MAGIC     FTSE_100 numeric(32,6),
# MAGIC     Nikkei_225 numeric(32,6),
# MAGIC     SP_500 numeric(32,6),
# MAGIC     SSE_Composite_Index numeric(32,6),
# MAGIC     Pipeline_Trend numeric(32,6),
# MAGIC     Pipeline_Active_Unrecognized numeric(32,6),
# MAGIC     Pipeline_Opportunity numeric(32,6),
# MAGIC     Pipeline_Opportunity_ML numeric(32,6),
# MAGIC     Pipeline_Recognized numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Active_Unrecognized numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Opportunity numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Opportunity_ML numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Recognized numeric(32,6),
# MAGIC     Yield_Active_Unrecognized numeric(32,6),
# MAGIC     Yield_Opportunity_ML numeric(32,6),
# MAGIC     Yield_Recognized numeric(32,6),
# MAGIC     Run_Date DATE,
# MAGIC     Run_ID string,
# MAGIC     IBIBatchID	bigint,
# MAGIC     IBICreatedBy	string,
# MAGIC     IBIUpdatedBy	string,
# MAGIC     IBICreatedDate	timestamp,
# MAGIC     IBIUpdatedDate	timestamp,
# MAGIC     IBIChangeReason	string,
# MAGIC     IBIOperationType	string,
# MAGIC     IBISourceFormat	string,
# MAGIC     IBISourceName	string,
# MAGIC     IBISourcePath	string,
# MAGIC     IBISourceSystemID	string,
# MAGIC     IBIDataCategory	string,
# MAGIC     IBILineageLevel	int,
# MAGIC     IBIIngestionMethod	string,
# MAGIC     IBIBatchStartDateTime	timestamp,
# MAGIC     IBIDataClassification	string,
# MAGIC     IBIProgramName	string,
# MAGIC     IBIProgramPath	string,
# MAGIC     IBIValidationStatus	string,
# MAGIC     IBIHashCode	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/dev/silver/s_automl_fact_revenue_forecast";
# MAGIC 
# MAGIC CREATE TABLE delta_gold_dev_01.g_automl_fact_revenue_forecast
# MAGIC (
# MAGIC     RevenueForecastKey int,
# MAGIC     ValidFrom DATE,
# MAGIC     ValidTo DATE,
# MAGIC     Forecast_Date DATE,
# MAGIC     Snapshot_Date DATE,
# MAGIC     End_of_Month DATE,
# MAGIC     Sub_Region_Code string,
# MAGIC     Relative_Month_Offset int,
# MAGIC     Relative_Offset int,
# MAGIC     Relative_Snapshot_Month_Offset int,
# MAGIC     Snapshot_Day_of_Month int,
# MAGIC     Snp_Seq_No int,
# MAGIC     Currency_Code string,
# MAGIC     Predicted_Revenue numeric(32,6),
# MAGIC     Revenue numeric(32,6),
# MAGIC     Project_Period_Count int, 
# MAGIC     Project_Count int ,
# MAGIC     Project_Period_Price numeric(32,6),
# MAGIC     Project_Price numeric(32,6),
# MAGIC     Conversions numeric(32,6),
# MAGIC     Opportunity_Period_Count int, 
# MAGIC     Opportunity_Count int, 
# MAGIC     Current_Opp_Period_Value numeric(32,6),
# MAGIC     Opportunity_Value numeric(32,6),
# MAGIC     Win_Rate numeric(32,6),
# MAGIC     Nominal_Hours numeric(32,6),
# MAGIC     Utilization_Billable numeric(32,6),
# MAGIC     Headcount_Billable numeric(32,6),
# MAGIC     Headcount_Non_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Non_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Unknown numeric(32,6),
# MAGIC     EURONEXT_100 numeric(32,6),
# MAGIC     FTSE_100 numeric(32,6),
# MAGIC     Nikkei_225 numeric(32,6),
# MAGIC     SP_500 numeric(32,6),
# MAGIC     SSE_Composite_Index numeric(32,6),
# MAGIC     Pipeline_Trend numeric(32,6),
# MAGIC     Pipeline_Active_Unrecognized numeric(32,6),
# MAGIC     Pipeline_Opportunity numeric(32,6),
# MAGIC     Pipeline_Opportunity_ML numeric(32,6),
# MAGIC     Pipeline_Recognized numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Active_Unrecognized numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Opportunity numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Opportunity_ML numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Recognized numeric(32,6),
# MAGIC     Yield_Active_Unrecognized numeric(32,6),
# MAGIC     Yield_Opportunity_ML numeric(32,6),
# MAGIC     Yield_Recognized numeric(32,6),
# MAGIC     Run_Date DATE,
# MAGIC     Run_ID string,
# MAGIC     IBIBatchID	bigint,
# MAGIC     IBICreatedBy	string,
# MAGIC     IBIUpdatedBy	string,
# MAGIC     IBICreatedDate	timestamp,
# MAGIC     IBIUpdatedDate	timestamp,
# MAGIC     IBIChangeReason	string,
# MAGIC     IBIOperationType	string,
# MAGIC     IBISourceFormat	string,
# MAGIC     IBISourceName	string,
# MAGIC     IBISourcePath	string,
# MAGIC     IBISourceSystemID	string,
# MAGIC     IBIDataCategory	string,
# MAGIC     IBILineageLevel	int,
# MAGIC     IBIIngestionMethod	string,
# MAGIC     IBIBatchStartDateTime	timestamp,
# MAGIC     IBIDataClassification	string,
# MAGIC     IBIProgramName	string,
# MAGIC     IBIProgramPath	string,
# MAGIC     IBIValidationStatus	string,
# MAGIC     IBIHashCode	string,
# MAGIC     IsLatest boolean,
# MAGIC     IsDeleted boolean
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/dev/gold/g_automl_fact_revenue_forecast";
# MAGIC 
# MAGIC CREATE TABLE delta_platinum_dev_01.p_automl_fact_revenue_forecast
# MAGIC (
# MAGIC     RevenueForecastKey int,
# MAGIC     ValidFrom DATE,
# MAGIC     ValidTo DATE,
# MAGIC     ValidFromDateKey int,
# MAGIC     ValidToDateKey int,
# MAGIC     CreatedDateKey int,
# MAGIC     UpdatedDateKey int,
# MAGIC     BatchStartDateKey int,
# MAGIC     ForecastDateKey int,
# MAGIC     SnapshotDateKey int,
# MAGIC     EndOfMonthDateKey int,
# MAGIC     RunDateKey int,
# MAGIC     CurrencyKey int, 
# MAGIC     Forecast_Date DATE,
# MAGIC     Snapshot_Date DATE,
# MAGIC     End_of_Month DATE,
# MAGIC     Sub_Region_Code string,
# MAGIC     Relative_Month_Offset int,
# MAGIC     Relative_Offset int,
# MAGIC     Relative_Snapshot_Month_Offset int,
# MAGIC     Snapshot_Day_of_Month int,
# MAGIC     Snp_Seq_No int,
# MAGIC     Currency_Code string,
# MAGIC     Predicted_Revenue numeric(32,6),
# MAGIC     Revenue numeric(32,6),
# MAGIC     Project_Period_Count int, 
# MAGIC     Project_Count int ,
# MAGIC     Project_Period_Price numeric(32,6),
# MAGIC     Project_Price numeric(32,6),
# MAGIC     Conversions numeric(32,6),
# MAGIC     Opportunity_Period_Count int, 
# MAGIC     Opportunity_Count int, 
# MAGIC     Current_Opp_Period_Value numeric(32,6),
# MAGIC     Opportunity_Value numeric(32,6),
# MAGIC     Win_Rate numeric(32,6),
# MAGIC     Nominal_Hours numeric(32,6),
# MAGIC     Utilization_Billable numeric(32,6),
# MAGIC     Headcount_Billable numeric(32,6),
# MAGIC     Headcount_Non_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Non_Billable numeric(32,6),
# MAGIC     Headcount_Contingent_Unknown numeric(32,6),
# MAGIC     EURONEXT_100 numeric(32,6),
# MAGIC     FTSE_100 numeric(32,6),
# MAGIC     Nikkei_225 numeric(32,6),
# MAGIC     SP_500 numeric(32,6),
# MAGIC     SSE_Composite_Index numeric(32,6),
# MAGIC     Pipeline_Trend numeric(32,6),
# MAGIC     Pipeline_Active_Unrecognized numeric(32,6),
# MAGIC     Pipeline_Opportunity numeric(32,6),
# MAGIC     Pipeline_Opportunity_ML numeric(32,6),
# MAGIC     Pipeline_Recognized numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Active_Unrecognized numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Opportunity numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Opportunity_ML numeric(32,6),
# MAGIC     Pipeline_at_100_Percent_Recognized numeric(32,6),
# MAGIC     Yield_Active_Unrecognized numeric(32,6),
# MAGIC     Yield_Opportunity_ML numeric(32,6),
# MAGIC     Yield_Recognized numeric(32,6),  
# MAGIC     Run_Date DATE,
# MAGIC     Run_ID string,
# MAGIC     IBIBatchID	bigint,
# MAGIC     IBICreatedBy	string,
# MAGIC     IBIUpdatedBy	string,
# MAGIC     IBICreatedDate	timestamp,
# MAGIC     IBIUpdatedDate	timestamp,
# MAGIC     IBIChangeReason	string,
# MAGIC     IBIOperationType	string,
# MAGIC     IBISourceFormat	string,
# MAGIC     IBISourceName	string,
# MAGIC     IBISourcePath	string,
# MAGIC     IBISourceSystemID	string,
# MAGIC     IBIDataCategory	string,
# MAGIC     IBILineageLevel	int,
# MAGIC     IBIIngestionMethod	string,
# MAGIC     IBIBatchStartDateTime	timestamp,
# MAGIC     IBIDataClassification	string,
# MAGIC     IBIProgramName	string,
# MAGIC     IBIProgramPath	string,
# MAGIC     IBIValidationStatus	string,
# MAGIC     IBIHashCode	string,
# MAGIC     IsLatest boolean,
# MAGIC     IsDeleted boolean
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/dev/platinum/p_automl_fact_revenue_forecast";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- migrate to prod - 2021-06-16
# MAGIC ALTER TABLE delta_bronze_dev_01.b_automl_fact_revenue_forecast
# MAGIC ADD COLUMNS (Explained_Variance numeric(32,6), 
# MAGIC              Mean_Absolute_Error numeric(32,6),
# MAGIC              Mean_Absolute_Percentage_Error numeric(32,6),
# MAGIC              Median_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Mean_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Median_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Root_Mean_Squared_Error numeric(32,6),
# MAGIC              Normalized_Root_Mean_Squared_Log_Error numeric(32,6),
# MAGIC              R2_Score numeric(32,6),
# MAGIC              Root_Mean_Squared_Error numeric(32,6),
# MAGIC              Root_Mean_Squared_Log_Error numeric(32,6),
# MAGIC              Spearman_Correlation numeric(32,6));
# MAGIC              
# MAGIC ALTER TABLE delta_silver_dev_01.s_automl_fact_revenue_forecast
# MAGIC ADD COLUMNS (Explained_Variance numeric(32,6), 
# MAGIC              Mean_Absolute_Error numeric(32,6),
# MAGIC              Mean_Absolute_Percentage_Error numeric(32,6),
# MAGIC              Median_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Mean_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Median_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Root_Mean_Squared_Error numeric(32,6),
# MAGIC              Normalized_Root_Mean_Squared_Log_Error numeric(32,6),
# MAGIC              R2_Score numeric(32,6),
# MAGIC              Root_Mean_Squared_Error numeric(32,6),
# MAGIC              Root_Mean_Squared_Log_Error numeric(32,6),
# MAGIC              Spearman_Correlation numeric(32,6));
# MAGIC              
# MAGIC ALTER TABLE delta_gold_dev_01.g_automl_fact_revenue_forecast
# MAGIC ADD COLUMNS (Explained_Variance numeric(32,6), 
# MAGIC              Mean_Absolute_Error numeric(32,6),
# MAGIC              Mean_Absolute_Percentage_Error numeric(32,6),
# MAGIC              Median_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Mean_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Median_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Root_Mean_Squared_Error numeric(32,6),
# MAGIC              Normalized_Root_Mean_Squared_Log_Error numeric(32,6),
# MAGIC              R2_Score numeric(32,6),
# MAGIC              Root_Mean_Squared_Error numeric(32,6),
# MAGIC              Root_Mean_Squared_Log_Error numeric(32,6),
# MAGIC              Spearman_Correlation numeric(32,6));
# MAGIC              
# MAGIC ALTER TABLE delta_platinum_dev_01.p_automl_fact_revenue_forecast
# MAGIC ADD COLUMNS (Explained_Variance numeric(32,6), 
# MAGIC              Mean_Absolute_Error numeric(32,6),
# MAGIC              Mean_Absolute_Percentage_Error numeric(32,6),
# MAGIC              Median_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Mean_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Median_Absolute_Error numeric(32,6),
# MAGIC              Normalized_Root_Mean_Squared_Error numeric(32,6),
# MAGIC              Normalized_Root_Mean_Squared_Log_Error numeric(32,6),
# MAGIC              R2_Score numeric(32,6),
# MAGIC              Root_Mean_Squared_Error numeric(32,6),
# MAGIC              Root_Mean_Squared_Log_Error numeric(32,6),
# MAGIC              Spearman_Correlation numeric(32,6));             

# COMMAND ----------

# MAGIC %sql
# MAGIC -- migrate to prod - 2021-06-16
# MAGIC ALTER TABLE delta_bronze_dev_01.b_automl_fact_revenue_forecast
# MAGIC ADD COLUMNS (Current_Opp_Period_Count int, 
# MAGIC              Opportunity_Period_Value numeric(32,6));
# MAGIC              
# MAGIC ALTER TABLE delta_silver_dev_01.s_automl_fact_revenue_forecast
# MAGIC ADD COLUMNS (Current_Opp_Period_Count int, 
# MAGIC              Opportunity_Period_Value numeric(32,6));
# MAGIC              
# MAGIC ALTER TABLE delta_gold_dev_01.g_automl_fact_revenue_forecast
# MAGIC ADD COLUMNS (Current_Opp_Period_Count int, 
# MAGIC              Opportunity_Period_Value numeric(32,6));
# MAGIC              
# MAGIC ALTER TABLE delta_platinum_dev_01.p_automl_fact_revenue_forecast
# MAGIC ADD COLUMNS (Current_Opp_Period_Count int, 
# MAGIC              Opportunity_Period_Value numeric(32,6));

# COMMAND ----------

# migrate to prod - 2021-06-16
from delta.tables import *
table_name = "delta_bronze_dev_01.b_automl_fact_revenue_forecast"
spark.read.table(table_name)\
  .withColumnRenamed("Relative_Offset", "Relative_EOM_Snp_Month_Offset")\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .saveAsTable(table_name)

table_name = "delta_silver_dev_01.s_automl_fact_revenue_forecast"
spark.read.table(table_name)\
  .withColumnRenamed("Relative_Offset", "Relative_EOM_Snp_Month_Offset")\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .saveAsTable(table_name)

table_name = "delta_gold_dev_01.g_automl_fact_revenue_forecast"
spark.read.table(table_name)\
  .withColumnRenamed("Relative_Offset", "Relative_EOM_Snp_Month_Offset")\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .saveAsTable(table_name)

table_name = "delta_platinum_dev_01.p_automl_fact_revenue_forecast"
spark.read.table(table_name)\
  .withColumnRenamed("Relative_Offset", "Relative_EOM_Snp_Month_Offset")\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .saveAsTable(table_name)


# COMMAND ----------


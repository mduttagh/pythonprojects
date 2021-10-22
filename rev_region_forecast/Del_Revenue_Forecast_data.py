# Databricks notebook source
# MAGIC %sql
# MAGIC -- select distinct 1,forecast_date from delta_silver_prod_01.s_automl_fact_revenue_forecast
# MAGIC -- union all 
# MAGIC -- select distinct 2,forecast_date  from delta_gold_prod_01.g_automl_fact_revenue_forecast 
# MAGIC -- union all 
# MAGIC -- select distinct 3,forecast_date from delta_platinum_prod_01.p_automl_fact_revenue_forecast 
# MAGIC -- order by 1,2
# MAGIC 
# MAGIC -- select distinct 1,  forecast_date from delta_silver_prod_01.s_automl_fact_revenue_forecast where forecast_date like '2021-04%' or forecast_date in ('2021-09-20','2021-09-26')
# MAGIC -- union all 
# MAGIC -- select distinct 2,  forecast_date  from delta_gold_prod_01.g_automl_fact_revenue_forecast where forecast_date like '2021-04%' or forecast_date in ('2021-09-20','2021-09-26')
# MAGIC -- union all 
# MAGIC -- select distinct 3,  forecast_date from delta_platinum_prod_01.p_automl_fact_revenue_forecast where forecast_date like '2021-04%' or forecast_date in ('2021-09-20','2021-09-26')
# MAGIC -- order by 1,2
# MAGIC 
# MAGIC -- delete from delta_silver_prod_01.s_automl_fact_revenue_forecast where forecast_date like '2021-04%' or forecast_date in ('2021-09-20','2021-09-26');
# MAGIC -- delete from delta_gold_prod_01.g_automl_fact_revenue_forecast where forecast_date like '2021-04%' or forecast_date in ('2021-09-20','2021-09-26');
# MAGIC -- delete from delta_platinum_prod_01.p_automl_fact_revenue_forecast where forecast_date like '2021-04%' or forecast_date in ('2021-09-20','2021-09-26');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct 1,  count(forecast_date) from delta_silver_prod_01.s_automl_fact_revenue_forecast
# MAGIC -- union all 
# MAGIC -- select distinct 2,  count(forecast_date)  from delta_gold_prod_01.g_automl_fact_revenue_forecast 
# MAGIC -- union all 
# MAGIC -- select distinct 3,  count(forecast_date) from delta_platinum_prod_01.p_automl_fact_revenue_forecast 
# MAGIC -- order by 1,2
# MAGIC 
# MAGIC -- select distinct 1,  count(forecast_date) from delta_silver_prod_01.s_automl_fact_revenue_forecast where forecast_date  in ('2021-09-02','2021-09-28')
# MAGIC -- union all 
# MAGIC -- select distinct 2,  count(forecast_date)  from delta_gold_prod_01.g_automl_fact_revenue_forecast where forecast_date in ('2021-09-02','2021-09-28')
# MAGIC -- union all 
# MAGIC -- select distinct 3,  count(forecast_date) from delta_platinum_prod_01.p_automl_fact_revenue_forecast where forecast_date in ('2021-09-02','2021-09-28')
# MAGIC -- order by 1,2
# MAGIC 
# MAGIC -- delete from delta_silver_prod_01.s_automl_fact_revenue_forecast where forecast_date  in ('2021-09-02','2021-09-28');
# MAGIC -- delete from delta_gold_prod_01.g_automl_fact_revenue_forecast where forecast_date  in ('2021-09-02','2021-09-28');
# MAGIC -- delete from delta_platinum_prod_01.p_automl_fact_revenue_forecast where forecast_date  in ('2021-09-02','2021-09-28');
# MAGIC 
# MAGIC -- select distinct forecast_date from delta_platinum_prod_01.p_automl_fact_revenue_forecast
# MAGIC -- order by 1 desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 1,  count(forecast_date) from delta_silver_prod_01.s_automl_fact_revenue_forecast
# MAGIC union all 
# MAGIC select distinct 2,  count(forecast_date)  from delta_gold_prod_01.g_automl_fact_revenue_forecast 
# MAGIC union all 
# MAGIC select distinct 3,  count(forecast_date) from delta_platinum_prod_01.p_automl_fact_revenue_forecast 
# MAGIC order by 1,2
# MAGIC 
# MAGIC -- select distinct 1,  count(forecast_date) from delta_silver_prod_01.s_automl_fact_revenue_forecast where forecast_date in ('2021-10-14')
# MAGIC -- union all 
# MAGIC -- select distinct 2,  count(forecast_date)  from delta_gold_prod_01.g_automl_fact_revenue_forecast where forecast_date in ('2021-10-14')
# MAGIC -- union all 
# MAGIC -- select distinct 3,  count(forecast_date) from delta_platinum_prod_01.p_automl_fact_revenue_forecast where forecast_date in ('2021-10-14')
# MAGIC -- order by 1,2
# MAGIC 
# MAGIC -- delete from delta_silver_prod_01.s_automl_fact_revenue_forecast where forecast_date  in ('2021-10-14');
# MAGIC -- delete from delta_gold_prod_01.g_automl_fact_revenue_forecast where forecast_date  in ('2021-10-14');
# MAGIC -- delete from delta_platinum_prod_01.p_automl_fact_revenue_forecast where forecast_date  in ('2021-10-14');
# MAGIC 
# MAGIC -- select distinct forecast_date from delta_platinum_prod_01.p_automl_fact_revenue_forecast
# MAGIC -- order by 1 desc  

# COMMAND ----------


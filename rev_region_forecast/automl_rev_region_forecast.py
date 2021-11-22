# Databricks notebook source
# DBTITLE 1,Setup
# MAGIC %pip install --upgrade --force-reinstall -r https://aka.ms/automl_linux_requirements.txt

# COMMAND ----------

#%pip install --upgrade azureml-automl-core sentry_sdk stringcase pandas pandas_datareader yfinance plotly # Story No. 3018 modified Mukesh Dutta 9/3/2021 

# COMMAND ----------

import azureml.core

print("SDK Version:", azureml.core.VERSION)

# COMMAND ----------

# MAGIC %run ../../bi_config/pbi_common

# COMMAND ----------

# DBTITLE 1,Define widgets
# define widgets - NEED TO DEFINE IT ONCE
# dynamic variables (pass it from ADF)
# first time runtime parameter
# dbutils.widgets.dropdown("environment", "dev", ["dev","uat","prod"])
# dbutils.widgets.dropdown("new_training", "False", ["True","False"])
# dbutils.widgets.dropdown("system_name", "bimodelapi", ["bimodelapi"])
# dbutils.widgets.dropdown("data_load_type", "Incremental", ["Full","Incremental"])
# dbutils.widgets.text("system_name", "","")
# dbutils.widgets.text("runid", "","")
# dbutils.widgets.remove("pbiapi")
environment = dbutils.widgets.get("environment")
new_training = dbutils.widgets.get("new_training")
system_name = dbutils.widgets.get("system_name")
data_load_type = dbutils.widgets.get("data_load_type") # Full/Incremental
runid = dbutils.widgets.get("runid")
if environment in {"prod"}:
    aml_compute_cluster_name = "cc-bi-ml-prod01"
else:
    aml_compute_cluster_name = "cc-bi-ml-devqa01"

print(environment, system_name, new_training, aml_compute_cluster_name, data_load_type, runid)


# COMMAND ----------

# DBTITLE 1,Data load and prep
# MAGIC %run ./data_prep_rev_region_forecast

# COMMAND ----------

# DBTITLE 1,Compute
# Import the Workspace class and configure your local envionment
from azureml.core import Workspace

# from azureml.core.authentication import InteractiveLoginAuthentication
import os
from azureml.core.authentication import ServicePrincipalAuthentication

tenant_id = "cfa930ff-a3f0-4933-8cea-744f8ead7682"

if environment == "prod":
    svc_pr_password = dbutils.secrets.get(
        scope="kv-bi-prod-01-secrets", key="dbw-azureml-prod-key"
    )
    service_principal_id = "23bd5a19-b0e0-44af-9320-24b578ec0f74"
else:
    svc_pr_password = dbutils.secrets.get(
        scope="kv-bi-devqa-01-secrets", key="dbw-azureml-devqa-key"
    )
    service_principal_id = "626c2c00-26de-46ee-a20d-6027445518ff"
    


svc_pr = ServicePrincipalAuthentication(
    tenant_id=tenant_id,
    service_principal_id=service_principal_id,
    service_principal_password=svc_pr_password,
)

# Create Workspace if required
"""
from azureml.core import Workspace

ws = Workspace.create(name = workspace_name,
                      subscription_id = subscription_id,
                      resource_group = resource_group, 
                      location = workspace_region,                      
                      exist_ok=True)
ws.get_details()

"""

try:
    ws = Workspace(
        workspace_name=workspace_name,
        subscription_id=subscription_id,
        resource_group=resource_group,
        auth=svc_pr,  # forced_interactive_auth
    )

    # Persist the subscription id, resource group name, and workspace name in aml_config/config.json.
    ws.write_config()
    print(ws)
    print("Found workspace {} at location {}".format(ws.name, ws.location))
    
    print("workspace_name",workspace_name)
    print("subscription_id",subscription_id)
    print("resource_group",resource_group)
    print("svc_pr",svc_pr)
    print("service_principal_id",service_principal_id)
    print("svc_pr_password",svc_pr_password)

    # ws.write_config(path="./aml_config/",file_name="ws_config.json")
    # use the get method to load an existing workspace without using configuration files.
except Exception as error:
    print(error)
    log_error("{} {}".format(notebook, error)) #log error in sentry
    #raise dbutils.notebook.exit(error) #raise the exception
    raise error #raise the exception


# COMMAND ----------

# Create an experiement
import os
import random
import time
import json

from matplotlib import pyplot as plt
from matplotlib.pyplot import imshow

import azureml.core
import pandas as pd
import numpy as np
import logging

from azureml.core.workspace import Workspace
from azureml.core.experiment import Experiment
from azureml.train.automl import AutoMLConfig
from azureml.train.automl.run import AutoMLRun
from azureml.automl.core.featurization import FeaturizationConfig

# https://github.com/Azure/MachineLearningNotebooks/blob/master/how-to-use-azureml/automated-machine-learning/forecasting-orange-juice-sales/auto-ml-forecasting-orange-juice-sales.ipynb
# Choose a name for the experiment and specify the project folder.
experiment_name = "automl-revenue-region-forecast"

try:

    experiment = Experiment(ws, experiment_name)

    output = {}
    output["SDK version"] = azureml.core.VERSION
    output["Subscription ID"] = ws.subscription_id
    output["Workspace Name"] = ws.name
    output["Resource Group"] = ws.resource_group
    output["Location"] = ws.location
    output["Experiment Name"] = experiment.name
    pd.set_option("display.max_colwidth", -1)
    print(pd.DataFrame(data=output, index=[""]).T)
except Exception as error:
    print(error)
    log_error("{} {}".format(notebook, error)) #log error in sentry
    raise dbutils.notebook.exit(error) #raise the exception
    
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException

# Choose a name for your CPU clustera
try:
    amlcompute_cluster_name = aml_compute_cluster_name

    compute_target = ComputeTarget(workspace=ws, name=amlcompute_cluster_name)
    print("Found existing cluster, use it:", amlcompute_cluster_name)
except Exception as error:
    print(error)
    log_error("{} {}".format(notebook, error)) #log error in sentry
    #raise dbutils.notebook.exit(error) #raise the exception
    raise error #raise the exception

# COMMAND ----------

# DBTITLE 1,Data splitting
# Data Split
# split the data into a training and a testing set for later forecast evaluation. The test set will contain the final test_size months of observed sales for each time-series.
# The splits should be stratified by series, so we use a group-by statement on the time series identifier columns.
df = merge_final.copy()
nseries = df.groupby(time_series_id_column_names).ngroups # MD Change 11/17/2021
print("Data contains {0} individual time-series.".format(nseries)) # MD Change 11/17/2021
    
def split_last_n_by_series_id1(df, n):
    """Group df by series identifiers and split on last n rows for each group."""
    df_grouped = df.sort_values(by=final_sort_order,ascending=final_sort_order_ascending).groupby(  # Sort by ascending time
        time_series_id_column_names, group_keys=False
    )
    df_head = df_grouped.apply(lambda dfg: dfg.iloc[:-n])
    df_tail = df_grouped.apply(lambda dfg: dfg.iloc[-n:])
    return df_head, df_tail

#train = df.query("Relative_Month_Offset < 0") # MD Change 11/17/2021
#test = df.drop(train.index) # MD Change 11/17/2021

(train, test) = split_last_n_by_series_id1(df, n_test_periods) # MD Change 11/17/2021

print("df: ", df.shape) 
print("train: ", train.shape)
print("test: ", test.shape)

train.to_csv(output_data_path + "revregionforecast_train.csv", index=None, header=True)
test.to_csv(output_data_path + "revregionforecast_test.csv", index=None, header=True)

# COMMAND ----------

# Summary Stats for Train and Test dataframes

print("Train************************")
#print(train.info(verbose=True))
#train.describe(include="all").transpose().head() pipehist1 #Story No. 3404
#train.tail(10)


# COMMAND ----------

print("Test************************")
#print(test.info(verbose=True))
#test.describe(include="all").transpose().head() # Story No. 3404
#test.tail(10)


# COMMAND ----------

# plot the example time series
#!pip install seaborn
"""
import seaborn as sns
import matplotlib.pyplot as plt

pd.set_option('display.float_format', lambda x: '%.4f' % x)

# sns.set_context('paper', font_scale=1.3)
# sns.set_style('white')

(fig, ax) = plt.subplots(figsize=(24, 9))
import matplotlib.pyplot as plt
whole_data = train.copy()
target_label = target_column_name
whole_data[target_label] = train[target_column_name]
for g in whole_data.groupby(time_series_id_column_names):
    plt.plot(g[1][time_column_name].values,
             g[1][target_column_name].values, label=g[0])
plt.legend()
#plt.show()

# plot the example time series

(fig, ax) = plt.subplots(figsize=(24, 9))
import matplotlib.pyplot as plt
whole_data = test.copy()
target_label = target_column_name
whole_data[target_label] = test[target_column_name]
for g in whole_data.groupby(time_series_id_column_names):
    plt.plot(g[1][time_column_name].values,
             g[1][target_column_name].values, label=g[0])
plt.legend()
#plt.show()
"""

# COMMAND ----------

# Plot Revenue
"""
import altair as alt
alt.data_transformers.disable_max_rows()

# alt.themes.enable('fivethirtyeight') #default, latimes, ggplot2, fivethirtyeight, urbaninstitute

theme = alt.themes.enable("fivethirtyeight")
theme.fontsize = 14
fontsize = 12
h = 450
w = 1200

source = train.copy()

alt.Chart(source).mark_line().encode(
    x=alt.X('End_of_Month:T'),
    y=alt.Y('average(Revenue):Q'),
    #color=alt.Color('Relative_EOM_Snp_Month_Offset:N'),
    #strokeDash='Snp_Seq_No:N',
    facet=alt.Facet('Relative_EOM_Snp_Month_Offset:N', columns=3)
).properties(
    height=h,
    width=w
)
"""

# COMMAND ----------

# DBTITLE 1,Upload data to datastore
# Create dataset for training

from azureml.core import Dataset, Datastore

datastore = ws.get_default_datastore()
print("Default datastore's name: {}".format(datastore.name))

# register_spark_dataframe(dataframe=train_sdf, target=blobstore_datadir, name="revforecast_train.parquet", description=None, tags=None, show_progress=True)

datastore.upload_files(
    files=[
        output_data_path + "revregionforecast_train.csv",
        output_data_path + "revregionforecast_test.csv",
    ],
    target_path=blobstore_datadir,
    overwrite=True,
    show_progress=True,
)

dataset_filename = blobstore_datadir + "revregionforecast_train.csv"
train_dataset = Dataset.Tabular.from_delimited_files(
    path=[(datastore, dataset_filename)]
)

#display(train_dataset.to_pandas_dataframe().head())
#display(train_dataset.to_pandas_dataframe().tail()) # Story No. 3404


# COMMAND ----------

# DBTITLE 1,Forecasting parameters
# Set forecasting_parameters for training

from azureml.automl.core.forecasting_parameters import ForecastingParameters

target_lag = [x for x in range(1,7)] # MD Change 11/17/2021  "auto" # 
window_size = "auto" 
feature_lag = "auto"
forecast_horizon = n_test_periods # MD Change 11/17/2021
seasonality = "auto" 

print("past_period", past_period)
print("n_test_periods", n_test_periods)
print("target_lags:", target_lag)
print("target_rolling_window_size:", window_size)
print("forecast_horizon:", forecast_horizon)
print("feature_lags:", feature_lag)
print("seasonality:", seasonality) 

forecasting_parameters = ForecastingParameters(
    time_column_name=time_column_name,
    forecast_horizon=forecast_horizon,
    time_series_id_column_names=time_series_id_column_names,
    target_lags=target_lag,
    feature_lags=feature_lag,
    target_rolling_window_size=window_size, 
    seasonality=seasonality#, 
)

automl_config = AutoMLConfig(  # featurization_config,
    task="forecasting",
    debug_log="rev_region_forecast_errors.log",
    primary_metric="normalized_root_mean_squared_error",
    experiment_timeout_hours=4,
    training_data=train_dataset,
    label_column_name=target_column_name,
    enable_early_stopping=False,
    #spark_context=sc, #enable this for databricks cluster
    compute_target=compute_target,  # enable this for ml cluster
    enable_dnn=True,  # enable this for ml cluster
    featurization="auto",
    n_cross_validations=5,
    verbosity=logging.INFO,
    max_concurrent_iterations=9,
    max_cores_per_iteration=-1,
    forecasting_parameters=forecasting_parameters,
)

# COMMAND ----------

# DBTITLE 1,Train
# submit a new training run
from azureml.train.automl.run import AutoMLRun

try:
    if new_training == "True":
        print("New Training Run")
        remote_run = experiment.submit(automl_config, show_output=False) # Story No. 3018 modified Mukesh Dutta 9/3/2021 
    else:
        # If you need to retrieve a run that already started, use the following code
        print("Existing Training Run")
        remote_run = AutoMLRun(
            experiment=experiment, run_id = runid
        )
except Exception as error:
    print(error)
    log_error("{} {}".format(notebook, error)) #log error in sentry
    #raise dbutils.notebook.exit(error) #raise the exception
    raise error #raise the exception

remote_run

# COMMAND ----------

# DBTITLE 1,Retrieve the best model
# !pip install xgboost==0.90
# Get run_id and run_datetime
rr = remote_run.wait_for_completion()
run_id = rr.get("runId")
run_datetime = rr.get("endTimeUtc")
print(run_id, run_datetime)

# Retrieve the Best Model
try:
    best_run, fitted_model = remote_run.get_output()
    print("Best Run Model: ", best_run)
    #print(fitted_model.steps)
    model_name = best_run.properties["model_name"]
except Exception as error:
    print(error)
    log_error("{} {}".format(notebook, error)) #log error in sentry
    #raise dbutils.notebook.exit(error) #raise the exception
    raise error #raise the exception
# print ('Model Name: ', model_name)


# COMMAND ----------

# DBTITLE 1,Transparency
# View updated featurization summary
# Transparency
try:
    featurization_summary = fitted_model.named_steps[
        "timeseriestransformer"
    ].get_featurization_summary()

    # View the featurization summary as a pandas dataframe

    fs = pd.DataFrame.from_records(featurization_summary)
    fs.reset_index(inplace=True, drop=True)
    fs_filename = (
    output_data_path
    + "./featurization_summary.csv"
    )
    fs.to_csv(fs_filename, header=True, index=False) 
    fs.tail(10)
    
except Exception as error:
    print(error)
    log_error("{} {}".format(notebook, error)) #log error in sentry
    #raise dbutils.notebook.exit(error) #raise the exception
    raise error #raise the exception


# COMMAND ----------

# DBTITLE 1,Model metrics
# Get metrics for best run
try:
    pd.set_option("display.float_format", lambda x: "%.5f" % x)
    metricslist = {}
    properties = best_run.get_properties()
    # print(properties)
    metrics = {k: v for k, v in best_run.get_metrics().items() if isinstance(v, float)}
    metricslist[int(properties["iteration"])] = metrics
    rundata = pd.DataFrame.from_records(metricslist).sort_index(1)
    rundata.reset_index(inplace=True)
    rundata.rename(columns={"index": "Metric"}, inplace=True)

    rundata_filename = (
        output_data_path + "./rundata_metrics.csv"
        )
    rundata.to_csv(rundata_filename, header=True, index=False) 
except Exception as error:
    print(error)
    log_error("{} {}".format(notebook, error)) #log error in sentry
    #raise dbutils.notebook.exit(error) #raise the exception
    raise error #raise the exception
rundata

# COMMAND ----------

# DBTITLE 1,Forecasting
# FORECASTING
X_test = test.copy()
y_test = X_test.pop(target_column_name).values

# forecast returns the predictions and the featurized data, aligned to X_test.
# This contains the assumptions that were made in the forecast
# The featurized data, aligned to y, will also be returned.
# and helps align the forecast to the original data

try:
    y_predictions, X_trans = fitted_model.forecast(X_test) # MD Change 11/17/2021

    # from forecasting_helper import align_outputs

    df_all = align_outputs(y_predictions, X_trans, X_test, y_test, target_column_name)
    df_all.rename(columns={"predicted": "Predicted_Revenue"}, inplace=True)
    df_all["Predicted_Revenue"] = np.round(df_all["Predicted_Revenue"],2)
    # df_all.info() 
    df_all.tail() 
except Exception as error:
    print(error)
    log_error("{} {}".format(notebook, error)) #log error in sentry
    #raise dbutils.notebook.exit(error) #raise the exception
    raise error #raise the exception

# COMMAND ----------

# DBTITLE 1,Evaluate
# Evaluate
# To evaluate the accuracy of the forecast, we'll compare against the actual sales quantities for some select metrics, included the mean absolute percentage error (MAPE).

# We'll add predictions and actuals into a single dataframe for convenience in calculating the metrics.

#assign_dict = {'Predicted_Revenue': y_predictions, target_column_name: y_test}
#df_all = X_test.assign(**assign_dict)
#df_all.tail(10)
''' # Story No. 3404
from azureml.automl.core.shared import constants
from azureml.automl.runtime.shared.score import scoring
from matplotlib import pyplot as plt

# use automl scoring module

scores = scoring.score_regression(
    y_test=df_all[target_column_name],
    y_pred=df_all["Predicted_Revenue"],
    metrics=list(constants.Metric.SCALAR_REGRESSION_SET),
)

print("[Test data scores]")
for key, value in scores.items():
    print("{}:   {:.3f}".format(key, value))

# Plot outputs

#%matplotlib inline
test_pred = plt.scatter(
    df_all[target_column_name], df_all["Predicted_Revenue"], color="b"
)
test_test = plt.scatter(
    df_all[target_column_name], df_all[target_column_name], color="g"
)
plt.legend(
    (test_pred, test_test), ("prediction", "truth"), loc="upper left", fontsize=8
)
plt.show() 
''' # Story No. 3404

# COMMAND ----------

# Print metrics by different groups
''' # Story No. 3404
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

#print(df_all.info())
if "horizon_origin" in df_all.columns:
    group_by = "Relative_EOM_Snp_Month_Offset" # Story No. 3018 modified Mukesh Dutta 9/3/2021 
    print("TRUE")
else:
    group_by = "Relative_EOM_Snp_Month_Offset"

print('group_by: ' + group_by) # Story No. 3018 modified Mukesh Dutta 9/3/2021 

df_mpae = df_all.groupby(group_by).apply(
    lambda df: pd.Series(
        {
            "MAPE": MAPE(df[target_column_name], df["Predicted_Revenue"]),
            "RMSE": np.sqrt(
                mean_squared_error(df[target_column_name], df["Predicted_Revenue"])
            ),
            "MAE": mean_absolute_error(df[target_column_name], df["Predicted_Revenue"]),
            "R2_SCORE": r2_score(df[target_column_name], df["Predicted_Revenue"])
        }
    )
)
print(df_mpae) # Story No. 3018 modified Mukesh Dutta 9/3/2021 

# Box plot with different groups
df_all_MAPE = df_all.assign(
    MAPE = MAPE(df_all[target_column_name], df_all["Predicted_Revenue"])
)

MAPEs = [df_all_MAPE[df_all[group_by] == h].MAPE.values for h in range(1, forecast_horizon + 1)]


#%matplotlib inline
plt.boxplot(MAPEs)
plt.yscale("log")
plt.xlabel(group_by)
plt.ylabel("MAPE (%)")
plt.title("Mean Absolute Percentage Errors")

plt.show()
''' # Story No. 3404

# COMMAND ----------

# DBTITLE 1,Final merge with Train
# Merge df_all with train

from functools import reduce

# Get the column index list in the right order

cols_sort_list = list(df_all.columns)
#print(cols_sort_list)
# df_all.tail(10)

nan_value = 0
merge_dfs = [train, df_all]

final_merge_df = train.append(df_all, ignore_index=True, sort=False).sort_values(
    by=final_sort_order, ascending=final_sort_order_ascending
)

final_merge_df = final_merge_df.replace(np.nan, 0, regex=True)

final_merge_df["Relative_Month_Offset"] = round(
    (final_merge_df["End_of_Month"] - current_eom) / np.timedelta64(1, "M"), 0
).astype(int)

# sort using original cols_sort_list

final_merge_df = final_merge_df.sort_values(by=final_sort_order, ascending=final_sort_order_ascending).reset_index(drop=True)
final_merge_df = final_merge_df.reindex(columns=cols_sort_list)

# add new cols
final_merge_df.insert(0,"Forecast_Date", today)
final_merge_df["Run_Date"] = pd.to_datetime(run_datetime).normalize().date()
final_merge_df["Run_ID"] = run_id
final_merge_df["Currency_Code"] = "USD"

#final_merge_df = df_crossjoin(final_merge_df, rundata.query("Metric == 'normalized_root_mean_squared_error'").iloc[:, 1])
#final_merge_df = final_merge_df.reset_index(drop=True)
'''
final_merge_df["NRMSE"] = rundata.query("Metric == 'normalized_root_mean_squared_error'").iloc[:, 1]
final_merge_df["RMSE"] = rundata.query("Metric == 'root_mean_squared_error'").iloc[:, 1]
final_merge_df["R2_Score"] = rundata.query("Metric == 'r2_score'").iloc[:, 1]
final_merge_df["Explained_Variance"] = rundata.query("Metric == 'explained_variance'").iloc[:, 1]
final_merge_df["MAE"] = rundata.query("Metric == 'mean_absolute_error'").iloc[:, 1]
final_merge_df["MAPE"] = rundata.query("Metric == 'mean_absolute_percentage_error'").iloc[:, 1]
final_merge_df["Spearman_Correlation"] = rundata.query("Metric == 'spearman_correlation'").iloc[:, 1]
'''
final_merge_df = final_merge_df.reset_index(drop=True)

move_column_inplace(final_merge_df, 'Forecast_Date', 0)
move_column_inplace(final_merge_df, 'Snapshot_Date', 1)
move_column_inplace(final_merge_df, 'End_of_Month', 2)
move_column_inplace(final_merge_df, 'Sub_Region_Code', 3)
move_column_inplace(final_merge_df, 'Relative_Month_Offset', 4)
move_column_inplace(final_merge_df, 'Relative_Snapshot_Month_Offset', 5)
move_column_inplace(final_merge_df, 'Relative_EOM_Snp_Month_Offset', 6)
move_column_inplace(final_merge_df, 'Snapshot_Day_of_Month', 7)
move_column_inplace(final_merge_df, 'Snp_Seq_No', 8)
move_column_inplace(final_merge_df, 'Currency_Code', 9)
move_column_inplace(final_merge_df, 'Predicted_Revenue', 10)
move_column_inplace(final_merge_df, 'Revenue', 11)

# Cross join with rundata series to populate training metrics columns
metrics_df = pd.DataFrame(rundata).transpose()
new_header = metrics_df.iloc[0] 
metrics_df = metrics_df[1:]
metrics_df.columns = new_header
metrics_df.columns = metrics_df.columns.str.title()
final_merge_df = df_crossjoin(final_merge_df, metrics_df)

# MD Change 11/17/2021
if "origin" in final_merge_df.columns: 
    final_merge_df["origin"] = pd.to_datetime(final_merge_df["origin"])
else:
    final_merge_df["origin"] = final_merge_df["End_of_Month"]
    
if "horizon_origin" in final_merge_df.columns:
    print("Origins exists")
else:
    final_merge_df["horizon_origin"] = final_merge_df["Relative_Month_Offset"] + 1
    # .dt.date
# MD Change 11/17/2021

final_merge_df = movecol(final_merge_df, 
             cols_to_move=['origin','horizon_origin','Current_Opp_Period_Count','Opportunity_Period_Value'], 
             ref_col='Spearman_Correlation',
             place='After')
'''
final_merge_df["Predicted_Revenue_Variance"] = np.where(
    final_merge_df["Relative_Month_Offset"] < 0,
    final_merge_df["Revenue"] - final_merge_df["Predicted_Revenue"],
    np.nan,
)
final_merge_df["Predicted_Revenue_Variance_Percent"] = np.where(
    final_merge_df["Relative_Month_Offset"] < 0,
    round(
        final_merge_df["Predicted_Revenue_Variance"]
        / final_merge_df["Predicted_Revenue"]
        * 100,
        ndigits=4,
    ),
    np.nan,
)
final_merge_df["Predicted_Revenue_Variance"].where(
    final_merge_df["Predicted_Revenue"] != 0, np.nan, inplace=True
)
final_merge_df["Predicted_Revenue_Variance_Percent"].where(
    final_merge_df["Predicted_Revenue"] != 0, np.nan, inplace=True
)
final_merge_df["Predicted_Revenue_Variance_Percent"] = round(
    final_merge_df["Predicted_Revenue_Variance_Percent"], ndigits=4
)
'''

new_cols = ["Predicted_Revenue","Explained_Variance","Mean_Absolute_Error","Mean_Absolute_Percentage_Error","Median_Absolute_Error","Normalized_Mean_Absolute_Error",
            "Normalized_Root_Mean_Squared_Error","Normalized_Root_Mean_Squared_Log_Error","R2_Score","Root_Mean_Squared_Error","Root_Mean_Squared_Error","Root_Mean_Squared_Log_Error","Spearman_Correlation"]

numeric_cols_final_merge = numeric_cols + new_cols
#if "Billable_Headcount" in numeric_cols_final_merge:
#    numeric_cols_final_merge.remove("Billable_Headcount")
#print(numeric_cols_final_merge)

numeric_cols = numeric_cols_final_merge
final_merge_df = convert_date_cols(final_merge_df)
final_merge_df = coerce_to_numeric(final_merge_df, numeric_cols)
final_merge_df = coerce_to_int(final_merge_df, int_cols)

print(" final_merge_df: ")
# display(final_merge_df.tail()) # Story No. 3404
final_merge_df.to_csv(output_data_path + "final_merge_df.csv", index=False)
final_merge_df.to_parquet(output_data_path + "final_merge_df.parquet", index=None)

# COMMAND ----------

# DBTITLE 1,Pivot final merge for verification
# pivot by EOM1, Fin_Entity_ID
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #

#final_cond = "Relative_Snapshot_Month_Offset == 0 and Snp_Seq_No == 6"

final_merge_df1 = final_merge_df.copy() #query(final_cond)

cols = [
    "Predicted_Revenue",
    "Revenue",
    "Pipeline_Recognized",
    "Pipeline_Active_Unrecognized",
    "Pipeline_Opportunity_ML",
    "Pipeline_Opportunity"]
# final_cond = "Relative_EOM_Snp_Month_Offset >= 0 and Relative_Snapshot_Month_Offset <= -3"
# final_merge_df1 = final_merge_df.query(final_cond)

final_merge_df1 = convert_date_cols(final_merge_df1)

final_merge_pivot1 = final_merge_df1.pivot_table(
    index=[
        "Forecast_Date",
        "Snapshot_Date",
        "End_of_Month",
        "Relative_Snapshot_Month_Offset",
        "Relative_Month_Offset",
        "Relative_EOM_Snp_Month_Offset",
        "Snp_Seq_No"
    ],
    values=cols,
    aggfunc={
        "Predicted_Revenue": np.sum,
        "Revenue": np.sum,
        "Pipeline_Recognized": np.sum,
        "Pipeline_Active_Unrecognized": np.sum,
        "Pipeline_Opportunity_ML": np.sum,
        "Pipeline_Opportunity": np.sum
    },
    margins=None,
).fillna(nan_value)
# final_merge_pivot1['End_of_Month'] = pd.to_datetime(final_merge_pivot1['End_of_Month']).dt.date
# final_merge_pivot1.reset_index(level=final_merge_pivot1.index.names)

# show_stats(revtime_pivot1)
final_merge_pivot1.sort_values(
    by=[ "Forecast_Date", "Snapshot_Date", "End_of_Month", "Relative_EOM_Snp_Month_Offset","Snp_Seq_No"], inplace=True
)
# reorder columns
# cols_order = [0,4,1,2,3]
# final_merge_pivot1 = final_merge_pivot1[[final_merge_pivot1.columns[i] for i in cols_order]]

# set ALL float columns to '${:,.2f}' formatting (including the percentage)
# format_dict = {col_name: '${:,.1f}' for col_name in final_merge_pivot1.select_dtypes(float).columns}
# override the percentage column
format_dict = {col_name: "{:,}" for col_name in final_merge_pivot1.columns}
#format_dict["Predicted_Revenue_Variance_Percent"] = "{:.2f}"


# Format with commas and round off to two decimal places in pandas
# final_merge_pivot1 = final_merge_pivot1[[['Snapshot_Date_Short','End_of_Month'] cols]]


# COMMAND ----------

# Predicted Revenue by EOM for latest snapshot date and Seq
final_merge_pivot1.query(
    "Relative_Snapshot_Month_Offset == 0 and Relative_EOM_Snp_Month_Offset >= 0 and Snp_Seq_No == @max_seq"
)   # .tail(30)#.style.format(format_dict)#.style.format('{:,}'# .query('(End_of_Month == @current_eom)')


# COMMAND ----------

# DBTITLE 1,Forecasting output file
#============================= Global Variable Declaration ===================================#
from datetime import datetime
from datetime import date, timedelta, datetime
from pyspark.sql import *
from delta.tables import *
#from pyspark.sql.types import TimestampType, LongType,StructType, StructField, DateType, StringType, DecimalType, IntegerType
from pyspark.sql.functions import col,concat,lit,current_date, when, to_date, unix_timestamp, from_unixtime, regexp_replace
import os

#static variables
g_bi_config_parameters_path = "/mnt/"+ environment + "/gold/g_bi_config_parameters"

#reading config table
df_bi_configuration  = spark.read.format("delta").load(g_bi_config_parameters_path)
df_bi_configuration  = df_bi_configuration.filter((df_bi_configuration.SystemName == "bimodelapi"))

#initializing config parameter values
gold_folder_path     =  df_bi_configuration.filter(df_bi_configuration.ParameterName == "gold_folder_path")\
                                           .select("ParameterValue")\
                                           .collect()[0][0]
#setting delta tables path
g_automljobruninfo_path = gold_folder_path + "/automljobruninfo"  # Story # 3181
 

#reading job run info
df_job_info = spark.read.format("delta").load(g_automljobruninfo_path)  # Story # 3181

#initialize batch id and batch start date time variables
batch_id = df_job_info.agg({"batch_id" : "max"}).collect()[0][0]
batch_start_datetime = df_job_info.agg({"batch_start_datetime" : "max"}).collect()[0][0]

# Create new forecasting file with new forecast date
revenue_predict_new = final_merge_df1.query('Predicted_Revenue != 0 and Relative_Month_Offset >= 0 and Relative_Month_Offset <= @n_test_periods') # MD Change 11/17/2021
revenue_predict_new = convert_date_cols(revenue_predict_new)
revenue_predict_new = revenue_predict_new.sort_values(by=final_sort_order, ascending=final_sort_order_ascending).reset_index(
    drop=True
)
print("new:")
# print(revenue_predict_new.info(verbose=True)) # Story No. 3196 modified Mukesh Dutta 7/13/2021
# reorder columns
# cols_order = [7,0,1,2,3,4,5,6]
# revenue_predict_new = revenue_predict_new[[revenue_predict_new.columns[i] for i in cols_order]]

# filename = (
#     output_data_path
#     + "./forecast/revenue_forecast_"
#     + str(today.normalize().date())
#     + ".csv"
# )

filename = (
    output_data_path
    + "./forecast/revenue_forecast_"
    + datetime.strftime(batch_start_datetime,'%Y-%m-%d')
    + ".csv"
)
print(filename)

revenue_predict = revenue_predict_new.copy()
revenue_predict = revenue_predict.sort_values(by=final_sort_order, ascending=final_sort_order_ascending).reset_index(drop=True)
revenue_predict.to_csv(filename, header=True, index=False)
display(revenue_predict.tail())


# COMMAND ----------

# DBTITLE 1,Metrics
# Model Metrics 
# Start - Story No. 3018 modified Mukesh Dutta 9/3/2021 
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

pd.set_option("display.float_format", lambda x: "%.4f" % x)

def MDAPE(actual, pred):
        """
        Calculate median absolute percentage error.
        Remove NA and values where actual is close to zero
        """

        not_na = ~(np.isnan(actual) | np.isnan(pred))
        not_zero = ~np.isclose(actual, 0.0)
        actual_safe = actual[not_na & not_zero]
        pred_safe = pred[not_na & not_zero]
        return np.median(APE(actual_safe, pred_safe))

group_by = "Relative_EOM_Snp_Month_Offset"
df_mpae = df_all.query('Sub_Region_Code != "Global"').groupby(group_by).apply(  # Story No. 3404 
    lambda df: pd.Series(
        {
            "Relative_EOM_Snp_Month_Offset": np.mean(df[group_by]),
            "MAE": np.round(mean_absolute_error(df[target_column_name], df["Predicted_Revenue"]),2),
            "MAPE%": np.round(MAPE(df[target_column_name], df["Predicted_Revenue"]),2),
            "MDAPE%": np.round(MDAPE(df[target_column_name], df["Predicted_Revenue"]),2),
            "MSE": np.round(mean_squared_error(df[target_column_name], df["Predicted_Revenue"]),2),
            "RMSE": np.round(np.sqrt(
                mean_squared_error(df[target_column_name], df["Predicted_Revenue"])
            ),4),
            "R2_SCORE": np.round(r2_score(df[target_column_name], df["Predicted_Revenue"]),4),
        }
    )
)
# display(df_mpae) # Story No. 3404 
"""
# Box plot with different groups
df_all_MAPE = df_all.assign(
    MAPE = MAPE(df_all[target_column_name], df_all["Predicted_Revenue"])
)

MAPEs = [df_all_MAPE[df_all[group_by] == h].MAPE.values for h in range(1, forecast_horizon + 1)]

#%matplotlib inline
plt.boxplot(MAPEs)
plt.yscale("log")
plt.xlabel(group_by)
plt.ylabel("MAPE (%)")
plt.title("Mean Absolute Percentage Errors")

plt.show()
"""
# End - Story No. 3018 modified Mukesh Dutta 9/3/2021 

# COMMAND ----------

# DBTITLE 1,Model Performance
# evaluate the total result with standard performance metrics
# Start - Story No. 3018 modified Mukesh Dutta 9/3/2021 
from sklearn import metrics # for the evaluation
y_true = np.array(df_all.query('Sub_Region_Code != "Global"')['Revenue']) # Story No. 3404 
y_pred = np.array(df_all.query('Sub_Region_Code != "Global"')['Predicted_Revenue']) # Story No. 3404 
def timeseries_evaluation_metrics_func(y_true, y_pred):
    print('Evaluation metric results:-')
    mae = metrics.mean_absolute_error(y_true, y_pred)
    print(f'MAE is : {np.round(mae, 2)}')
    mape = MAPE(y_true, y_pred)
    print(f'MAPE is : {np.round(mape, 2)} %')
    mdape = MDAPE(y_true, y_pred)
    print(f'MDAPE is : {np.round(mdape, 2)} %')
    mse = metrics.mean_squared_error(y_true, y_pred)
    print(f'MSE is : {np.round(mse, 2)}')
    rmse = metrics.mean_squared_error(y_true, y_pred)
    print(f'RMSE is : {np.round(np.sqrt(rmse), 4)}')
    r2 = metrics.r2_score(y_true, y_pred)
    print(f'R2 is : {np.round(r2, 4)}',end='\n\n') 
        
# timeseries_evaluation_metrics_func(y_true,y_pred) # Story No. 3404 
# End - Story No. 3018 modified Mukesh Dutta 9/3/2021 

# COMMAND ----------

# DBTITLE 1,Actual vs Predicted chart
# Actual Revenue vs Forecast by latest Snapshot Date
# Start - Story No. 3018 modified Mukesh Dutta 9/3/2021 
import plotly.express as px

#Available templates:
#        ['ggplot2', 'seaborn', 'simple_white', 'plotly',
#         'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
#         'ygridoff', 'gridon', 'none']

template = 'seaborn'

df = final_merge_pivot1 \
                .query('Relative_Month_Offset >= -5 and Relative_Month_Offset <= 6 and Relative_Snapshot_Month_Offset == 0  and Snp_Seq_No == @max_seq') \
                .groupby(['Snapshot_Date','End_of_Month','Relative_Month_Offset','Relative_Snapshot_Month_Offset',
                          'Relative_EOM_Snp_Month_Offset','Snp_Seq_No']) \
                .agg('sum') \
                .reset_index()

df1 = df.query('Relative_Month_Offset >= 0')
df2 = df.query('Relative_Month_Offset < 0')
x = 'End_of_Month' # df['Importance'].astype(str)
y = 'Predicted_Revenue'  # df['Feature'].astype(str)
y1 = 'Revenue'
color = 'Snp_Seq_No'
facet = df['Snapshot_Date'].dt.date.astype(str) #'Snapshot_Date_Short'
fig_width = 900
fig_height = 600

fig = px.bar(
    df1,
    x=x,
    y=y,
    #color=color,
    #facet_col=facet, 
    #facet_col_wrap=2,
    text=y,
    height=fig_height,
    #width=fig_width,
    template=template,
    color_discrete_sequence=['RoyalBlue'],
    title='Revenue Forecast by Snapshot Date and Month'
)

fig.add_bar(
    x=df2[x],
    y=df2[y1],
    #color=color,
    #facet_col=facet, 
    #facet_col_wrap=2,
    text=df2[y1],
    name = 'Revenue',
    marker=dict(color='grey')
)

fig.update_traces(texttemplate='%{text:.3s}', textposition='auto')

fig.update_layout(uniformtext_minsize=10, 
                  uniformtext_mode='hide',
                  title_font_size=24,
                  xaxis = dict(
                    title_font_size = 16,
                    dtick='M1',
                    ticklabelmode='period',
                    #tickangle=45
                    ),
                  yaxis = dict(
                    title_font_size = 16
                    )
                 )

fig.show()
# End - Story No. 3018 modified Mukesh Dutta 9/3/2021 

# COMMAND ----------

# Actual Revenue vs Forecast for all Snapshot Dates
# Start - Story No. 3018 modified Mukesh Dutta 9/3/2021 
''' # Story No. 3404 
import plotly.express as px

#Available templates:
#        ['ggplot2', 'seaborn', 'simple_white', 'plotly',
#         'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
#         'ygridoff', 'gridon', 'none']

template = 'seaborn'

df = final_merge_pivot1 \
                .query('Relative_Month_Offset >= -5 and Relative_Month_Offset <= 6 and Snp_Seq_No == @max_seq') \
                .groupby(['Snapshot_Date','End_of_Month','Relative_Month_Offset','Relative_Snapshot_Month_Offset',
                          'Relative_EOM_Snp_Month_Offset','Snp_Seq_No']) \
                .agg('sum') \
                .reset_index()

df1 = df.query('Relative_Month_Offset >= 0')
df2 = df.query('Relative_Month_Offset < 0')
x = 'End_of_Month' # df['Importance'].astype(str)
y = 'Predicted_Revenue'  # df['Feature'].astype(str)
y1 = 'Revenue'
color = 'Relative_Snapshot_Month_Offset'
facet = df['Snapshot_Date'].dt.date.astype(str) #'Snapshot_Date_Short'
fig_width = 900
fig_height = 600

fig = px.line(
    df1,
    x=x,
    y=y,
    color=color,
    #facet_col=facet, 
    #facet_col_wrap=2,
    #text=y,
    height=fig_height,
    #width=fig_width,
    template=template,
    #color_discrete_sequence=['RoyalBlue'],
    title='Revenue Forecast by All Snapshot Dates and Month'
)

fig.add_scatter(
    x=df2[x],
    y=df2[y1],
    #color=color,
    #facet_col=facet, 
    #facet_col_wrap=2,
    #text=df2[y1],
    name = 'Revenue',
    marker=dict(color='grey')
)

fig.update_traces(texttemplate='%{text:.3s}', textposition='middle center')

fig.update_layout(uniformtext_minsize=10, 
                  uniformtext_mode='hide',
                  title_font_size=24,
                  xaxis = dict(
                    title_font_size = 16,
                    dtick='M1',
                    ticklabelmode='period',
                    #tickangle=45
                    ),
                  yaxis = dict(
                    title_font_size = 16
                    )
                 )

fig.show() 
# End - Story No. 3018 modified Mukesh Dutta 9/3/2021 
''' # Story No. 3404 

# COMMAND ----------

# DBTITLE 1,Model explanation
from azureml.interpret import ExplanationClient
from azureml.interpret.common.exceptions import ExplanationNotFoundException
# Get model explanation data
if new_training == "True":
  time.sleep(1200) 
try:  
    explaination_client = ExplanationClient.from_run(best_run)
    if not explaination_client is None:  # TS : added if condition - 2021-05-17 12:57pm
        client = explaination_client
        engineered_explanations = client.download_model_explanation(raw=False)
        #print(engineered_explanations.get_feature_importance_dict())
        print(
            "You can visualize the engineered explanations under the 'Explanations (preview)' tab in the AutoML run at:-\n"
            + best_run.get_portal_url()
        )
        
        feature_imp_dict_eng = pd.DataFrame(
           engineered_explanations.get_feature_importance_dict().items()
        )
        feature_imp_dict_eng.columns = ["Feature", "Importance"]
        
        raw_explanations = client.download_model_explanation(raw=True)
        # print(raw_explanations.get_feature_importance_dict())
        print(
            "You can visualize the raw explanations under the 'Explanations (preview)' tab in the AutoML run at:-\n"
            + best_run.get_portal_url()
        )

        feature_imp_dict_raw = pd.DataFrame(
            raw_explanations.get_feature_importance_dict().items()
        )
        feature_imp_dict_raw.columns = ["Feature", "Importance"]
        # feature_imp_dict.plot() # Story No. 3404 
    else:
        print("Explaination_client not found")
        
except ExplanationNotFoundException:
    print("Explaination_client not found  {}".format(ExplanationNotFoundException))  # TS : added if condition - 2021-05-17 12:57pm
except Exception as error:
    print(error)
    log_error("{} {}".format(notebook, error)) #log error in sentry
    #raise dbutils.notebook.exit(error) #raise the exception
    raise error #raise the exception

# COMMAND ----------

# Print feature importance - Raw
# Start - Story No. 3018 modified Mukesh Dutta 9/3/2021 
import plotly.express as px

#Available templates:
#        ['ggplot2', 'seaborn', 'simple_white', 'plotly',
#         'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
#         'ygridoff', 'gridon', 'none']

template = 'seaborn'
source = feature_imp_dict_raw.query('Feature != "Headcount_Billable"')
x = 'Importance'  # df['Importance'].astype(str)
y = 'Feature'  # df['Feature'].astype(str)
y1 = 'Revenue'
fig_width = 1200
fig_height = 2400

# Use textposition='auto' for direct text
fig = px.bar(source,
             x=x,
             y=y,
             text=x,
             height=fig_height,
             width =fig_width,
             template=template,
             title='Forecast Model Drivers Importance (excluding "Headcount_Billable")',
            )
fig.update_traces(texttemplate='%{text:.2s}', textposition='auto')
fig.update_layout(
    uniformtext_minsize=8, 
    uniformtext_mode='hide',
    title_font_size=24,
    xaxis = dict(title_font_size = 16),
    yaxis = dict(title_font_size = 16, autorange="reversed")
) 
fig.show()
# End - Story No. 3018 modified Mukesh Dutta 9/3/2021 

# COMMAND ----------

# DBTITLE 1,Print feature importance
# Print feature importance - Engineered
# Start - Story No. 3018 modified Mukesh Dutta 9/3/2021 
import plotly.express as px

#Available templates:
#        ['ggplot2', 'seaborn', 'simple_white', 'plotly',
#         'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
#         'ygridoff', 'gridon', 'none']

template = 'seaborn'
source = feature_imp_dict_eng.query('Feature != "Headcount_Billable"')
x = 'Importance'  # df['Importance'].astype(str)
y = 'Feature'  # df['Feature'].astype(str)
y1 = 'Revenue'
fig_width = 1200
fig_height = 2400

# Use textposition='auto' for direct text
fig = px.bar(source,
             x=x,
             y=y,
             text=x,
             height=fig_height,
             width =fig_width,
             template=template,
             title='Forecast Model Drivers Importance (excluding "Headcount_Billable")',
            )
fig.update_traces(texttemplate='%{text:.2s}', textposition='auto')
fig.update_layout(
    uniformtext_minsize=8, 
    uniformtext_mode='hide',
    title_font_size=24,
    xaxis = dict(title_font_size = 16),
    yaxis = dict(title_font_size = 16, autorange="reversed")
) 
fig.show()
# End - Story No. 3018 modified Mukesh Dutta 9/3/2021 

# COMMAND ----------

# Revenue Forecast by Snapshot Date and Month with lines
# Start - Story No. 3018 modified Mukesh Dutta 9/3/2021 
"""
df = revenue_predict \
                .groupby(['Snapshot_Date','End_of_Month','Relative_Month_Offset','Relative_Snapshot_Month_Offset',
                          'Relative_EOM_Snp_Month_Offset','Snapshot_Day_of_Month','Snp_Seq_No']) \
                .agg('sum') \
                .reset_index()

x = 'Relative_Month_Offset'  # df['Importance'].astype(str)
y = 'Predicted_Revenue'  # df['Feature'].astype(str)
y1 = 'Revenue'
color = 'Snp_Seq_No'
facet = df['Snapshot_Date'].dt.date.astype(str) #'Snapshot_Date_Short'
fig_width = 1200
fig_height = 1200

fig = px.bar(
    df,
    x=x,
    y=y,
    #color=color,
    facet_col=facet, 
    facet_col_wrap=2,
    text=y,
    height=fig_height,
    width=fig_width,
    template=template,
    title='Revenue Forecast by Snapshot Date and Month'
)

fig.update_traces(texttemplate='%{text:.3s}', textposition='auto')

#fig.add_scatter(
#    x=df[x], y=df[y1], text=df[y1], name="Revenue", line_color="gray"
#)

fig.update_layout(uniformtext_minsize=10, 
                  uniformtext_mode='hide',
                  title_font_size=24,
                  xaxis = dict(
                    title_font_size = 16,
                    dtick='M1',
                    ticklabelmode='period',
                    #tickangle=45
                    ),
                  yaxis = dict(
                    title_font_size = 16
                    )
                 )                 
fig.show()
"""
# End - Story No. 3018 modified Mukesh Dutta 9/3/2021 

# COMMAND ----------

# Variance% and Variance Amount by Month as of Current Month
# Start - Story No. 3018 modified Mukesh Dutta 9/3/2021 
"""
df = revenue_predict \
                .groupby(['Snapshot_Date','End_of_Month','Relative_Month_Offset','Relative_Snapshot_Month_Offset',
                          'Relative_EOM_Snp_Month_Offset','Snapshot_Day_of_Month','Snp_Seq_No']) \
                .agg('sum') \
                .reset_index()
df['AE'] = abs(df['Predicted_Revenue'] - df['Revenue'])
#df['MAE'] = round(np.mean(df['AE']),2)
df['APE'] = round(abs(df['AE'] / df['Revenue']),2)
#df['MAPE'] = round(np.mean(df['APE']),2)

x = 'Relative_EOM_Snp_Month_Offset'  # df['Importance'].astype(str)
y = 'APE'  # df['Feature'].astype(str)
y1 = 'AE'
color = 'Snp_Seq_No'
facet = df['Snapshot_Date'].dt.date.astype(str) #'Snapshot_Date_Short'
fig_width = 1200
fig_height = 1200

fig = px.bar(
    df,
    x=x,
    y=y,
    #color=color,
    #facet_col=facet, 
    #facet_col_wrap=2,
    text=y,
    height=fig_height,
    width=fig_width,
    template=template,
    title='Revenue Forecast by Snapshot Date and Month'
)

fig.update_traces(texttemplate='%{text:.3s}', textposition='auto')

#fig.add_scatter(
#    x=df[x], y=df[y1], text=df[y1], name="Revenue", line_color="gray"
#)

fig.update_layout(uniformtext_minsize=10, 
                  uniformtext_mode='hide',
                  title_font_size=24,
                  xaxis = dict(
                    title_font_size = 16
                    ),
                  yaxis = dict(
                    title_font_size = 16
                    )
                 )
                 
fig.update_xaxes(tickangle=45)

fig.show()
"""
# End - Story No. 3018 modified Mukesh Dutta 9/3/2021 

# COMMAND ----------

#**********************************************************DONE*************************************************************
print("Model processing completed")

# COMMAND ----------

# DBTITLE 1,Operationalize
#Operationalization means getting the model into the cloud so that other can run it after you close the notebook. We will create a docker running on Azure Container Instances with the model.
"""
description = 'AutoML Revenue forecaster'
tags = None
model = remote_run.register_model(model_name = model_name, description = description, tags = tags)

print(remote_run.model_id)

#Develop the scoring script
#For the deployment we need a function which will run the forecast on serialized data. It can be obtained from the best_run.
script_file_name = 'score_fcast.py'
best_run.download_file('outputs/scoring_file_v_1_0_0.py', script_file_name)

#Deploy the model as a Web Service on Azure Container 
from azureml.core.model import InferenceConfig
from azureml.core.webservice import AciWebservice
from azureml.core.webservice import Webservice
from azureml.core.model import Model

inference_config = InferenceConfig(environment = best_run.get_environment(), 
                                   entry_script = script_file_name)

aciconfig = AciWebservice.deploy_configuration(cpu_cores = 2, 
                                               memory_gb = 4, 
                                               tags = {'type': 'automl-forecasting'},
                                               description = 'Automl revenue forecasting service')

aci_service_name = 'automl-revenue-region-forecast'
print(aci_service_name)
aci_service = Model.deploy(ws, aci_service_name, [model], inference_config, aciconfig)
aci_service.wait_for_deployment(True)
print(aci_service.state)

aci_service.get_logs()

#Call the service
import json
X_query = test.copy()
# We have to convert datetime to string, because Timestamps cannot be serialized to JSON.
X_query[time_column_name] = X_query[time_column_name].astype(str)
# The Service object accept the complex dictionary, which is internally converted to JSON string.
# The section 'data' contains the data frame in the form of dictionary.
test_sample = json.dumps({'data': X_query.to_dict(orient='records')})
response = aci_service.run(input_data = test_sample)
# translate from networkese to datascientese
try: 
    res_dict = json.loads(response)
    y_fcst_all = pd.DataFrame(res_dict['index'])
    y_fcst_all[time_column_name] = pd.to_datetime(y_fcst_all[time_column_name], unit = 'ms')
    y_fcst_all['forecast'] = res_dict['forecast']    
except:
    print(res_dict)
    
y_fcst_all.head()    

#Delete the web service if desired
#serv = Webservice(ws, 'automl-revenue-forecast-01')
#serv.delete()     # don't do it accidentally
"""
print("Done")

# COMMAND ----------



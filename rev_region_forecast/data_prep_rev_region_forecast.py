# Databricks notebook source
# DBTITLE 1,Setup
# define widgets - NEED TO DEFINE IT ONCE
# dynamic variables (pass it from ADF)
# first time runtime parameter
# dbutils.widgets.dropdown("environment", "dev", ["dev","uat","prod"])
# dbutils.widgets.dropdown("new_training", "False", ["True","False"])
# dbutils.widgets.dropdown("system_name", "bimodelapi", ["bimodelapi"])
# dbutils.widgets.dropdown("data_load_type", "Full", ["Full"])
# dbutils.widgets.text("system_name", "","")
# dbutils.widgets.remove("pbiapi")
environment = dbutils.widgets.get("environment")
new_training = dbutils.widgets.get("new_training")
system_name = dbutils.widgets.get("system_name")
data_load_type = dbutils.widgets.get("data_load_type") # Full/Incremental
if environment in {"prod"}:
    aml_compute_cluster_name = "cc-bi-ml-prod01"
else:
    aml_compute_cluster_name = "cc-bi-ml-devqa01"

print(environment, system_name, new_training, aml_compute_cluster_name, data_load_type)



# COMMAND ----------

# DBTITLE 1,Data load
# MAGIC %run ./data_load_rev_region_forecast

# COMMAND ----------

# DBTITLE 1,Initial prep
# Prepare Data & Set time column and series columns
print("Preparing data")
# set variales

target_column_name = "Revenue"
time_column_name = "End_of_Month"
time_series_id_column_names = ["Relative_EOM_Snp_Month_Offset", "Snp_Seq_No", "Sub_Region_Code"]
sort_cols_snp = ["Snapshot_Date", "End_of_Month", "Sub_Region_Code"]
sort_cols_eom = ["End_of_Month", "Sub_Region_Code"]
final_sort_order = [time_column_name] + time_series_id_column_names
final_sort_order_ascending = [True, False, True, True]

exclude_sub_region = ["Singapore"] # Story No. 3018 modified Mukesh Dutta 9/3/2021

pd.set_option("display.float_format", lambda x: "%.2f" % x)

# remove future dated rows for all source systems except pipeline add this code below .query('End_of_Month <= @current_eom')
talenthist1 = talenthist.copy()  # .toPandas() to convert from spark df to pandas
pipehist1 = pipehist.copy()
opphist1 = opphist.copy()
projhist1 = projhist.copy()
revhist1 = revhist.copy()
pipetrend1 = pipetrend.copy()
projectactualshist1 = projectactualshist.copy()

# pipetrend1.drop(columns="Snapshot_Date_Short", axis=1, inplace=True)

#pipehist1.rename(columns={"Snapshot_Date_Short": "Snapshot_Date"}, inplace=True)
#pipetrend1.rename(columns={"Snapshot_Date_Short": "Snapshot_Date"}, inplace=True)
#pipetrend1.rename(columns={"Snapshot_End_of_Month": "End_of_Month"}, inplace=True)
#talenthist1["Billable_Headcount"] = (
#    talenthist1["Headcount"] + talenthist1["Headcount_Contingent"]
#)
#talenthist1.drop(columns=["Headcount"], axis=1, inplace=True)

numeric_cols = [
    "Revenue",
    "Pipeline",
    "Pipeline_at_100_Percent",
    "Yield",
    "Pipeline_Trend",
    "Conversions",
    "Project_Period_Count",
    "Project_Count",
    "Project_Period_Price",
    "Project_Price",	
    "Opportunity_Period_Count",
    "Opportunity_Period_Value",
    "Opportunity_Count",
    "Current_Opp_Period_Value",
    "Current_Opp_Period_Count",
    'Opportunity_Value',
    "Win_Rate",
    "Headcount",
    "Utilization_Billable",
    "Nominal_Hours"
]

df_list = [
    pipehist1,
    talenthist1,
    revhist1,
    opphist1,
    projhist1,
    pipetrend1,
    projectactualshist1
]
for x in df_list:
    x.infer_objects()

    # convert date to to_datetime

    x = convert_date_cols(x)
    
    # coerce numeric_cols to numeric
    x = coerce_to_numeric(x, numeric_cols)

    # replace null values to NA
    
    x["Sub_Region_Code"] = x["Sub_Region_Code"].replace(np.nan, "NA", regex=True)
    
    x = x.replace(np.nan, 0, regex=True)

    # make all dates to End of Month values to later merge
    if "End_of_Month" in x.columns:
        x["End_of_Month"] = x["End_of_Month"] + pd.offsets.MonthEnd(0)
    

    if "Snapshot_Date" in x.columns:
        # x['Snapshot_Date'] = x['Snapshot_Date'] + pd.offsets.MonthEnd(0)
        x.sort_values(by=["End_of_Month", "Snapshot_Date", "Sub_Region_Code"]).reset_index(drop=True)
    else:
        x.sort_values(by=sort_cols_eom).reset_index(drop=True)
    
    print(get_df_name(x), ":", x.shape)
    '''
    display(x.info())
    display(x.tail())
    '''

# COMMAND ----------

# DBTITLE 1,Base table prep
# Preparing eom_region_snp_final 
# Cross join to get cross-join of all unquie values of End_of_Month + Relative_EOM_Snp_Month_Offset + Snp_Seq_No + Sub_Region_Code with forward filling the last snapshot date for that relative_month
print("Preparing eom_region_snp_final")
# Set Parameters---------------------------------------------------------------------------------------------------------------------

past_period = 18
n_test_periods = 6
nan_value = 0
dt1 = pd.to_datetime('2018-07-31') #ignore revenues before pipeline data was not there

d = pd.date_range(start_date, today + pd.offsets.MonthEnd(n_test_periods), freq="m")
eom = pd.DataFrame(d, columns=["End_of_Month"])

# eom = pipehist_pivot1[['End_of_Month']].drop_duplicates(subset='End_of_Month',keep='last').sort_values(by=['End_of_Month']).reset_index(drop=True)

sub_region = pd.DataFrame(
    pipehist1["Sub_Region_Code"].unique(), columns=["Sub_Region_Code"]
)
snp_date = pd.DataFrame(pipehist1["Snapshot_Date"].unique(), columns=["Snapshot_Date"])
#print("Debug:", snp_date["Snapshot_Date"].unique())

eom1 = df_crossjoin(snp_date, eom)
eom1 = eom1.reset_index(drop=True)

eom2 = df_crossjoin(eom1, sub_region)
eom2 = eom2.reset_index(drop=True)
# eom1 = eom.merge(snp_date, how="cross")
# eom2 = eom1.merge(sub_region, how="cross")

eom_region_snp = eom2.replace(np.nan, "NA", regex=True).query(
    "Sub_Region_Code not in @exclude_sub_region"
)
eom_region_snp = eom_region_snp.reset_index(drop=True)
# eom_region_snp.drop(columns=['key'], axis=1, inplace = True)
#print(pd.DataFrame(eom_region_snp.describe(include='all')))
# Add offset columns

eom_region_snp["Relative_Month_Offset"] = round(
    (eom_region_snp["End_of_Month"] - current_eom) / np.timedelta64(1, "M"), 0
).astype(int)
eom_region_snp["Relative_Snapshot_Month_Offset"] = round(
    (eom_region_snp["Snapshot_Date"] + pd.offsets.MonthEnd(0) - current_eom)
    / np.timedelta64(1, "M"),
    0,
).astype(int)
eom_region_snp["Relative_EOM_Snp_Month_Offset"] = (
    eom_region_snp["Relative_Month_Offset"] -
    eom_region_snp["Relative_Snapshot_Month_Offset"]
)
eom_region_snp["Relative_EOM_Snp_Month_Offset"] = eom_region_snp["Relative_EOM_Snp_Month_Offset"].astype(int)
eom_region_snp["Snapshot_Day_of_Month"] = eom_region_snp["Snapshot_Date"].dt.day

eom_region_snp1 = eom_region_snp.copy()

# display(eom_region_snp.tail())

# Add a sequence number to each element in a group using python

eom_region_snp1 = eom_region_snp1.sort_values(by=["End_of_Month", "Snapshot_Date", "Sub_Region_Code"]).reset_index(drop=True)
#print("eom_region_snp1:")
#eom_region_snp1.info()
#display(eom_region_snp1.tail())


eom_region_snp2 = (
    eom_region_snp1[["End_of_Month", "Relative_EOM_Snp_Month_Offset", "Snapshot_Date"]]
    .groupby(["End_of_Month", "Relative_EOM_Snp_Month_Offset", "Snapshot_Date"])
    .last()
)

# eom_region_snp['Snp_Seq_No'] = normalize(eom_region_snp['Snapshot_Date'].dt.day,1,15) #Normalizing from one range to another

eom_region_snp2["Snp_Seq_No"] = eom_region_snp2.groupby(
    ["End_of_Month", "Relative_EOM_Snp_Month_Offset"]
).cumcount()
#print(eom_region_snp2["Snp_Seq_No"].unique())

seq = pd.DataFrame()

seq["Snp_Seq_No"] = eom_region_snp2["Snp_Seq_No"].unique()
max_seq = seq["Snp_Seq_No"].max()

cols1 = ["End_of_Month", "Relative_EOM_Snp_Month_Offset"]
eom_offset_region = eom_region_snp1[cols1].drop_duplicates()
eom_offset_seq = df_crossjoin(eom_offset_region, seq)

#display(eom_offset_seq)
eom_region_snp2 = eom_region_snp2.reset_index(
    level=eom_region_snp2.index.names
).reset_index(drop=True)
#print("eom_region_snp2:")
#eom_region_snp2.info()
#display(eom_region_snp2)
#display(seq.info())
#print("eom_region_snp2:", eom_region_snp2["Snp_Seq_No"].max())
#print("eom_region_snp2:", eom_region_snp2["Snapshot_Date"].max())
eom_region_snp3 = pd.merge(
    eom_offset_seq,
    eom_region_snp2,
    how = "left",
    on=["End_of_Month","Relative_EOM_Snp_Month_Offset","Snp_Seq_No"]
)
eom_region_snp3 = eom_region_snp3.reset_index(drop=True)
#print("eom_region_snp3:")
#display(eom_region_snp3.info())

#display(eom_region_snp3.tail())
eom_region_snp3 = eom_region_snp3.ffill(axis=0)

#print("eom_region_snp3:", eom_region_snp3["Snapshot_Date"].max())

eom_region_snp_final = pd.merge(
    eom_region_snp1,
    eom_region_snp3,
    how="right",
    on=["End_of_Month", "Relative_EOM_Snp_Month_Offset", "Snapshot_Date"],
).fillna(nan_value)
eom_region_snp_final = eom_region_snp_final.reset_index(drop=True)

eom_region_snp_final = eom_region_snp_final.replace(np.nan, 0, regex=True)
#print("eom_region_snp_final:")
#display(eom_region_snp_final.info())

int_cols = [
    "Relative_Snapshot_Month_Offset",
    "Relative_Month_Offset",
    "Relative_EOM_Snp_Month_Offset",
    "Snp_Seq_No",
    "Snapshot_Day_of_Month"
]

eom_region_snp_final = coerce_to_int(eom_region_snp_final, int_cols)


#print(eom_region_snp_final["Snapshot_Date"].max())
# FILTER for relevant history and forecast period rows #
main_filter = "Relative_EOM_Snp_Month_Offset >= -@past_period and \
                Relative_EOM_Snp_Month_Offset < @n_test_periods and \
                Relative_Snapshot_Month_Offset <= 0 and \
                Snp_Seq_No in [0, 1, 2, 3, 4, @max_seq]" # Story No. 3404 modified Mukesh Dutta 9/21/2021
    
eom_region_snp_final = eom_region_snp_final.query(main_filter).sort_values(by=final_sort_order).reset_index(drop=True)

eom_region_snp_final.to_csv(output_data_path + "eom_region_snp.csv", index=False)

# print("eom_region_snp_final:") # User Story 3404
#eom_region_snp_final['End_of_Month'].unique()
# eom_region_snp_final.info() # Story No. 3196 modified Mukesh Dutta 7/13/2021
print("eom_region_snp_final:", eom_region_snp_final.shape) # Story No. 3018 modified Mukesh Dutta 9/3/2021
# eom_region_snp_final.tail(10) # User Story 3404

# COMMAND ----------

# DBTITLE 1,EOM Region Snp Pivot
# print(pd.DataFrame(eom_region_snp_final.describe(include='all').T)) # User Story 3404
'''
eom_region_snp_pvt = eom_region_snp_final.pivot_table(
    index=[time_column_name, "Relative_EOM_Snp_Month_Offset"],
    columns=["Snp_Seq_No", "Sub_Region_Code"],
    values=["Snapshot_Date"],
    aggfunc={"Snapshot_Date": "count"},
    margins=False,
)
# eom_region_snp_pvt = eom_region_snp_pvt.reset_index(level=eom_region_snp_pvt.index.names).reset_index(drop=True)
#eom_region_snp_pvt
'''

# COMMAND ----------

# DBTITLE 1,Pivot talent
# Pivot Talent from long to wide to match month end grain
print("Preparing talent wide")
nan_value = 0
#display(talenthist1.tail())

talenthist_wide1 = talenthist1.pivot_table(
    index=["End_of_Month", "Sub_Region_Code"],
    columns=["Billable"], #, "Journey_Level"
    values=["Headcount"],
    aggfunc={"Headcount": np.sum},
    margins=False,
).fillna(nan_value)

talenthist_wide1.columns = [
    "_".join(tuple(map(str, t))) for t in talenthist_wide1.columns.values
]

'''
"Headcount_Billable_Temp",
             "Headcount_Billable_Unknown",
             "Headcount_Non-Billable_Unknown",
             "Headcount_Unknown_Director",
             "Headcount_Unknown_Enterprise",
             "Headcount_Unknown_Individual",
             "Headcount_Unknown_Team",
             "Headcount_Unknown_Unknown"
''' 
talenthist_wide1.reset_index(inplace=True)
talent_cols1 = talenthist_wide1.columns
remove_list1 = ["End_of_Month", "Sub_Region_Code", "Headcount", "Headcount_Unknown"]
talent_cols1 = difflist(talent_cols1, remove_list1)
talenthist_wide1.drop(
    columns=["Headcount_Unknown"], axis=1, inplace = True)
#print(talent_cols1)

#display(talenthist_wide1.tail())

# Sum contingent at month, billable, sub-region level
talenthist_wide2 = talenthist1.pivot_table(
    index=["End_of_Month", "Sub_Region_Code"],
    columns=["Billable"],
    values=["Headcount_Contingent"],
    aggfunc={"Headcount_Contingent": np.sum},
    margins=False,
).fillna(nan_value)

talenthist_wide2.columns = [
    "_".join(tuple(map(str, t))) for t in talenthist_wide2.columns.values
]

talenthist_wide2.reset_index(inplace=True)
talent_cols2 = talenthist_wide2.columns
remove_list2 = ["End_of_Month", "Sub_Region_Code", "Headcount_Contingent"]
talent_cols2 = difflist(talent_cols2, remove_list2)
#talenthist_wide2.drop(columns=[""], axis=1, inplace = True)
#print(talent_cols2)

# talenthist_wide.tail()
# pipetrend_wide['Pipeline'] = pd.to_numeric(pipetrend_wide['Pipeline'], errors='coerce').astype(int)

talent_cols = talent_cols1 + talent_cols2
numeric_cols_talent = numeric_cols
numeric_cols_talent = numeric_cols_talent + talent_cols
numeric_cols = numeric_cols_talent
#print(numeric_cols)

talenthist_wide1 = coerce_to_numeric(talenthist_wide1, numeric_cols)
talenthist_wide1 = convert_date_cols(talenthist_wide1)
talenthist_wide1 = talenthist_wide1.sort_values(by=sort_cols_eom).reset_index(drop=True)

talenthist_wide2 = coerce_to_numeric(talenthist_wide2, numeric_cols)
talenthist_wide2 = convert_date_cols(talenthist_wide2)
talenthist_wide2 = talenthist_wide2.sort_values(by=sort_cols_eom).reset_index(drop=True)
#print(talenthist_wide1.info())
# print(' pipetrend_wide: ')

#display(talenthist_wide1.info())
#display(talenthist_wide2.info())
'''
# pipetrend_wide.query('Fin_Entity_ID==@entity_debug')
#print("talenthist_wide:", talenthist_wide.shape)
#talenthist_wide.tail()  # .query('Relative_Snapshot_Month_Offset == 0')
'''

# COMMAND ----------

# DBTITLE 1,Pivot ProjectActuals
# Pivot ProjectActuals from long to wide to match month end grain
print("Preparing ProjectActuals wide")
nan_value = 0

projectactualshist_wide = projectactualshist1.pivot_table(
    index=["End_of_Month", "Sub_Region_Code"],
    #columns="Journey_Level",
    values=["Utilization_Billable","Nominal_Hours"],
    aggfunc={"Utilization_Billable": np.mean, "Nominal_Hours": np.sum},
    margins=False,
).fillna(nan_value)

#projectactualshist_wide.columns = [
#    "_".join(tuple(map(str, t))) for t in projectactualshist_wide.columns.values
#]

projectactualshist_wide.reset_index(inplace=True)
'''
,"Utilization_Billable_Temp",
               "Utilization_Billable_Unknown",
               "Nominal_Hours_Temp"
'''
projectactuals_cols = projectactualshist_wide.columns
remove_list = ["End_of_Month", "Sub_Region_Code", "Utilization_Billable"
              ]
projectactuals_cols = difflist(projectactuals_cols, remove_list)
#projectactualshist_wide.drop(
#    columns=[""], axis=1, inplace = True)
#print(projectactuals_cols)
    
numeric_cols_projectactuals = numeric_cols
numeric_cols_projectactuals = numeric_cols_projectactuals + projectactuals_cols
#print(numeric_cols_projectactuals)

numeric_cols = numeric_cols_projectactuals
projectactualshist_wide = coerce_to_numeric(projectactualshist_wide, numeric_cols)
projectactualshist_wide = convert_date_cols(projectactualshist_wide)
projectactualshist_wide = projectactualshist_wide.sort_values(by=sort_cols_eom).reset_index(drop=True)

# print(' projectactualshist_wide: ')

#display(projectactualshist_wide.info())
#display(projectactualshist_wide.tail())
'''
# projectactualshist_wide.query('Fin_Entity_ID==@entity_debug')
print("projectactualshist_wide:", projectactualshist_wide.shape)
'''

# COMMAND ----------


# Pivot Pipeline_Trend from long to wide to match month end grain
'''
print("Preparing Pipeline_Trend Wide")

nan_value = 0
pipetrend_wide = pipetrend1.pivot_table(
    index=["Snapshot_Date", "End_of_Month", "Sub_Region_Code"],
    columns="Pipeline_Type",
    values=["Pipeline_Trend"],  # , 'Pipeline_3M_Rolling_Avg'
    aggfunc={"Pipeline_Trend": np.sum},
    margins=False,
).fillna(nan_value)

pipetrend_wide.columns = [
    "_".join(tuple(map(str, t))) for t in pipetrend_wide.columns.values
]

pipe_cols = [
    "Active_Unrecognized_Trend",
    "Opportunity_Trend",
    "Opportunity_ML_Trend",  # ,    "Recognized",
]

pipetrend_wide.columns = pipe_cols

pipetrend_wide = pipetrend_wide.reset_index(
    level=pipetrend_wide.index.names
).reset_index(drop=True)

numeric_cols_pipetrend = numeric_cols
numeric_cols_pipetrend = numeric_cols_pipetrend + pipe_cols
if "Pipeline_Trend" in numeric_cols_pipetrend:
    numeric_cols_pipetrend.remove("Pipeline_Trend")
print(numeric_cols_pipetrend)

numeric_cols = numeric_cols_pipetrend
pipetrend_wide = convert_date_cols(pipetrend_wide)
pipetrend_wide = coerce_to_numeric(pipetrend_wide,numeric_cols)
pipetrend_wide = coerce_to_int(pipetrend_wide, int_cols)

pipetrend_wide = pipetrend_wide.sort_values(by=["Snapshot_Date", "End_of_Month", "Sub_Region_Code"]).reset_index(drop=True)

# print(' pipetrend_wide: ')

display(pipetrend_wide.info())

# pipetrend_wide.query('Fin_Entity_ID==@entity_debug')
print("pipetrend_wide:", pipetrend_wide.shape)
#pipetrend_wide.tail()  # .query('Relative_Snapshot_Month_Offset == 0')

'''

# COMMAND ----------

# DBTITLE 1,First merge with base table
# Merge eom_studio with Revenue, Talent, Opportunity, Project
print("Preparing merge_df")
from functools import reduce

nan_value = 0
merge_dfs = [eom_region_snp_final, revhist1, projhist1, opphist1, projectactualshist_wide, talenthist_wide1, talenthist_wide2]

merge_df = reduce(
    lambda left, right: pd.merge(
        left, right, how="left", on=["End_of_Month", "Sub_Region_Code"]
    ),
    merge_dfs,
).fillna(nan_value)

merge_df = convert_date_cols(merge_df)
merge_df = coerce_to_numeric(merge_df, numeric_cols)
merge_df = coerce_to_int(merge_df, int_cols)

merge_df = merge_df.replace(np.nan, 0, regex=True)
merge_df.sort_values(by=sort_cols_snp).reset_index(drop=True)

# merge_df.drop(columns=['Current_Opp._Period_Value','Conversions'], axis=1, inplace = True)

#print(" merge_df: ", merge_df.shape)

# merge_df.to_csv(output_data_path   'merge_df.csv', index=False)
# show_stats(merge_df)

print("merge_df: ", merge_df["Snapshot_Date"].max())
# merge_df.info() # Story No. 3196 modified Mukesh Dutta 7/13/2021

print("merge_df:", merge_df.shape) # Story No. 3018 modified Mukesh Dutta 9/3/2021
# merge_df.tail(10)  # .query('Fin_Entity_ID==@entity_debug')') # Story No. 3018 modified Mukesh Dutta 9/3/2021

# COMMAND ----------

# DBTITLE 1,Second merge with base table
# Merge with eom_region_snp1 with stock_final_pivot
print("Preparing merge_df1")
# eom1 = eom  # pd.DataFrame(d, columns=['End_of_Month'])
# stock_final_df = pd.merge(
#    eom_region_snp1, stock_final_pivot, how="left", on=["End_of_Month","Relative_Month_Offset","Relative_Snapshot_Month_Offset","Relative_EOM_Snp_Month_Offset"]
# ).fillna(nan_value)

# sp500_df = sp500_df.ffill(axis = 0)

# stock_final_df = stock_final_df.sort_values(by=sort_cols_eom).reset_index(drop=True)
# display(sp500_df.tail(10))

merge_df1 = pd.merge(merge_df, stock_final_pivot, how="left", on="End_of_Month")  # TS 2021-08-24 - remove fillna(nan_value)
#merge_df1 = pd.merge(merge_df, stock_final_pivot, how="left", on="End_of_Month").ffill(axis=0)
""" # User Story 3404
merge_df2 = merge_df1.query("End_of_Month <= @current_eom").ffill(axis=0)    # TS 2021-08-24 - line add
merge_df3 = merge_df1.query("End_of_Month > @current_eom").fillna(nan_value) # TS 2021-08-24 - line add
df_list = [merge_df2,merge_df3]  # TS 2021-08-24 - line add
merge_df1 = pd.concat(df_list)   # TS 2021-08-24 - line add
""" # User Story 3404
#df[(df["pos"] == "GK") & (df["goals"].isnull())].fillna(0)

# Start - Story No. 3404 
merge_df1 = pd.merge(merge_df, stock_final_pivot, how="left", on="End_of_Month").ffill(axis = 0) 
# # replace future month with 0 values for market indices
for col in symbols_name:
    merge_df1[col].mask(merge_df1["End_of_Month"] > current_eom, 0, inplace = True)
# merge_df1.query('End_of_Month > @current_eom')
# End - Story No. 3404     
#print(symbols_name)

numeric_cols_stock = numeric_cols
numeric_cols_stock = numeric_cols_stock + symbols_name
#if "Pipeline_Trend" in numeric_cols_stock:
#    numeric_cols_stock.remove("Pipeline_Trend")
#print(numeric_cols_stock)

numeric_cols = numeric_cols_stock
merge_df1 = convert_date_cols(merge_df1)
merge_df1 = coerce_to_numeric(merge_df1, numeric_cols)
merge_df1 = coerce_to_int(merge_df1, int_cols)

merge_df1 = merge_df1.sort_values(by=final_sort_order).reset_index(drop=True)
#merge_df1.to_csv(output_data_path +  'merge_df1.csv', index=False)
#merge_df4.to_csv(output_data_path +  'merge_df4.csv', index=False)
# merge_sdf1 = spark.createDataFrame(merge_df1);
# merge_sdf1.write.format("parquet").mode("overwrite").parquet(output_data_path   'merge_df1.parquet')

# show_stats(merge_df1.tail(10))
# merge_df1.info()
#print(merge_df1.shape)

# print("merge_df1: ", merge_df1["Snapshot_Date"].max()) # User Story 3404
# merge_df1.info() # Story No. 3196 modified Mukesh Dutta 7/13/2021
# display(merge_df1) # User Story 3404
print("merge_df1:", merge_df1.shape) # Story No. 3018 modified Mukesh Dutta 9/3/2021
# merge_df1.tail(10) # Story No. 3018 modified Mukesh Dutta 9/3/2021


# COMMAND ----------

# DBTITLE 1,Pivot Pipeline

# Pivot Pipeline_Type from long to wide to match month end grain
print("Preparing pipehist_wide")
pipehist_long = pipehist1.copy()
pipehist_wide = pipehist_long.pivot_table(
    index=[
        "Snapshot_Date",
        "End_of_Month",
        "Sub_Region_Code",
        "Relative_Snapshot_Month_Offset",
        "Relative_Month_Offset"
    ],
    columns="Pipeline_Type",
    values=["Pipeline", "Pipeline_at_100_Percent", "Yield"],  
    # aggfunc={"Pipeline": np.sum},
    margins=False,
).fillna(nan_value)

pipehist_wide.columns = [
    "_".join(tuple(map(str, t))) for t in pipehist_wide.columns.values
]
pipehist_wide.columns = pipehist_wide.columns.astype(str).str.replace(' ', '_')
pipehist_wide.reset_index(inplace=True)

'''
"Yield_Opportunity", "Pipeline_at_100_Percent_Recognized",
               "Pipeline_at_100_Percent_Active_Unrecognized"
'''
pipe_cols = pipehist_wide.columns
remove_list = ["Pipeline", "Pipeline_at_100_Percent", "Yield", 
               "Yield_Opportunity"
               ]
pipe_cols = difflist(projectactuals_cols, remove_list)
#print(pipe_cols)
    
numeric_cols_pipe = numeric_cols
numeric_cols_pipe = numeric_cols_pipe + pipe_cols
pipehist_wide.drop(
    columns=["Yield_Opportunity"], axis=1, inplace = True)
#print(numeric_cols_pipe)

numeric_cols = numeric_cols_pipe
pipehist_wide = convert_date_cols(pipehist_wide)
pipehist_wide = coerce_to_numeric(pipehist_wide, numeric_cols)
pipehist_wide = coerce_to_int(pipehist_wide, int_cols)

pipehist_wide = pipehist_wide.sort_values(by=["End_of_Month", "Snapshot_Date", "Sub_Region_Code"]).reset_index(drop=True)

# print(' pipehist_wide: ')

# pipehist_wide.query('Fin_Entity_ID==@entity_debug')

print("pipehist_wide: ", pipehist_wide["Snapshot_Date"].max())
print("Pipehist_wide:", pipehist_wide.shape)

# pd.DataFrame(pipehist_wide.describe(include='all')) # Story No. 3196 modified Mukesh Dutta 7/13/2021

# pipehist_wide.tail(10)  # .query('Relative_Snapshot_Month_Offset == 0') # Story No. 3018 modified Mukesh Dutta 9/3/2021


# COMMAND ----------

# DBTITLE 1,Final merge
# FINAL MERGE with Pipeline and Pipeline Trend
print("Preparing merge_final")

'''
# Old version
# Get last (1) rows of each group for Pipeline Trend
pipetrend2 = pipetrend2.sort_values(by=["End_of_Month", "Sub_Region_Code", "Snapshot_Date"]).reset_index(drop=True)
#print("eom_region_snp1:")
#eom_region_snp1.info()
#display(eom_region_snp1.tail())

pipetrend2 = (
    pipetrend2
    .groupby(["End_of_Month", "Sub_Region_Code"])
    .tail(1)
)
pipetrend2.drop("Snapshot_Date", axis = 1, inplace = True)
#display(pipetrend2)

merge_df2 = pd.merge(
    merge_df1,
    pipetrend2,
    how="left",
    on=["End_of_Month", "Sub_Region_Code"]
).fillna(nan_value)
'''
# Merge with Pipelinte Trend using merge_asof with the nearest snapshot_date

print("Sort Order:", final_sort_order)
#display(pipetrend1)
# set the sort order same before the fuzzy merge
pipetrend2 = pipetrend1.drop("End_of_Month", axis = 1) #, inplace = True
pipetrend2 = pipetrend2.sort_values(by=["Snapshot_Date", "Sub_Region_Code"])
merge_df1 = merge_df1.sort_values(by=["Snapshot_Date", "Sub_Region_Code"])
#print("merge_df1: ", merge_df1["Snapshot_Date"].max())
#print("pipetrend2: ", pipetrend2["Snapshot_Date"].max())

# Start - Story No. 3404 modified Mukesh Dutta 9/21/2021
"""
merge_df2 = pd.merge_asof(
    left=merge_df1,
    right=pipetrend2,
    on=["Snapshot_Date"],
    by=["Sub_Region_Code"],
    direction='nearest' 
).fillna(nan_value)
"""
merge_df2 = pd.merge(
    merge_df1,
    pipetrend2,
    how="left",
    on=["Snapshot_Date", "Sub_Region_Code"]
).fillna(nan_value)
# End - Story No. 3404 modified Mukesh Dutta 9/21/2021

merge_df2 = convert_date_cols(merge_df2)
merge_df2 = coerce_to_numeric(merge_df2, numeric_cols)
merge_df2 = coerce_to_int(merge_df2, int_cols)
#display(merge_df2.query('Relative_Month_Offset == -1 and Sub_Region_Code == "North America"'))

# Merge with Pipeline History
pipehist_wide1 = pipehist_wide.copy()
merge_df2 = merge_df2.sort_values(by=final_sort_order,ascending=final_sort_order_ascending)
pipehist_wide1 = pipehist_wide1.sort_values(by=sort_cols_snp)

merge_final = pd.merge(
    merge_df2,
    pipehist_wide1,
    how="left",
    on=[
        "Snapshot_Date",
        "End_of_Month",
        "Sub_Region_Code",
        "Relative_Snapshot_Month_Offset",
        "Relative_Month_Offset"
    ],
).fillna(nan_value)
# print(merge_final.info())
# print(merge_final.tail())
# merge_final.drop(columns=['Snapshot_Date_Short','Relative_Snapshot_Month_Offset'], axis=1, inplace = True)
# New Code end
#display(merge_final.tail())
merge_final = merge_final.infer_objects()
merge_final = convert_date_cols(merge_final)
merge_final = coerce_to_numeric(merge_final, numeric_cols)
merge_final = coerce_to_int(merge_final, int_cols)

# Start - Story No. 3018 modified Mukesh Dutta 9/3/2021 
# Merge Global with NA 
merge_final = merge_final \
                .replace({'Sub_Region_Code': 'NA'}, {'Sub_Region_Code': 'Global'})\
                .groupby(['Snapshot_Date','End_of_Month','Sub_Region_Code','Relative_Month_Offset','Relative_Snapshot_Month_Offset',
                          'Relative_EOM_Snp_Month_Offset','Snapshot_Day_of_Month','Snp_Seq_No',
                          'EURONEXT_100','FTSE_100','Nikkei_225','SP_500','SSE_Composite_Index']).agg('sum') \
              .reset_index()
merge_final = merge_final.query('Sub_Region_Code not in "NA"') 
# End - Story No. 3018 modified Mukesh Dutta 9/3/2021

merge_final = merge_final.sort_values(by=final_sort_order, ascending=final_sort_order_ascending).reset_index(drop=True)


merge_final.to_csv(output_data_path + "merge_final.csv", index=False)
merge_final.to_parquet(output_data_path + "merge_final.parquet", index=None)
#pipehist1.to_csv(output_data_path + "pipehist1_ts.csv", index=False)

"""
#merge_final_sdf = spark.createDataFrame(merge_final);
#merge_final_sdf.repartition(1).write.format("parquet").mode("overwrite").parquet(output_data_path + 'merge_final.parquet')
"""
print("merge_final:", merge_final.shape)
# merge_final.tail()
#print(merge_final["Snapshot_Date"].unique())
# merge_final.query('Relative_Snapshot_Month_Offset == 0')
# show_stats(merge_final)
# pd.DataFrame(merge_final.describe(include='all').T) # Story No. 3196 modified Mukesh Dutta 7/13/2021
merge_final.tail(10) # Story No. 3018 modified Mukesh Dutta 9/3/2021
display(merge_final)

# COMMAND ----------

#merge_final.query('Relative_Month_Offset == -1 and Sub_Region_Code == "North America" and Snp_Seq_No == @max_seq').tail()

# COMMAND ----------

# merge_final["Snapshot_Date"].max() # User Story 3404
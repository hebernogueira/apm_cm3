# Databricks notebook source
#dbutils.widgets.remove("date")
#dbutils.widgets.text("last_occurence", "20211215_pba_hfm_936005_al21a_F1")

# COMMAND ----------

##-- CREATE WIDGETS
#dbutils.widgets.text("event_type", "rule")
#dbutils.widgets.text("production_date", "20211215")
#dbutils.widgets.text("plant", "pba")
#dbutils.widgets.text("machine_center", "hfm")
#dbutils.widgets.text("condition_id", "al21a_F1")

# COMMAND ----------

# MAGIC %run
# MAGIC /config/secret_gen2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing libraries

# COMMAND ----------

# import libraries
import pandas as pd
from datetime import datetime
import numpy as np
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from pyspark.sql.functions import *
from tslearn.preprocessing import TimeSeriesResampler

#import pytz
#from azure.storage.filedatalake import DataLakeServiceClient
#from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run
# MAGIC ././cm3_pm_function_library

# COMMAND ----------

# MAGIC %run
# MAGIC ./PlotAPI_AuxiliarFunctions

# COMMAND ----------

# MAGIC %run
# MAGIC ../Rule_JSON/cm3_ReadJsonFromADLS

# COMMAND ----------

# Takes 10 seconds
#dbutils.notebook.run("./hfm_pm_function_library",60)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get inputs from call

# COMMAND ----------

def cm3_collect_needed_signals(rule_dict = {}, condition_id = ''):
  # Filter only needed columns
  keep = []
  cols_to_keep =[]
  for keys, value in rules['alarms'].items():
    if keys==condition_id:
      if value['rule_type'] =='independent':
          keep.append(value['input_signal'])
          if 'trimmer' in value.keys():
            if value['trimmer']['trim_needed'].lower() == 'true':
              keep.append(value['trimmer']['trim_sig'])
          if 'scale' in value.keys():
            if value['scale']['scale_needed'].lower() == 'true':
              keep.append(value['scale']['scale_sig']) 
      elif value['rule_type'] =='nested':
        for al in rules['alarms'][condition_id]['combine']['components']:
         #print(al)
          value = rules['alarms'][al]
          #print(value['rule_type'],'~~')
          keep.append(value['input_signal'])
          if 'trimmer' in value.keys():
            if value['trimmer']['trim_needed'].lower() == 'true':
              keep.append(value['trimmer']['trim_sig'])
          if 'scale' in value.keys():
            if value['scale']['scale_needed'].lower() == 'true':
              keep.append(value['scale']['scale_sig']) 
    #else:
      #print(f'{keys} not selected')

  #flat the list of signals    
  flat_list = [item for sublist in keep for item in sublist]

  fflat_list = []
  for item in flat_list:
    if isinstance(item, list):
      fflat_list = fflat_list + item
    elif isinstance(item, str):
      fflat_list.append(item)
    else:
      print("Something is wrong")
  fflat_list = list(set(fflat_list))

  cols_to_keep = fflat_list 
  return cols_to_keep

# COMMAND ----------

# Input parameter: last_ocurrence
# eg: last_occurence = "20211215_pba_hfm_936005_al21a_F1"
try:
  #last_occurence = dbutils.widgets.get("last_occurence")
  event = dbutils.widgets.get("event_type")
  date = dbutils.widgets.get("production_date")
  plant = dbutils.widgets.get("plant")
  machine_center = dbutils.widgets.get("machine_center")
  condition_id = dbutils.widgets.get("condition_id")
  year, month, day = int(date[0:4]), int(date[4:6]), int(date[6:])
  SHOW_LOCAL_NAMES = False
  
  if machine_center=='HTM':
    machine_center = 'hfm'
  
  
  #-- get last coil id with the alarm
  status_table = f'daaentdevg2.{plant}_apm_{machine_center}_statustable'
  read_table = f'opsentprodg2.{plant}_{machine_center}_time'
  global_to_local_dict = global_to_local_name(plant=plant,machine_center=machine_center)
  
  
  #TODO: LIMIT 10 as the LIMIT 1 (last event coil) is not yet in the hfm time as we check for parquets in HFM
  # When HFM pipeline changed from parquets to Delta, then try LIMIT 1 and rdd.collect[0]
  query = f'''SELECT Coil_ID, Status_Detail
              FROM {status_table} 
              WHERE Year={year}
                    AND Month = {month}
                    AND Day = {day}
                    AND Alarm_ID="{condition_id}" 
                    AND Status=1
              ORDER BY Production_time DESC
              LIMIT(20)'''
  print(query)
  coil_id = [x[0] for x in spark.sql(query).select('Coil_ID').rdd.collect()][-1]
  status_detail = [x[0] for x in spark.sql(query).select('Status_Detail').rdd.collect()][-1]
  
  # Input parameter: last_ocurrence
  # eg: last_occurence = "20211215_pba_hfm_936005_al21a_F1"
  ##--- Extract only the needed signals from the json
  cols_to_keep ,additional_signals,trimmer_signals = collect_needed_signals(rules['alarms'], condition_id=condition_id)

  ##--- Extract the signals which raised the alarms
  rules['alarms'][condition_id] = json.loads(json.dumps(rules['alarms'][condition_id]).lower())
  status_detail_list = [x.lower() for x in flatten_list(eval(status_detail))]

  ##--- Ingest
  alarm_pdf = ingest_and_resample(columns = cols_to_keep,table=read_table, year=year, month=month, day=day,coil_id=coil_id )

  ##--- Collect alloy, Production timestamps
  alloy = alarm_pdf['cg_gen_alloy'].mode().values[0]
  if not isinstance(alloy, str):
    alloy = str(int(alloy))
  start_timestamp, start_date_str = alarm_pdf['time'].min(), str(alarm_pdf['time'].min())[:19]
  end_timestamp = alarm_pdf['time'].max() 
  
  ##--- Call functions to customize text labels
  texts_dict = plot_texts(plant=plant,machine=machine_center,condition_id=condition_id, coil_id= coil_id, alloy=alloy, production_date=start_date_str )
    
  ##--- Fix rules json differences
  alarm_pdf,rules['alarms'][condition_id] = pba_hfm_handlejsons_differences(alarm_pdf = alarm_pdf, al=condition_id)
  
  ##--- Evaluate data with json
  alarm,stats,res = eval(rules['alarms'][condition_id]['function'])( pdf = alarm_pdf, sig = status_detail_list, args = rules['alarms'][condition_id]['args'] )
  if not isinstance(rules['alarms'][condition_id]['args']['threshold'],list):
    alarm_pdf[texts_dict['threshold']] = rules['alarms'][condition_id]['args']['threshold']
  else:
    for i,v in enumerate(rules['alarms'][condition_id]['args']['threshold']):
      alarm_pdf[f'{texts_dict["threshold"]}_{i}'] = v
  
  ##--- Mount result
  alarm_result = pd.DataFrame(index=alarm_pdf.index)
  alarm_result = alarm_result.reset_index().merge(res.reset_index(),on='index', how='left').drop(columns='index')
  alarm_result.isna().sum()
  alarm_result.columns = [str(x) + '_' +texts_dict['result'] for x in alarm_result.columns]
  alarm_result = alarm_result.reset_index()
  alarm_pdf = alarm_pdf.reset_index().merge(alarm_result , how='left', on='index')
  
  ##--- Collect auxiliary signals
  if "trimmer_signals" in locals():
    #trimmers = [x.lower() for x in list(set(trimmer_signals))]
    trimmers = [x.lower() for x in flatten_list(trimmer_signals)]
  elif "trim_signal" in locals():
    #trimmers = [x.lower() for x in list(set(trimmer_signals))]
    trimmers = [x.lower() for x in flatten_list(trimmer_signals)]
  else:
    trimmers = []

  if "additional_signals" in locals():
    additional_signals = [x.lower() for x in flatten_list(additional_signals)]
  else:
    additional_signals = [ ]
    
  result_list = [x for x in alarm_pdf.columns if texts_dict['result'] in x ] + [x for x in alarm_pdf.columns if texts_dict['threshold'] in x ]
  add_signals_trimers = trimmers + additional_signals
  try:
    add_signals_trimers.remove('eg_timeindex')
  except:
    pass
  
  
  ##--Resample Data for plot
  alarm_pdf = resample_pdf(alarm_pdf.drop(columns=['time']))
  alarm_pdf['time'] = pd.date_range(start = start_timestamp,
                                    end = end_timestamp,
                                    periods = alarm_pdf.shape[0])
  
  ##--- Translate global names to local names
  if SHOW_LOCAL_NAMES:
    alarm_pdf = alarm_pdf.rename(columns=global_to_local_dict)
    status_detail_list = [global_to_local_dict[x] if x in global_to_local_dict.keys() else x for x in status_detail_list ]
    add_signals_trimers = [global_to_local_dict[x] if x in global_to_local_dict.keys() else x for x in add_signals_trimers ]
    result_list = [global_to_local_dict[x] if x in global_to_local_dict.keys() else x for x in result_list ]
  
  
  figjson = plot_multiple_charts(pdf = alarm_pdf,
                                 x_axis = 'eg_timeindex',
                                 signal_list = [ status_detail_list, add_signals_trimers,result_list],
                                 title = texts_dict['plot_title']
                                )
  
except Exception as e:
  df_error = pd.DataFrame(index =[0],columns=['error','eg_timeindex'])
  coil_id = ''
  alloy = ''
  start_date_str=''
  texts_dict = plot_texts(plant=plant,
                          machine=machine_center,
                          condition_id=condition_id, 
                          coil_id= coil_id, 
                          alloy=alloy, 
                          production_date=start_date_str )
  figjson = plot_multiple_charts(df_error, signal_list = [['error']],title = texts_dict['error_message'])
  
  print(e)
  

# COMMAND ----------

figjson

# COMMAND ----------

dbutils.notebook.exit(figjson)

# COMMAND ----------

def plot_chart_from_json(json_plot =''):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.io as pio
    fig = pio.from_json(json_plot)
    return fig

# COMMAND ----------

plot_chart_from_json(figjson)

# COMMAND ----------



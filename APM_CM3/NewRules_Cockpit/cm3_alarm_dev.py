# Databricks notebook source
#dbutils.widgets.text("alarm_id", "test-json")
#dbutils.widgets.remove("alarm_id")

# COMMAND ----------

# import libraries
import pandas as pd
from datetime import datetime
import numpy as np
import json
import pytz
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from azure.storage.filedatalake import DataLakeServiceClient

# COMMAND ----------

# MAGIC %run
# MAGIC /config/secret_gen2

# COMMAND ----------

# MAGIC %run
# MAGIC /Repos/heber.nogueira@novelis.com/APM_CM3/APM_CM3/cm3_pm_function_library

# COMMAND ----------

# MAGIC %run
# MAGIC ./test-json

# COMMAND ----------

# MAGIC %run
# MAGIC /Repos/heber.nogueira@novelis.com/APM_CM3/APM_CM3/PBA_CM3_PlotAPI/PlotAPI_AuxiliarFunctions

# COMMAND ----------

rules = {}
r  = json.loads(rule)
rules['alarms'] = r
rules

# COMMAND ----------

#coil_ids = ['942865']
#production_dates =['01/28/2022 22:55:00']
#945018 02/02/2022
coil_ids = ['945018']
production_dates =['02/02/2022 21:29:00']

try:
  coil_id_string = dbutils.widgets.get("coil_id")
  production_date_string = dbutils.widgets.get("production_date")
  coil_ids = coil_id_string.split(",")
  production_dates = production_date_string.split(",")
except:
  pass


# Find earliest date
earliest_timestamp = pd.to_datetime(production_dates, format = '%m/%d/%Y %H:%M:%S').min()
min_day = earliest_timestamp.day
min_month = earliest_timestamp.month
min_year = earliest_timestamp.year
print(min_day, min_month, min_year)

# COMMAND ----------

coilid             = 'cg_gen_coilid'
alloy              = 'cg_gen_alloy'


# CM3 read table
MACHINE_TIME_TABLE = 'opsentprodg2.pba_cm3_time'
QUEUE_TABLE = 'opsentdevg2.pba_cpm_cm3_queue'
#CM3 write tables
STATUS_TABLE = ''

#define base path to access data lake
#base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

# COMMAND ----------

for al in rules['alarms'].keys():
  if rules['alarms'][al]['rule_type']=='independent':
    print(al,':', rules['alarms'][al]['function'])
  elif rules['alarms'][al]['rule_type']=='nested':
    for al1 in rules['alarms'][al]['combine']['components']:
      print(al1,':', rules['alarms'][al1]['function'])
  #print(al)

# COMMAND ----------

# DBTITLE 1,Ingest data from table
#query = """select {} from {} where  year>={} and month>={} and day>={} and cg_gen_coilid={} order by time""".format(",".join(cols_to_keep), read_table, date[:4], date[4:6], date[6:],coil_id)

query = f"""select * from {MACHINE_TIME_TABLE} where  year>={min_year} and month>={min_month} and day>={min_day} and cg_gen_coilid in ({",".join(coil_ids)}) order by time"""
print(query)
df = spark.sql(query)

# COMMAND ----------

row_count = df.count()

# exit notebook if df.count==0
if row_count==0:
    print(''' Zero rows for the selected coilid''')

# COMMAND ----------

# Filter only needed columns
keep = []
for keys, value in rules['alarms'].items():
  if value['rule_type'] =='independent':
      keep.append(value['input_signal'])
      if 'trimmer' in value.keys():
        if value['trimmer']['trim_needed'].lower() == 'true':
          keep.append(value['trimmer']['trim_sig'])
      if 'scale' in value.keys():
        if value['scale']['scale_needed'].lower() == 'true':
          keep.append(value['scale']['scale_sig'])  

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

cols_to_keep = fflat_list #[]'cg_Gen_Coilid', 'cg_Gen_EntryWidth_PDI', 'pg_Gen_Max_EntryWidth','cg_Gen_Alloy']
print(len(cols_to_keep))
#cols_to_keep

# COMMAND ----------

existent_signals = [x.lower() for x in cols_to_keep if x.lower() in df.columns] + ['cg_gen_coilid','eg_timeindex','dl_st1_wedgefactor','dl_excent_wedgefactor','dg_enwidth_width_meas','dl_incooltable_widthadjustment_a_meas','dl_incooltable_widthadjustment_b_meas','eg_st2_millstatus']

existent_signals = list(set(existent_signals))
missing_signals = [x.lower() for x in cols_to_keep if x.lower() not in df.columns]
print(f' #Existing signals {len(existent_signals)}\n', '#Missing Signals ',len(missing_signals))
missing_signals

# COMMAND ----------

'dg_InCoolTable_NumberActiveSlots'.lower() in df.columns

# COMMAND ----------

pdf_coils = df.select(*existent_signals).toPandas()
pdf_coils

# COMMAND ----------

def calculate_process_phases(pdf):
  print('FASES PROCESSO')
  try:
    pdf['st2_acc'] = pdf['dg_st2_speed_meas'].diff(5000)
    pdf['st2_acc_grad'] = np.gradient(pdf['dg_st2_speed_meas'],5000)
    pdf.fillna(method='bfill',inplace=True)

    #%%
    pdf['st2_acc_grad_bt']= ((pdf['st2_acc']> 0.10) & (pdf['eg_exit_length'] < pdf['eg_exit_length'].max()/2) & (pdf['eg_exit_length']>10 )).astype('int16')

    pdf['st2_acc_grad_bf']= ((pdf['st2_acc']<-0.10) & (pdf['eg_exit_length'] > pdf['eg_exit_length'].max()/2) & (pdf['eg_exit_length']>10)).astype('int16')

    #%%
    e1 = get_edges(pdf, 'st2_acc_grad_bt', threshold = 1, op='>=', index='eg_timeindex')
    r_edges_acc = e1[e1['type']==True]
    r_edge_index_acc = r_edges_acc['index'][r_edges_acc['range']==r_edges_acc['range'].max()].index[0]

    #%%
    e2 = get_edges(pdf, 'st2_acc_grad_bf', threshold = 1, op='>=', index='eg_timeindex')
    r_edges_dcc = e2[e2['type']==True]
    r_edge_index_dcc = r_edges_dcc['index'][r_edges_dcc['range']==r_edges_dcc['range'].max()].index[0]

    pdf['acc_phase'] = 0
    pdf.loc[:e1['index'][r_edge_index_acc+1],'acc_phase']=1

    pdf['dec_phase'] = 0
    pdf.loc[e2['index'][r_edge_index_dcc]:,'dec_phase']=1

    pdf['steady_phase'] = 0
    pdf['steady_phase'] = ((pdf['acc_phase']==0)&(pdf['dec_phase']== 0)).astype('int16')
  except:
    pdf['acc_phase'] = pdf['eg_st2_millstatus'][pdf['eg_st2_millstatus']==1].astype('int16')
    pdf['steady_phase'] = pdf['eg_st2_millstatus'][pdf['eg_st2_millstatus']==2].astype('int16')
    pdf['dec_phase'] = pdf['eg_st2_millstatus'][pdf['eg_st2_millstatus']==1].astype('int16')
  
  wedge_dict= {'trimmer':{'trim_first_value':0,
                              'trim_first_op':'==',
                              'trim_last_value':0,
                              'trim_last_op':'>',
                              'trim_modifier':'none'
                              }
                  }


  non_wedge_dict= {'trimmer':{'trim_first_value':0,
                              'trim_first_op':'==',
                              'trim_last_value':0,
                              'trim_last_op':'>',
                              'trim_modifier':'not'
                              }
                  }
  pdf['wedge'] = xMarkRange(pdf,'dl_excent_wedgefactor',args=wedge_dict).astype('int16')
  pdf['out_of_wedge'] = xMarkRange(pdf,'dl_excent_wedgefactor',args=non_wedge_dict).astype('int16')
  return pdf

# COMMAND ----------

def evaluate_single_rule(df,al,rules,sig):
  comment = [al]
  cols =[]
  status_detail =[]
  #for idx,sig in enumerate(rules['alarms'][al]['input_signal'].copy()):
  if type(sig) is list:
      cols = cols + sig
      comment.append('signal: '+ ",".join( s for s in sig))
  else:
      cols = cols + [sig]
      comment.append('signal: '+ sig)

  try:
      alarm, stats_dict = eval(rules['alarms'][al]['function'])(pdf=df, 
                                          sig=sig,
                                          args=rules['alarms'][al])
      #status_detail=comments
      if alarm:
          status_detail.append(sig)
          status = 1
          comment[1] = comment[1]+' --> !!!Alarm!!!'
      else:
          status = 0
          comment[1] = comment[1]+' --> OK'
  except Exception as e:
          status = 999999
          comment[1] = comment[1]+' --> FAILED '
          print('ERROR: ',e)
          pass
  return (status, status_detail, comment,stats_dict)

# COMMAND ----------

# MAGIC %run
# MAGIC /Repos/heber.nogueira@novelis.com/APM_CM3/APM_CM3/cm3_pm_function_library

# COMMAND ----------

print('ANALISANDO')
alarms_result = {}
alarms_list = []
stats_list = []

coils_pdf = pdf_coils.copy()
for i,coil_id in enumerate(coil_ids):
  ## Filters on coil of interest & extracts information for populating Coil_Basic_Info Table
  alarms_result[coil_id]={}
  production_date = datetime.strptime(production_dates[i],'%m/%d/%Y %H:%M:%S')
  str_prod_date = production_date.strftime("%Y-%m-%d %H:%M:%S")
  print('Actual coil_id ', coil_id)
  try:
    coil_pdf = coils_pdf[coils_pdf['cg_gen_coilid'] == coil_id].reset_index(drop=True)
    coil_pdf = calculate_process_phases(pdf=coil_pdf)
  except:
    pass
  
  for al in rules['alarms']:
      machine_part = rules['alarms'][al]['process_id']
      status_detail = []
      comment = [al]
      cols =[]
      status = 0

      try:

        if (rules['alarms'][al]['rule_type']=='independent'):
            #logger.info('--- Processing rule {r}.'.format(r=al))
            print("--- Processing rule: ", al)
            stats_dict = {'coil_id': coil_id,'alarm_id': al }
            for idx,sig in enumerate(rules['alarms'][al]['input_signal'].copy()):
                #print(sig)
                (status,status_detail,comment,stats) = evaluate_single_rule(df=coil_pdf, al=al,rules=rules,sig=sig)
                stats_dict.update(stats)
                stats_list.append(stats_dict)
                alarms_result[coil_id][al]={'Alarm_ID': al, 
                                            'Coil_ID': coil_id, 
                                            'Production_date': str_prod_date,
                                            'Status': status, 
                                            'Status_Detail': str(status_detail), 
                                            'Comment': str(comment), 
                                            'Machine_Partid': machine_part }
                alarms_list.append(alarms_result[coil_id][al])
        elif (rules['alarms'][al]['rule_type']=='nested'):
            #logger.info('--- Processing rule {r}.'.format(r=al))
            print("--- Processing rule: ", al)
            n_aux_rules = len(rules['alarms'][al]['combine']['components'])
            partial_condition = []
            pstatus = [''] * n_aux_rules
            pstatus_detail = [''] * n_aux_rules
            pcomment = [''] * n_aux_rules
            pstats = [''] * n_aux_rules
            
            stats_dict = {'coil_id': coil_id,'alarm_id': al }
            for jdx, aux_rule in enumerate(rules['alarms'][al]['combine']['components']):
                for idx,sig in enumerate(rules['alarms'][aux_rule]['input_signal'].copy()):
                    #Call function to evaluate each separate rule

                    (pstatus[jdx],pstatus_detail[jdx],pcomment[jdx],pstats[jdx]) = evaluate_single_rule(df=coil_pdf,
                                                                                            al=aux_rule,
                                                                                            rules=rules,
                                                                                            sig=sig)
                    comment.extend(pcomment[jdx])

            #status_detail = pstatus_detail
            status_detail = [x[0] for x in pstatus_detail if len(x)>0] 
            #TODO
            try:
              
              stats_dict.update(pstats[0])
              stats_list.append(stats_dict)
            except:
              pass
            
            #- Calculate the output for compound rule
            alarm_cond =''
            if (n_aux_rules>1):
                for i in range(0,n_aux_rules-1):
                    alarm_cond += '''{status} {logic_op} '''.format(status=pstatus[i],\
                                                                    logic_op=rules['alarms'][al]['combine']['logic_op'][i])

            alarm_cond += '''{status} '''.format(status=pstatus[n_aux_rules-1])
            status = eval(alarm_cond)
            alarms_result[coil_id][al]={'Alarm_ID': al, 
                                            'Coil_ID': coil_id, 
                                            'Production_date': str_prod_date,
                                            'Status': status, 
                                            'Status_Detail': str(status_detail), 
                                            'Comment': str(comment), 
                                            'Machine_Partid': machine_part }
            alarms_list.append(alarms_result[coil_id][al])   
      except Exception as e:
        print(al, sig, e)
        pass


#%%
alarm_df = pd.DataFrame(alarms_list)

# COMMAND ----------

alarm_df

# COMMAND ----------

rules

# COMMAND ----------

# MAGIC %run
# MAGIC /Repos/heber.nogueira@novelis.com/APM_CM3/APM_CM3/cm3_pm_function_library

# COMMAND ----------

def threshold_compare_mod(pdf, sig, args):
  if args['trimmer']['trim_needed'].capitalize()=='True':
    pdf['trim'] = generate_boolean_series(df = pdf, 
                                      cols = args['trimmer']['trim_sig'], 
                                      operators= args['trimmer']['trim_op'],
                                      thresholds = args['trimmer']['trim_thr'],
                                      logic_operation=args['trimmer']['trim_logic_op'],
                                      offset_needed= args['trimmer']['trim_offset_needed'],
                                      offset_start_sec = args['trimmer']['trim_offset_start'], 
                                      offset_end_sec = args['trimmer']['trim_offset_end'], 
                                      sample_time_ms = 1)
    pdf = pdf[pdf['trim']==True]
    #pdf['res'] = pdf['sig']
  
    #alarm  = False
    alarm = (rel_ops[args['cond_par']['cond_op'][0]](pdf[sig],args['cond_par']['cond_thresholds']['threshold'])).any()
    
    print(sig)
    #if args['how'] == 'any':
    #    alarm = alarm.any()
    #else:
    #    alarm = alarm.all()
    
    stats_dict = {'thresholds': [args['cond_par']['cond_thresholds']['threshold']], 
                     'min': 0,'max': 0, 'mean': 0, 'median': 0, 'std': 0,'mape': 0, 'zscore': 0}
  
    return alarm,stats_dict

# COMMAND ----------

evaluate_single_rule(df=coil_pdf, al=al,rules=rules,sig=sig[0])

# COMMAND ----------

alarm

# COMMAND ----------

stop

# COMMAND ----------

st_df = pd.DataFrame(stats_list)

# Find the row which has the maximum dev for an alarm_id
i_max = st_df.groupby(['coil_id','alarm_id'])['mape'].transform('max') == st_df['mape']
sttc_df = st_df[i_max].drop_duplicates(subset=['coil_id','alarm_id'],keep='first')
sttc_df['thresholds'] = sttc_df['thresholds'].apply(lambda x: str(x))
#statistics_df = spark.createDataFrame(sttc_df).withColumn('thresholds',col('thresholds').cast(StringType()))
sttc_df['deviation'] = sttc_df['zscore'].map(lambda x: zscore_to_deviation(x))#.head(20)
sttc_df = sttc_df.rename(columns={'coil_id':'Coil_ID',
                                  'alarm_id':'Alarm_ID'
                                 })

# COMMAND ----------

#TODO: Extract Year Month and day in the main loop
status_pdf1 = alarm_df.copy()
#status_pdf1['Production_date'] = pd.to_datetime(status_pdf1['Production_date']+'.000 -0300',format='%Y-%m-%d %H:%M:%S')
status_pdf1['Production_date'] = pd.to_datetime(status_pdf1['Production_date'],format='%Y-%m-%d %H:%M:%S')
status_pdf1['Year'] = status_pdf1['Production_date'].dt.year
status_pdf1['Month'] = status_pdf1['Production_date'].dt.month
status_pdf1['Day'] = status_pdf1['Production_date'].dt.day

# COMMAND ----------

import random

dev = [2, 4, 6]
dev_factor = random.choice(dev)

# COMMAND ----------

STATUS_TABLE = 'daaentdevg2.pba_apm_cm3_statustable'

left_cols = ['Coil_ID','Alarm_ID','Production_date','Status','Status_Detail','Year','Month','Day','Comment']
right_cols = ['Coil_ID','Alarm_ID','min','max','mean','std','mape','thresholds','deviation']

alarms_pdf = pd.merge(status_pdf1[left_cols],
                       sttc_df[right_cols],
                      how='left',
                       on=['Coil_ID','Alarm_ID']
                       )
alarms_pdf['EventType'] = 'rule'
alarms_pdf = alarms_pdf.rename(columns={'deviation':'Deviation'})


alarms_pdf['Deviation'] = dev_factor*alarms_pdf['Status']
alarms_pdf['Acknowledge'] = False
alarms_pdf['Sync'] = False

#alarms_pdf['Production_time'] = pd.to_datetime(alarms_pdf['Production_date'],format='%m/%d/%Y %H:%M:%S-03:00')#'%Y-%m-%d %H:%M:%S')
alarms_pdf['Production_time'] = pd.to_datetime(alarms_pdf['Production_date'],format='%m/%d/%Y %H:%M:%S')#'%Y-%m-%d %H:%M:%S')
alarms_pdf['Production_date'] = alarms_pdf['Production_time'].dt.date

alarms_pdf = alarms_pdf.sort_values(by=['Production_time','Alarm_ID'])
alarms_pdf = alarms_pdf.drop_duplicates()#duplicated().sum()
#alarms_pdf

# COMMAND ----------

max_rownum = [x[0] for x in spark.sql(f'''SELECT MAX(rownum) FROM {STATUS_TABLE} ''').rdd.take(1)]
alarms_pdf['rownum'] = alarms_pdf.index+1+max_rownum[0]

#
alarms_pdf['rownum'] = alarms_pdf['rownum'].astype('int32')
alarms_pdf['Coil_ID'] = alarms_pdf['Coil_ID'].astype('int32')
alarms_pdf['Year'] = alarms_pdf['Year'].astype('int32')
alarms_pdf['Month'] = alarms_pdf['Month'].astype('int32')
alarms_pdf['Day'] = alarms_pdf['Day'].astype('int32')
alarms_pdf['Status'] = alarms_pdf['Status'].astype('int32')
alarms_pdf['Deviation'] = alarms_pdf['Deviation'].astype('int32')


alarms_pdf['min'] = alarms_pdf['min'].astype('float32')
alarms_pdf['max'] = alarms_pdf['max'].astype('float32')
alarms_pdf['mean'] = alarms_pdf['mean'].astype('float32')
alarms_pdf['mape'] = alarms_pdf['mape'].astype('float32')
alarms_pdf['std'] = alarms_pdf['std'].astype('float32')

#allign
alarms_pdf = alarms_pdf[['rownum', 'EventType', 'Production_date', 'Production_time', 'Coil_ID', 'Alarm_ID', 'Status', 'min', 'max', 'mean', 'std', 'mape', 'Deviation',
 'Year', 'Month', 'Day', 'thresholds', 'Status_Detail', 'Comment', 'Acknowledge', 'Sync']]

alarms_Df = spark.createDataFrame(alarms_pdf)
#alarms_Df.write.format("delta").mode("append").saveAsTable(STATUS_TABLE)
#display(alarms_Df)

# COMMAND ----------

## STATUS TABLE
status_pdf = alarm_df.copy()
status_pdf['Alarm_ID'] = status_pdf['Alarm_ID'].apply(lambda x: x.upper())
status_pdf['Machine_Part_ID'] = status_pdf['Machine_Partid'].apply(lambda x: x.upper())
status_pdf = status_pdf.drop(columns='Machine_Partid')
#status_pdf[['Year','Month','Day','Status_Detail','Comment']] =  status_pdf[['Year','Month','Day','Status_Detail','Comment']].astype(str)

schemaStatusTable = StructType([
  StructField("Alarm_ID", StringType(), True), 
  StructField("Coil_ID", StringType(), True),
  StructField("Production_date", StringType(),True),
  StructField("Status", IntegerType(), True),
  StructField("Status_Detail", StringType(), True),
  StructField("Comment", StringType(), True),
  StructField("Machine_Part_ID", StringType(),True),
  #StructField("Timestamp", StringType(),True),
  
])

status_table = spark.createDataFrame(status_pdf,schema=schemaStatusTable)
#status_table.write.format("delta").mode("append").saveAsTable('daaentdevg2.pba_cpm_cm3_test')

# COMMAND ----------

dbutils.notebook.exit('Finished')

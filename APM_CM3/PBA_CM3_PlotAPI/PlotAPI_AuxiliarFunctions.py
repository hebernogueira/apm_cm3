# Databricks notebook source
# MAGIC %md
# MAGIC ##### Flat a list

# COMMAND ----------

def flatten_list(list_to_flat = []):
  flattened = []
  for item in list_to_flat:
    if isinstance(item, list):
      flattened = flattened + item
    elif isinstance(item, str):
      flattened.append(item)
    else:
      print("Something is wrong")
      
  flattened = list(set(flattened))
  return flattened

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Collect needed signals

# COMMAND ----------

def collect_needed_signals(rule_dict = {}, condition_id = ''):
  fflat_list,flat_list,keep, cols_to_keep = [],[],[],[]
  additional_signals = []
  trimmer_signals =[]

  for keys, value in rule_dict.items():
    if keys==condition_id:
      if value['additional_signal']['add_signal_needed']:
        additional_signals = value['additional_signal']['add_signal']
      else:
        additional_signals = []
      keep.append(additional_signals)
      
      #
      if value['trimmer']['trim_needed']:
        trimmer_signals = value['trimmer']['trim_signal']
      else:
        trimeer_signals = []
      keep.append(trimmer_signals)
      keep.append(value['input_signal'])
      flat_list = [item for sublist in keep for item in sublist]

      for item in flat_list:
        if isinstance(item, list):
          fflat_list = fflat_list + item
        elif isinstance(item, str):
          fflat_list.append(item)
        else:
          print("Something is wrong")
      fflat_list = list(set(fflat_list))

      cols_to_keep = fflat_list + ['cg_Gen_Coilid', 'cg_Gen_Alloy','eg_TimeIndex']
      cols_to_keep = list(set([x.lower() for x in list(set(cols_to_keep))] + ['time']))
    else:
      pass

  return cols_to_keep ,additional_signals,trimmer_signals

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ##### Plot multiple charts

# COMMAND ----------

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
last_occurence = 'test_plotAPI'


def plot_multiple_charts(pdf, signal_list =[], x_axis = "eg_timeindex", text_snippet= "cg_gen_coilid", plot_map=False, vector_signals=[] , title ='' ,file_name = last_occurence):
  if plot_map:
    n_rows = len(signal_list)+1
  else:
    n_rows = len(signal_list)
  n_cols = 1
  fig = make_subplots(rows=n_rows, cols=n_cols, specs=[n_rows*[n_cols*[{"secondary_y": False}]]][0]) 
  for i,sigs in enumerate(signal_list):
    #print(sigs)
    for k,sig in enumerate(sigs):
      #print(sig)
      fig.add_trace(go.Scatter(x=list(pdf[x_axis]), 
                                y = list(pdf[sig]),
                                #text = pdf[text_snippet],
                                name = pdf[sig].name,
                                legendgroup = f'{i+1}'
                                 ),
              row=i+1, col=1,
              secondary_y=False,
              )
      
    #fig['layout'][f'yaxis{i+1}']['range'] = (np.min(pdf[sigs].min())*0.99,np.max(pdf[sigs]).max()*1.01)
    fig['layout'][f'yaxis{i+1}']['range'] = (np.min(pdf[sigs].min())*0.99,np.max(pdf[sigs].max())*1.01)
    
  
  
  if plot_map:
    temp1 = pdf[vector_signals].T
    data_plotly = {'z': temp1.values.tolist(),
                   'x': temp1.columns.tolist(),
                   'y': temp1.index.str[-6:].tolist(),
    #                  'colorscale':'RdBu',
                   'colorscale':[
                     [0, "rgb(255, 0, 0)"],
                     [0.25, "rgb(255, 255, 0)"],
                     [0.5, "rgb(255, 255, 255)"],
                     [0.75, "rgb(0, 255, 0)"],
                     [1, "rgb(0, 0, 255)"],
                   ],
                   'zmid':0,
                   'zmax':5,
                   'zmin':-5,
                  }
    fig.add_trace(go.Heatmap(data_plotly,colorbar_y=0,colorbar_thickness=10),row=n_rows, col=1)
  
  
  #template options: ["plotly", "plotly_white", "plotly_dark", "ggplot2", "seaborn", "simple_white", "none"]
  template = "plotly_white"
  fig.update_layout( template=template,
                   title_text=title,
                   autosize=False,
                   width=1500,
                   height=n_rows*300,
                   legend=dict(bgcolor='rgba(0,0,0,0)',
                            yanchor="top",
                            y=0.99,
                            xanchor="left",
                            x=0.01
                        ),
                   legend_tracegroupgap = 240,
                    hovermode='x unified',
                    hoverlabel=dict( namelength = -1,
                                      bgcolor='rgba(255,255,255)',
                                  )
                   
                   )
  figjson = pio.to_json(fig)
  #figimg = pio.to_image(fig)
  fightml = pio.to_html(fig)
  pio.write_json(fig, file =f'/dbfs/mnt/sandbox/heber_nogueira/PlotAPI/{file_name}.json')
  #pio.write_html(fig, file =f'/dbfs/mnt/sandbox/heber_nogueira/PlotAPI/{file_name}.html')
  return figjson
  #fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Fix rule jsons difference

# COMMAND ----------

def pba_hfm_handlejsons_differences(alarm_pdf, al=''):
  #al = condition_id
  trim_sig = None
  status = 0
  for idx,sig in enumerate(rules['alarms'][al]['input_signal'].copy()):
    cols = []
    #alarm_pdf = pd.DataFrame()
    if rules['alarms'][al]['additional_signal']['add_signal_needed']:
      temp = rules['alarms'][al]['additional_signal']['add_signal'].copy()
      temp = temp[idx]
      if type(temp) is list:
        rules['alarms'][al]['args']['add_signal'] = temp
        cols = temp
      else:
        rules['alarms'][al]['args']['add_signal'] = [temp]
        cols = [temp]
    try:
      if type(sig) is list:      
        cols = cols + sig
      else:
        cols = cols + [sig]
      if rules['alarms'][al]['trimmer']['trim_needed']:
        cols = cols + [rules['alarms'][al]['trimmer']['trim_signal'].copy()[idx]]
        if rules['alarms'][al]['trimmer']['offset']['offset_needed']:
          cols = cols + [rules['alarms'][al]['trimmer']['offset']['offset_index_signal']]
        #alarm_pdf = coil_pdf[cols].copy()
        #alarm_pdf = alarm_pdf.dropna()
        condition = rel_ops[rules['alarms'][al]['trimmer']['op']](
                                                   alarm_pdf[rules['alarms'][al]['trimmer']['trim_signal'][idx]],
                                                   rules['alarms'][al]['trimmer']['trim_value']
                                                  )
        if rules['alarms'][al]['trimmer']['offset']['offset_needed']:
          offset_index = rules['alarms'][al]['trimmer']['offset']['offset_index_signal']
          offset_start = rules['alarms'][al]['trimmer']['offset']['offset_start']
          offset_end = rules['alarms'][al]['trimmer']['offset']['offset_end']
          offset_start += alarm_pdf[condition][offset_index].iloc[0]
          offset_end += alarm_pdf[condition][offset_index].iloc[-1]
          condition = (alarm_pdf[offset_index]>=offset_start)&(alarm_pdf[offset_index]<=offset_end)
        alarm_pdf = alarm_pdf#[condition]
      else:
        alarm_pdf = alarm_pdf#.dropna()
    except Exception as e:
      print ('Error: '+al)
      print (e)
      continue
    #alarm_pdf = alarm_pdf.reset_index()
#    try:
#      rules['alarms'][al]['args']["alarm_id"] = al
#      alarm,stats,l = eval(rules['alarms'][al]['function'])( pdf = alarm_pdf, sig = sig, args = rules['alarms'][al]['args'] )
#    except Exception as e:
#        print("Error ",al, e)
    
    if 'threshold' not in rules['alarms'][condition_id]['args']:
      rules['alarms'][condition_id]['args']['threshold'] = list()
    if 'parts' in rules['alarms'][condition_id]['args'].keys():
      rules['alarms'][condition_id]['args']['threshold'] = flatten_list(rules['alarms'][condition_id]['args']['parts'])[1]
    if 'high_threshold' in rules['alarms'][condition_id]['args'].keys():
      rules['alarms'][condition_id]['args']['threshold'].append(rules['alarms'][condition_id]['args']['high_threshold'])
    if 'low_threshold' in rules['alarms'][condition_id]['args'].keys():
      rules['alarms'][condition_id]['args']['threshold'].append(rules['alarms'][condition_id]['args']['low_threshold'])
    if 'region' in rules['alarms'][condition_id]['args'].keys():
      rules['alarms'][condition_id]['args']['threshold'] = flatten_list(rules['alarms'][condition_id]['args']['region'])[-1]
    #rules['alarms'][condition_id]['args']

    
    return alarm_pdf, rules['alarms'][condition_id]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ingestion and Resample Functions

# COMMAND ----------

def ingest_and_resample(columns, table, year, month, day, coil_id):
  from tslearn.preprocessing import TimeSeriesResampler
  query = """select {} from {} where  year>={} and month>={} and day>={} and cg_gen_coilid={} order by time""".format(",".join(columns), table, year, month, day,coil_id)
  print(query)
  df = spark.sql(query)

  # Convert to Pandas
  alarm_pdf = df.select(cols_to_keep).toPandas()

  # Resample Data
  #pdf_resample = TimeSeriesResampler(sz=1000).fit_transform(alarm_pdf.T)
  #alarm_pdf = pd.DataFrame(pdf_resample[:, :, 0].transpose(), columns= alarm_pdf.columns)
  return alarm_pdf

# COMMAND ----------

def resample_pdf(pdf):
  pdf_resample = TimeSeriesResampler(sz=1000).fit_transform(pdf.T)
  alarm_pdf = pd.DataFrame(pdf_resample[:, :, 0].transpose(), columns= pdf.columns)
  return alarm_pdf
  

# COMMAND ----------

def global_to_local_name(plant='',machine_center=''):
  dict_translate = pd.read_json(f"/dbfs/mnt/sandbox/heber_nogueira/GlobalCatalogs/{plant}_{machine_center}_global_to_local.json").to_dict()['LOCAL_NAME']
  return {k.lower():v for k,v in dict_translate.items()}

# COMMAND ----------

def plot_texts(plant='default',machine='default', condition_id='',coil_id='',alloy='', production_date='' ):
  dict_texts = {'default':{'default':{'plot_title': f'{condition_id} evaluated for Coil {coil_id}',
                                     'error_message': 'ERROR. Please report this bug to admin.  :(',
                                     'threshold': 'threshold',
                                     'result':'result'
                                   }
                        },
              'pba':{'hfm':{'plot_title': f'{condition_id} avaliada para CoilID: {coil_id}. Liga: {alloy}. Data: {production_date}',
                           'error_message': 'ERRO. Por favor, informe este erro para: heber.nogueira@novelis.com.  :(',
                           'threshold': 'limite',
                           'result': 'resultado'
                          },
                     'cm3':{'plot_title': f'{condition_id} avaliada para CoilID: {coil_id}. Liga: {alloy}. Data: {production_date}',
                           'error_message': f'Este recurso não está disponivel para a máquina {machine} :(',
                           'threshold': 'limite',
                           'result': 'resultado'
                          },
                 }
             }
  return dict_texts[plant][machine]
  

# COMMAND ----------



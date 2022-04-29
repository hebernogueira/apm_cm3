# Databricks notebook source
# MAGIC %md
# MAGIC # Function Library - Pinda Pred. Maintenance

# COMMAND ----------

import pandas as pd
from datetime import datetime
import numpy as np

# COMMAND ----------

import operator
rel_ops = {
      '>': operator.gt,
      '<': operator.lt,
      '>=': operator.ge,
      '<=': operator.le,
      '==': operator.eq,
      '=': operator.eq,
      '!=': operator.ne,
      '-': operator.sub,
      '+': operator.add,
      '*': operator.mul,
      '/': operator.truediv,
      'AND': operator.and_,
      'and': operator.and_,
      'OR':operator.or_,
      'or':operator.or_,
      '&': operator.and_,
      '|':operator.or_,
  }

bool_ops = {
    'AND' : '&',
    'and' : '&',
    '&'   : '&',
    'OR'  : '|',
    'or'  : '|',
    '|'   : '|'
}


aggreg_ops = {'max': 'max',
              'avg': 'mean',
              'min': 'min'
}

# COMMAND ----------

def str2bool(v):
      return v.lower() in ("positive", "true", "1")

# COMMAND ----------

def xFirst(df, sig, value, op):
    return df[sig][rel_ops[op](df[sig],value)].first_valid_index()
    
def xLast(df, sig, value, op):
    return df[sig][rel_ops[op](df[sig],value)].last_valid_index()

def xMarkRange(df, sig, args):
    
    start = xFirst(df,sig,args['trimmer']['trim_first_value'],args['trimmer']['trim_first_op'])
    end = xLast(df,sig,args['trimmer']['trim_last_value'],args['trimmer']['trim_last_op'])
    
    #Mark Range True
    rangeSeries = pd.Series(index=df.index, dtype='bool')
    rangeSeries.iloc[start:end+1] = True
    rangeSeries.iloc[end+1:len(df)+1] = False
    
    if args['trimmer']['trim_modifier'] == 'not':
        rangeSeries = ~rangeSeries
        
    return rangeSeries
# %%        
def rate_in_time(pdf,sig,args):
    import warnings
    warnings.simplefilter("ignore")
    if args['trimmer']['trim_needed'].capitalize()=='True':
        pdf['trim'] = generate_boolean_series(df = pdf, 
                                      cols = args['trimmer']['trim_sig'], 
                                      operators= args['trimmer']['trim_op'],
                                      thresholds = args['trimmer']['trim_thr'],
                                      logic_operation=args['trimmer']['trim_logic_op'],
                                      offset_needed= args['trimmer']['trim_offset_needed'],
                                      offset_start_sec = 0, 
                                      offset_end_sec = 0,
                                      sample_time_ms = 1)
        pdf = pdf.loc[pdf['trim']==True,:]
    
    pdf.loc[:,'aux_sig'] = (rel_ops[args['cond_par']['duration_op'][0]](pdf.loc[:,sig],args['cond_par']['cond_thresholds']['threshold'])).astype('int')
    alarm =rel_ops[args['cond_par']['duration_op'][0]](pdf.loc[:,'aux_sig'].mean(),args['cond_par']['duration_threshold']['threshold'])
    #print(sig, ':',pdf['aux_sig'].mean())
    
    #res = pdf[[sig]]
    res = pdf.loc[:,sig]
    #print(pdf[[sig]])
    stats_dict = signal_FE(res,
                           has_alarmed = alarm,
                           thr_list = [args['cond_par']['cond_thresholds']['threshold']],
                           operator = [args['cond_par']['cond_op'][0]],
                           stddev_base = 0)
        
    return alarm, stats_dict  

# COMMAND ----------

def generate_boolean_series(df, cols=[], operators=[], thresholds=[], logic_operation=[''],
                          offset_needed = 'False',offset_start_sec = 0, offset_end_sec = 0,sample_time_ms = 1):
    ''' Function returns a boolean dataframe trimmer from combination of inputs'''
    for i,t in enumerate(thresholds):
        if type(t)=='str':
            try:
                thresholds[i] = eval(t)
            except:
                print("ERROR: Not possible to evaluate threshold condition!")
                
    
    s_cond = ''
    if (len(cols)>1):
        for i in range(0,len(cols)):
            if (i< len(cols)-1) :
                s_cond += '''(rel_ops['{op}'](df['{col}'],{thr})) {log_op} '''.format(op=str(operators[i]),\
                                                                                   col=cols[i],\
                                                                                   thr = thresholds[i],\
                                                                                   log_op=bool_ops[logic_operation[i]])
            else:
                s_cond += '''(rel_ops['{op}'](df['{col}'],{thr})) '''.format(op=str(operators[i]),\
                                                                                   col=cols[i],\
                                                                                   thr = thresholds[i])
    else:
        s_cond += '''rel_ops['{op}'](df['{col}'],{thr}) '''.format(op=str(operators[0]),\
                                                                                   col=cols[0],\
                                                                                   thr = thresholds[0])
    ###--return pandas Series from 
    #print(s_cond)
    trim_series = eval(s_cond)
    
    # If need to offset boolean series
    if(offset_needed.capitalize()=='True'):
        trim_series = time_offset_series(pds = trim_series,
                                        operator = '==',
                                        conditional_value= True,
                                        start = offset_start_sec,
                                        end = offset_end_sec,
                                        sample_rate = sample_time_ms/1000)
    return trim_series



# COMMAND ----------

def time_offset_series(pds, operator, conditional_value, start, end, sample_rate):
    start_index = pds[rel_ops[operator](pds,conditional_value)==True].index[0]
    end_index = pds[rel_ops[operator](pds,conditional_value)==True].index[-1]
    try:
        index_offset_st = np.abs(int(start/sample_rate))
        index_offset_end = np.abs(int(end/sample_rate))
        index_offset_st = int(start/sample_rate)
        index_offset_end = int(end/sample_rate)
    except:
        print('Error when calculating indexes offsets. Check offsets and sample_rate inputs')

    if start>0:
        pds[pds.index<start_index+index_offset_st] = False
    else:
        pds.loc[start_index+index_offset_st:start_index+1] = True
        
    if end >0:
        pds[pds.index>end_index-index_offset_end] = False 
    else:
        pds.loc[end_index: end_index-index_offset_end+1] = True
    
    return pds

# COMMAND ----------

def get_edges(df, target, threshold, op, index):
    df_result = rel_ops[op](df[target], threshold)
    df_change = df_result.shift(1) ^ df_result
    df_change = df_change[df_change == True]
    status = df_result.iloc[0]
    change_range = [{'index': df.first_valid_index(), 'type': status}]
    if len(df_change) >= 1:
        for idx in range(len(df_change)):
            status = ~status
            change_range.append({
                'index':
                df_change[df_change == True].index[idx],
                'type':
                status,
            })

    change_range.append({'index': df.last_valid_index(), 'type': status})
    edges = pd.DataFrame(change_range)
    idx = [n for n in edges['index']]
    edges[index] = (df.iloc[idx][index]).values
    edges['range'] = edges[index].diff().shift(-1)

    return edges

# COMMAND ----------



# COMMAND ----------

def cnt_edges_bool_sig(pdf,sig,args= { 'trim_needed':'false','transform':{'to_boolean':'false'}}):
    ''' Count edges on boolean signal or integer 0,1 signals
    
    -------------------------------------------------------------------------------------------------------------
    eg: Generating a pulse signal and counting the rising and falling edges and comparint to the thresholds
    import pandas as pd
    df = pd.DataFrame([1,1,0,0,0,0,0,1,1,1,1,0,0,0,0,0,0,0,1,1,1,1,1,1,0,1,1,0,1,0,0],columns=['signal'])
    arguments = { 
                  "cond_par": {"cond_thresholds": {"r_edge_thr": 10, "f_edge_thr": 10 },  "cond_op": [">",">"] },
                  "transform": { "to_boolean": "false", "transf_offset_needed": "false" },
                  "trimmer": { "trim_needed": "false", "trim_offset_needed": "false"}
                 }
    al, res = cnt_edges_bool_sig(pdf = df,sig='signal',args = arguments)
    
    '''
    if args['trimmer']['trim_needed'].capitalize()=='True':
        pdf['trim'] = generate_boolean_series(df = pdf, 
                                      cols = args['trimmer']['trim_sig'], 
                                      operators= args['trimmer']['trim_op'],
                                      thresholds = args['trimmer']['trim_thr'],
                                      logic_operation=args['trimmer']['trim_logic_op'],
                                      offset_needed= args['trimmer']['trim_offset_needed'],
                                      offset_start_sec = 0, 
                                      offset_end_sec = 0,
                                      sample_time_ms = 1)
        
    if pdf[sig].dtype =='bool':
        pdf['aux_sig'] = pdf[sig].astype('int16')
    else:
        if (args['transform']['to_boolean'].capitalize()=='True'):
            #print('Transforming to boolean')
            pdf['aux_sig'] = generate_boolean_series(df = pdf, 
                                      cols = [sig], #args['transform']['transf_sig'], 
                                      operators= args['transform']['transf_op'],
                                      thresholds = args['transform']['transf_thr'],
                                      logic_operation=args['transform']['transf_logic_op'],
                                      offset_needed= args['transform']['transf_offset_needed'] )
            pdf['aux_sig'] = pdf['aux_sig'].astype('int16')
        else:
            #print('Nor boolean neither transformed to bool')
            pdf['aux_sig'] = pdf[sig]
    
    if args['trimmer']['trim_needed'].capitalize()=='True':
      edges_counter = pdf['aux_sig'][pdf['trim']==True].diff(periods=1).value_counts()
    else:
      edges_counter = pdf['aux_sig'].diff(periods=1).value_counts()
    
    edges_counter = edges_counter.astype('int16')
    
    
    # Evaluates if number of rising edges (edges_counter[1]) > r_edges_thr OR 
    # number of falling edges (edges_counter[-1]) > l_edges_thr
    if ((1 in edges_counter) & (-1 in edges_counter)):
        alarm = ((rel_ops[args['cond_par']['cond_op'][0]](edges_counter[1],args['cond_par']['cond_thresholds']['r_edge_thr'])) |
             (rel_ops[args['cond_par']['cond_op'][1]](edges_counter[-1],args['cond_par']['cond_thresholds']['f_edge_thr']))
            )
    else:
        alarm = False
        
    #signal_FE(edges_counter.iloc[[-1,1]], False, ["10","10"], [">"],1)
    edges_values = []
    if -1 in edges_counter.index:
      edges_values.append(-1)
    if 1 in edges_counter.index:
      edges_values.append(1)
    res = edges_counter.iloc[edges_values]
    
    if edges_values:
      stats_dict = signal_FE(res,
                           has_alarmed = alarm,
                           #args['alarm_id'],
                           thr_list = [args['cond_par']['cond_thresholds']['r_edge_thr'],args['cond_par']['cond_thresholds']['f_edge_thr']],
                           operator = [args['cond_par']['cond_op'][0],args['cond_par']['cond_op'][1]],
                           stddev_base = 0)
    else:
      stats_dict = {'thresholds': [args['cond_par']['cond_thresholds']['r_edge_thr'], args['cond_par']['cond_thresholds']['f_edge_thr']], 
                     'min': 0,'max': 0, 'mean': 0, 'median': 0, 'std': 0,'mape': 0, 'zscore': 0}
    return alarm,stats_dict
    #return  alarm, edges_counter


# COMMAND ----------

def get_edges_mod(pdf, sig, args):
    ''' Count edges on boolean signal or integer 0,1 signals
    
    -------------------------------------------------------------------------------------------------------------
    eg: Generating a pulse signal and measuring the duration in bool state true
    import pandas as pd
    df = pd.DataFrame([1,1,0,0,0,0,0,1,1,1,1,0,0,0,0,0,0,0,1,1,1,1,1,1,0,1,1,0,1,0,0],columns=['signal']).reset_index()
    arguments = { 
                  "cond_par": {"cond_thresholds": {"thr1": 1.5,"thr2": 1.5},"cond_op": [">",">"],"cond_seq": ["max","avg"]},
                  "transform": { "to_boolean": "false", "transf_offset_needed": "false" },
                  "trimmer": { "trim_needed": "false", "trim_offset_needed": "false"},
                  "additional_signal": {"time_index": "index" },
                  "return": {"bool_state": "true","value": "range","results": ["max", "avg" ]}
                 }
    al, res = get_edges_mod(pdf = df,sig='signal',args = arguments)
    
    '''
    if args['trimmer']['trim_needed'].capitalize()=='True':
      pdf['trim'] = generate_boolean_series(df = pdf, 
                                            cols = args['trimmer']['trim_sig'], 
                                            operators= args['trimmer']['trim_op'],
                                            thresholds = args['trimmer']['trim_thr'],
                                            logic_operation=args['trimmer']['trim_logic_op'],
                                            offset_needed= args['trimmer']['trim_offset_needed'] )

    index = args['additional_signal']['time_index']
    
    # Verifica se só tem 0 e 1 então 
    if (0 in pdf[sig].value_counts().index) | (1 in pdf[sig].value_counts().index) :
        pdf['aux_sig'] = pdf[sig].astype('bool')
    
    if (args['transform']['to_boolean'].capitalize()=='True'):
          print('Transforming to boolean')
          pdf['aux_sig'] = generate_boolean_series(df = pdf, 
                                    cols = args['transform']['transf_sig'], 
                                    operators= args['transform']['transf_op'],
                                    thresholds = args['transform']['transf_thr'],
                                    logic_operation=args['transform']['transf_logic_op'],
                                    offset_needed= args['transform']['transf_offset_needed'] )
            
    if args['trimmer']['trim_needed'].capitalize()=='True':
      df_result = pdf['aux_sig'][pdf['trim']==True]
    else:
      df_result = pdf['aux_sig']
      
    df_change = df_result.shift(1) ^ df_result
    df_change = df_change[df_change == True]
    
    status = df_result.iloc[0]
    change_range = [{'index': pdf.iloc[0:].index[0], 'type': status}]
    if len(df_change) >= 1:
        for idx in range(len(df_change)):
            status = ~status
            change_range.append({
                'index':
                df_change[df_change == True].index[idx],
                'type':
                status,
            })

    change_range.append({'index': pdf.iloc[-1:].index[0], 'type': status})
    edges = pd.DataFrame(change_range)
    idx = [n for n in edges['index']]
    edges[index] = (pdf.iloc[idx][index]).values
    edges['range'] = edges[index].diff().shift(-1)
    
    edges_dict = {}
    try:
        edges2 = edges[args['return']['value']][edges['type']==str2bool(args['return']['bool_state'])]
        agg_list = [aggreg_ops[i.lower()] for i in args['return']['results']]
        #print(agg_list)
        #edges2 = edges2.map(lambda x: x.total_seconds)
        edges_dict = edges2.agg(agg_list).fillna(value=0).to_dict()
        #print(sig, edges2, agg_list, edges_dict, '\n')
    except:
        print('Internal Error')
    
    alarm = (
     (rel_ops[args['cond_par']['cond_op'][0]](edges_dict[aggreg_ops[args['cond_par']['cond_seq'][0]]],
                                              args['cond_par']['cond_thresholds']['thr1'])) |
     (rel_ops[args['cond_par']['cond_op'][1]](edges_dict[aggreg_ops[args['cond_par']['cond_seq'][1]]],
                                              args['cond_par']['cond_thresholds']['thr2']))
 )
    res = edges2#.iloc[[-1,1]]
    #print(edges2)
    
    if edges2.any():
       stats_dict = signal_FE(res,
                           has_alarmed = alarm,
                           #args['alarm_id'],
                           thr_list = [args['cond_par']['cond_thresholds']['thr1'],args['cond_par']['cond_thresholds']['thr2']],
                           operator = [args['cond_par']['cond_op'][0],args['cond_par']['cond_op'][1]],
                           stddev_base = 0)
    else:
      stats_dict = {'thresholds': [args['cond_par']['cond_thresholds']['thr1'], args['cond_par']['cond_thresholds']['thr2']], 
                     'min': 0,'max': 0, 'mean': 0, 'median': 0, 'std': 0,'mape': 0, 'zscore': 0}
            
    return alarm,stats_dict

# COMMAND ----------

def diff_btw_sides(pdf, sig, args):
    ''' 
    Count edges on boolean signal or integer 0,1 signals
    
    -------------------------------------------------------------------------------------------------------------
    eg: 
    import pandas as pd
    import numpy as np
    df = pd.DataFrame(np.random.rand(30,2)*np.random.randint(1,10), columns=['signal0','signal1'])
    arguments = { 
                  "cond_par": {"cond_thresholds": {"threshold": 5 },  "cond_op": [">"] },
                  "transform": { "to_boolean": "false", "transf_offset_needed": "false" },
                  "trimmer": { "trim_needed": "false", "trim_offset_needed": "false"},
                  "scale": {"scale_needed":"false"},
                  "abs" : "false",
                  "how": "any"
                 }
    diff_btw_sides(pdf=df, sig=['signal0','signal1'], args=arguments)
    
    '''
    sides_diff = lambda x, y : (x-y)
    
    pdf2 = pdf.copy()
    
    if args['scale']['scale_needed'].capitalize()=='True':
        for i,signal in enumerate(args['scale']['scale_sig']):
            pdf2[signal] = rel_ops[args['scale']['scale_op'][i]](pdf2[signal], args['scale']['scale_factor'][i])
            #print(pdf2[signal])
    
    if args['trimmer']['trim_needed'].capitalize()=='True':
        pdf2['trim'] = generate_boolean_series(df = pdf2, 
                                      cols = args['trimmer']['trim_sig'], 
                                      operators= args['trimmer']['trim_op'],
                                      thresholds = args['trimmer']['trim_thr'],
                                      logic_operation=args['trimmer']['trim_logic_op'],
                                      offset_needed= args['trimmer']['trim_offset_needed'] )
        pdf2 = pdf2[pdf2['trim']==True]
        
    if args['abs'].capitalize() == 'True':
        pdf2['res'] = np.absolute(sides_diff(pdf2[sig[0]], pdf2[sig[1]]))
        #res = np.absolute(sides_diff(pdf2[sig[0]], pdf2[sig[1]]))
    else:
        pdf2['res'] = sides_diff(pdf2[sig[0]], pdf2[sig[1]])
        #res = sides_diff(pdf2[sig[0]], pdf2[sig[1]])

        
    alarm = rel_ops[args['cond_par']['cond_op'][0]](pdf2['res'],args['cond_par']['cond_thresholds']['threshold'])
    
    if args['how'] == 'any':
        alarm = alarm.any()
    else:
        alarm = alarm.all()
        
    res = pdf2['res']   
    stats_dict = signal_FE(res,
                           has_alarmed = alarm,
                           thr_list = [args['cond_par']['cond_thresholds']['threshold']],
                           operator = [args['cond_par']['cond_op'][0]],
                           stddev_base = 0)
        
    return alarm, stats_dict

# COMMAND ----------

def evaluate_parts(pdf, sig, args):
    """ Slice the dataframe in pieces """
    nbr_of_slices = args['number_of_parts']

    trim_sig = generate_boolean_series(df = pdf, 
                                      cols = args['trimmer']['trim_sig'], 
                                      operators= args['trimmer']['trim_op'],
                                      thresholds = args['trimmer']['trim_thr'],
                                      logic_operation=args['trimmer']['trim_logic_op'],
                                      offset_needed= args['trimmer']['trim_offset_needed'] )
    index_XFirst = trim_sig[trim_sig == True].first_valid_index()
    index_XLast = trim_sig[trim_sig == True].last_valid_index()
    interval = (index_XLast - index_XFirst) // nbr_of_slices
    # print('interval',interval)

    #--- Build the dictinary which the keys are on standard part0_
    x = {}

    for i in range(1, nbr_of_slices + 1):
        str_part = 'part0' + str(i)
        x[str_part] = pdf[index_XFirst + (i - 1) * interval : index_XFirst +i * interval][sig]
        #print( x[str_part]); print("=>",len(x[str_part]))

    area_interest = pd.Series([], dtype='float32')
    #area_interest = pd.Series()
    #--- Evaluates the condition in the parts
    for c in args['parts']:
        # Iterates over every specified part in the args['parts']
        for idx in range(0, len(c)):
            # Evaluates a combination of parts
            area_interest = area_interest.append(x[c[idx]])  #print(idx)
            if ((args['multiple_eval'].capitalize() == 'True') & (idx >= len(c) - 1)):
                #print('len area of int',len(area_interest))
                if args['modifier'] == "mean":
                    #print(area_interest.mean())
                    alarm = rel_ops[args['op'][0]](area_interest.mean(),args['upper_threshold']) | \
                            rel_ops[args['op'][1]](area_interest.mean(), args['lower_threshold'])
                        
                        
                else:
                    alarm = rel_ops[args['op'][0]](area_interest,args['upper_threshold']) |\
                            rel_ops[args['op'][1]](area_interest, args['lower_threshold'])
            elif(args['multiple_eval'] == False):
              # Check every single part 
              if args['modifier'] == "mean":
                  area_interest = x[c[idx]]
                  alarm = rel_ops[args['op'][0]](area_interest.mean(), args['upper_threshold']) | rel_ops[args['op'][1]](x[c[idx]].mean(), args['lower_threshold'])
              else:
                  alarm = rel_ops[args['op'][0]](area_interest, args['upper_threshold']) | rel_ops[args['op'][1]](x[c[idx]], args['lower_threshold'])
                  #print(idx)

    #---
    if args['how'] == 'any':
        alarm = alarm
    else:
        alarm = alarm
  
    res = area_interest
    stats_dict = signal_FE(res,
                           has_alarmed = alarm,
                           #args['alarm_id'],
                           thr_list = [args['lower_threshold'],args['upper_threshold']],
                           operator = [args['op'][1],args['op'][0]],
                           stddev_base = 0
                          )
    return alarm,stats_dict 

# COMMAND ----------

def count_edges(pdf, sig, args):
    pdf_shifted = pdf[sig].shift(1)
    pdf_shifted[0] = pdf[sig][0]
    pdf_edges = (pdf[sig] != pdf_shifted)
    cnt = pdf_edges.value_counts(sort=True)
    alarm = True in cnt
    if args['how'] == 'any':
        alarm = alarm
    else:
        alarm = alarm
    return alarm

# COMMAND ----------

# Z_score function
def z_score(pdf, amplitude):
    """Calculate the Z-score """
    mean_amp = pdf[amplitude].mean()
    std_amp = pdf[amplitude].std()
    if std_amp != 0:
        z_scores = (pdf[amplitude] - mean_amp) / std_amp
    else:
        print("ERROR: standard deviation is null !")
    return z_scores

# COMMAND ----------

def peak_detection(pdf, sig, args):
    # If any spikes is detected then Alarm
    z_res = z_score(pdf, sig)
    z_res = (z_res > args['high_threshold']) | (z_res < args['low_threshold'])
    if args['how'] == 'any':
        alarm = z_res.any()
    else:
        alarm = z_res.all()
    return alarm

# COMMAND ----------

def operation_between_signals(pdf, sig, args):
    res = rel_ops[args['signal_op']](pdf[sig[0]], pdf[sig[1]])

    if args['abs'] == True:
        res = res.abs() * args['scale_factor']
    else:
        res = res * args['scale_factor']

    alarm = rel_ops[args['threshold_op']](res, args['threshold'])
    if args['how'] == 'any':
        alarm = alarm.any()
    else:
        alarm = alarm.all()
    return alarm

# COMMAND ----------

def two_signals_threshold_compare(pdf, sig, args):
    pdf = pdf[pdf.index < 3000]  #look only the first 30 seconds
    edges0 = get_edges(df=pdf,
                       target=sig[0],
                       threshold=args['value_threshold'],
                       op=args['time_op'],
                       index=args['add_signal'][0])
    edges1 = get_edges(df=pdf,
                       target=sig[1],
                       threshold=args['value_threshold'],
                       op=args['time_op'],
                       index=args['add_signal'][0])

    if (args['area'] == "above_threshold"):
        edges_type = True
    else:
        edges_type = False

    #Alarm if only one of the signals has stayed in the condition
    if args['how'] == 'any':
        alarm = (
            (edges0['type'] == edges_type) &
            (rel_ops[args['time_op']](edges0['range'], args['time_threshold']))
            ^ (edges1['type'] == edges_type) &
            (rel_ops[args['time_op']](edges1['range'], args['time_threshold']))
        ).any()
    else:
        alarm = alarm.all()
    return alarm

# COMMAND ----------

def signal_threshold_compare(pdf, sig, args):
    edges = get_edges(
        df=pdf,
        target=sig,
        threshold=args['value_threshold'],
        op=args['time_op'],
        index=args['add_signal'][0],
    )

    if (args['area'] == "above_threshold"):
        edges_type = True
    else:
        edges_type = False
    #return ((edges['type']==False) & (edges['range'],args['time_threshold'])).any()
    return ((edges['type'] == edges_type) & (rel_ops[args['time_op']](
        edges['range'], args['time_threshold']))).any()

# COMMAND ----------

def slice_by_time(pdf, sig, args):
    """ Slice the dataframe according to times of a boolean signal """
    edges = get_edges(
        df=pdf,
        target=args['add_signal'][0],
        threshold=args['threshold'],
        op='>=',
        index=args['add_signal'][1],
    )
    #NOTE: that edges[0] -> start of dataframe and edges[-1] -> end of dataframe
    #Thus, first edge = index[1], last edge = index[-2]
    t0 = edges[args['add_signal'][1]][edges.index[1]]
    t1 = edges[args['add_signal'][1]][edges.index[-2]]

    x = {}
    x['tail'] = pdf[(pdf[args['add_signal'][1]] >= t1)][sig]
    x['head'] = pdf[(pdf[args['add_signal'][1]] <= t0)][sig]
    x['body'] = pdf[((pdf[args['add_signal'][1]] > t0) &
                     (pdf[args['add_signal'][1]] < t1))][sig]
    x['bite'] = pdf[(pdf[args['add_signal'][1]] <= (t0 + args['dt01']))
                    & (pdf[args['add_signal'][1]] >= (t0 + args['dt02']))][sig]

    for c in args['region']:
        if (c[2] == 'mean'):
            avg = np.absolute(x[c[0]].mean())
            alarm = rel_ops[c[3]](avg, c[1])
#      print('--mean-- =',avg)
        elif (c[2] == 'max'):
            if args['abs'] == True:
                x[c[0]] = x[c[0]].abs()
            alarm = rel_ops[c[3]](x[c[0]].max(), c[1])
#      print('--max--')
        elif (c[2] == 'min'):
            if args['abs'] == True:
                x[c[0]] = x[c[0]].abs()
            alarm = rel_ops[c[3]](x[c[0]].min(), c[1])


#      print('--min--')
        else:
            if args['abs'] == True:
                x[c[0]] = x[c[0]].abs()
            alarm = rel_ops[c[3]](x[c[0]], c[1])
        if args['how'] == 'any':
            alarm = alarm.any()
        else:
            alarm = alarm.all()
    return alarm

# COMMAND ----------


sig_dict = {
'TIME'   					: 'Time', 
'WR_BOT_DIAM_ST2'   		: 'Actual Work Roll Bot Diameter Stand 2',
'WR_TOP_TEMP_DS_ST1'       	: 'Actual Top Work Roll Chock DS Stand 1 Temperature',
'WR_TOP_TEMP_OS_ST1'       	: 'Actual Top Work Roll Chock OS Stand 1 Temperature',
'WR_BOT_TEMP_DS_ST1'       	: 'Actual Bottom Work Roll Chock DS Stand 1 Temperature',
'WR_BOT_TEMP_OS_ST1'       	: 'Actual Bottom Work Roll Chock OS Stand 1 Temperature',
'WR_TOP_TEMP_DS_ST2'       	: 'Actual Top Work Roll Chock DS Stand 2 Temperature',
'WR_TOP_TEMP_OS_ST2'       	: 'Actual Top Work Roll Chock OS Stand 2 Temperature',
'WR_BOT_TEMP_DS_ST2'       	: 'Actual Bottom Work Roll Chock DS Stand 2 Temperature',
'WR_BOT_TEMP_OS_ST2'       	: 'Actual Bottom Work Roll Chock OS Stand 2 Temperature',
'BUR_TOP_TEMP_DS_ST1'       : 'Actual Top BackUp Roll Chock DS Stand 1 Temperature',
'BUR_TOP_TEMP_OS_ST1'       : 'Actual Top BackUp Roll Chock OS Stand 1 Temperature',
'BUR_BOT_TEMP_DS_ST1'       : 'Actual Bottom BackUp Roll Chock DS Stand 1 Temperature',
'BUR_BOT_TEMP_OS_ST1'       : 'Actual Bottom BackUp Roll Chock OS Stand 1 Temperature',
'BUR_TOP_TEMP_DS_ST2'       : 'Actual Top BackUp Roll Chock DS Stand 2 Temperature',
'BUR_TOP_TEMP_OS_ST2'       : 'Actual Top BackUp Roll Chock OS Stand 2 Temperature',
'BUR_BOT_TEMP_DS_ST2'       : 'Actual Bottom BackUp Roll Chock DS Stand 2 Temperature',
'BUR_BOT_TEMP_OS_ST2'       : 'Actual Bottom BackUp Roll Chock OS Stand 2 Temperature',
'STRIP_WIDTH'       		: 'Actual Strip width from CPC',
'3SIG_FLAT_ERROR_DS_ST2'    : '3Sigma Flatness Error Edge DS Stand 2 _edge zone_',
'3SIG_FLAT_ERROR_OS_ST2'    : '3Sigma Flatness Error Edge OS Stand 2 _edge zone_',
'3SIG_FLAT_ERROR_DS_ST1'    : '3Sigma Flatness Error Edge DS Stand 1 _edge zone_',
'3SIG_FLAT_ERROR_OS_ST1'    : '3Sigma Flatness Error Edge OS Stand 1 _edge zone_',
'IR_FREQ'       			: 'Ironning Roll Frequency', 
'HDR_SLIP_DETECTED'	   		: 'InterStand Hold Down Roll Slip Detected',
'HDR_SLIP_DS'       		: 'InterStand Hold Down Roll Slip DS',
'HDR_SLIP_OS'       		: 'InterStand Hold Down Roll Slip OS', 
'EXIT_ST1_SPEED'	   	: 'SLAC Speed S1 exit',
'EXIT_ST2_SPEED'       		: 'SLAC Speed S2 exit', 
'WEDGE_FACTOR_ST1'	   		: 'Wedge factor S1', 
'WEDGE_FACTOR_ST2'	   		: 'Wedge factor S2', 
'COIL_ID'	   				: 'CoilID',
'HDR_SPEED_OS'       		: 'InterStand Hold Down Roll Speed Operator Side',
'HDR_SPEED_DS'       		: 'InterStand Hold Down Roll Speed Drive Side', 
'IR_FULL_STROKE_DS'	   		: 'IR Full Stroke DS',
'IR_FULL_STROKE_OS'       	: 'IR Full Stroke OS', 
'IR_FULL_RET_DS'	   		: 'IR Full Retracted DS', 
'IR_FULL_RET_OS'	   		: 'IR Full Retracted OS',
'INTERST_COOL_DEV_A'       	: 'Interstand strip cooling device Width Adjustment A Actual Encoder measure',
'INTERST_COOL_DEV_B'        : 'Interstand strip cooling device Width Adjustment B Actual Encoder measure',
'ST2_EXIT_TENSION_DS'       	: 'T2 act exit tension DS', 
'ST2_EXIT_TENSION_OS'	   	: 'T2 act exit tension OS',
'3RD_OCTAVE_DS_ST2'       	: 'Stand 2 3rd Octave Vibration DS', 
'3RD_OCTAVE_OS_ST2'	   		: 'Stand 2 3rd Octave Vibration OS',
'WR_TOP_WEAR_ST2'       	: 'Stand 2 Top Work Roll Wear',
'XFT_EXIT_TENSION_DS_ST2'   : 'T2 XFT_X2_DS    : Tension exit stand 2 DS',
'XFT_EXIT_TENSION_OS_ST2'   : 'T2 XFT_X2_NDS    : Tension exit stand 2 NDS',
'IR_VIB_AV_OS'       		: 'IBA_VIB Ironing Roll OS AV _29_', 
'IR_VIB_VV_DS'	   			: 'IBA_VIB Ironing Roll DS VV _30_',
'IR_VIB_VV_OS'       		: 'IBA_VIB Ironing Roll OS VV _31_', 
'IR_VIB_AV_DS'	   			: 'IBA_VIB Ironing Roll DS AV _32_',
'IR_VIB_AV_OS_MIN'       	: 'InSpectra Ironing Roll _Vibration OS AV__Minimum',
'IR_VIB_AV_OS_MAX'       	: 'InSpectra Ironing Roll _Vibration OS AV__Maximum',
'IR_VIB_AV_OS_AVG'       	: 'InSpectra Ironing Roll _Vibration OS AV__Average',
'IR_VIB_AV_OS_RMS'       	: 'InSpectra Ironing Roll _Vibration OS AV__RMS',
'EXIT_LENGTH'               : 'Exit Length',
'STRIP_LENGHT'              : 'strip_length_act',
'STEADY_PHASE':'STEADY_PHASE',
'ACC_PHASE':'ACC_PHASE',
'DEC_PHASE':'DEC_PHASE',
'OUT_OF_WEDGE':'OUT_OF_WEDGE',
'WEDGE': 'WEDGE'
}
# %%


# COMMAND ----------

import numpy as np

def signal_FE(pds, has_alarmed, thr_list, operator,stddev_base):
  stats_dict = {}
  agg_series = pds.agg(['min','max','mean','median','std'])
  agg_series = agg_series.round(5)
  
  #stats_dict['alarm_id']  = alarm_id
  stats_dict['thresholds'] = thr_list#.sort()
  #stats_dict['signal']  = agg_series.name  
  
  agg_dict = agg_series.to_dict()
  stats_dict.update(agg_dict)
  
  # Order the thresholds ascending
  thr_list.sort()
  operator.sort()

  if (has_alarmed):
    if len(thr_list)>1:
      mape_low = mape(pds[pds<thr_list[0]], thr_list[0])
      mape_high = mape(pds[pds>thr_list[-1]], thr_list[-1])
      dev = mape_low + mape_high
    else:
      cond = rel_ops[operator[0]](pds, thr_list[0]) # pdf > thr_list[0]
      dev = mape(pds[cond], thr_list[0])
  else:
    dev = 0
    
    
  if (has_alarmed) & (stddev_base !=0) & (stddev_base is not None):
    zscore = pd.DataFrame()
    if len(thr_list)>1:
      zscore['low'] = -(pds-thr_list[0])/stddev_base
      zscore['high'] = (pds-thr_list[-1])/stddev_base
      deviation = zscore[["low", "high"]].max(axis=1).describe()['75%']
    else:
      zscore = (pds-thr_list[-1])/stddev_base
      deviation = zscore.describe()['75%']
  else:
    deviation = 0
    
  stats_dict['mape'] = dev
  stats_dict['zscore'] = np.abs(deviation)
  return stats_dict


def mape(val,thr):
  value, threshold = np.array(val), np.ones_like(np.array(val))*thr
  
  zeros = list(np.where(value == 0)[0])
  value, threshold = np.delete(value,zeros), np.delete(threshold,zeros)
  
  try:
    if (len(value)>0) :
      mape = np.mean(np.abs((value - threshold) / value)) * 100
    else:
      mape = 0
      #print('Length of array is zero')
  except ValueError as e:
      print('Error:',e)
  return np.round(mape,2)


def zscore_to_deviation(zscore):
  if ((zscore >0.0)and(zscore <0.5)):
    deviation=2
  elif ((zscore >=0.5)and(zscore <1.0)):
    deviation = 4
  elif ((zscore >=1.0)and(zscore <2.0)):
    deviation = 6
  elif ((zscore >=2.0)and(zscore <3.0)):
    deviation = 12
  elif zscore >=3.0:
    deviation = 16
  else:
    deviation = 0
  
  return int(deviation)

# COMMAND ----------



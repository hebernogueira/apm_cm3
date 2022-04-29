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
    return alarm, 'None'    

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

def cnt_edges_bool_sig(pdf,sig,args= { 'trim_needed':'false','transform':{'to_boolean':'false'}}):
    ''' Count edges on boolean signal or integer 0,1 signals'''
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
    
    edges_counter = pdf['aux_sig'][pdf['trim']==True].diff(periods=1).value_counts()
    #edges_counter = edges_counter.index.astype('int16')
    edges_counter = edges_counter.astype('int16')
    
    
    # Evaluates if number of rising edges (edges_counter[1]) > r_edges_thr OR 
    # number of falling edges (edges_counter[-1]) > l_edges_thr
    if ((1 in edges_counter) & (-1 in edges_counter)):
        alarm = ((rel_ops[args['cond_par']['cond_op'][0]](edges_counter[1],args['cond_par']['cond_thresholds']['r_edge_thr'])) |
             (rel_ops[args['cond_par']['cond_op'][1]](edges_counter[-1],args['cond_par']['cond_thresholds']['f_edge_thr']))
            )
    else:
        alarm = False
        
    return  alarm, dict(edges_counter)


# COMMAND ----------

def get_edges_mod(pdf, sig, args):
    #print('1')
    pdf['trim'] = generate_boolean_series(df = pdf, 
                                          cols = args['trimmer']['trim_sig'], 
                                          operators= args['trimmer']['trim_op'],
                                          thresholds = args['trimmer']['trim_thr'],
                                          logic_operation=args['trimmer']['trim_logic_op'],
                                          offset_needed= args['trimmer']['trim_offset_needed'] )
    
    index = args['additional_signal']['time_index']
    
    # Verifica se só tem 0 e 1 então 
    if(list(pdf[sig].value_counts().index) == [0,1]):
        pdf['aux_sig'] = pdf[sig].astype('bool')
    
    if (args['transform']['to_boolean'].capitalize()=='True'):
            print('Transforming to boolean')
            pdf['aux_sig'] = generate_boolean_series(df = pdf, 
                                      cols = args['transform']['transf_sig'], 
                                      operators= args['transform']['transf_op'],
                                      thresholds = args['transform']['transf_thr'],
                                      logic_operation=args['transform']['transf_logic_op'],
                                      offset_needed= args['transform']['transf_offset_needed'] )
            
            
    df_result = pdf['aux_sig'][pdf['trim']==True]
    
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
#    alarm = 0 
            
    return alarm,edges_dict

# COMMAND ----------



# COMMAND ----------

def diff_btw_sides(pdf, sig, args):
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
        
    return alarm,'None'

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
            elif (args['multiple_eval'].capitalize() == 'False'):
                # Check every single part
                if args['modifier'] == "mean":
                    #print(x[c[idx]].mean())
                    alarm = rel_ops[args['op'][0]]( x[c[idx]].mean(), args['upper_threshold']) | \
                            rel_ops[args['op'][1]]( x[c[idx]].mean(), args['lower_threshold'])
                else:
                    alarm = rel_ops[args['op'][0]](x[c[idx]], args['upper_threshold']) | \
                            rel_ops[args['op'][1]](x[c[idx]], args['lower_threshold'])
                    #print(idx)

    #---
    if args['how'] == 'any':
        alarm = alarm
    else:
        alarm = alarm

    return alarm,'None'

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

def slice_in_six(pdf, sig, args):
    """ Slice the dataframe in pieces """
    nbr_of_slices = args['nbr_of_parts']

    # Cleans start of signal when
    pdf.loc[0:3000, args['add_signal'][0]] = 0
    pdf.loc[35000:, args['add_signal'][0]] = 0

    edges_F1_Load = get_edges(df=pdf,
                              target=args['add_signal'][0],
                              threshold=args['trim_value'][0],
                              op='>=',
                              index=args['add_signal'][2])
    #print('edges of F1 load: \n',edges_F1_Load)
    edges_Run_Mode = get_edges(df=pdf,
                               target=args['add_signal'][1],
                               threshold=args['trim_value'][1],
                               op='>=',
                               index=args['add_signal'][2])
    #print('edges_Run_Mode \n',edges_Run_Mode)

    #--- INDEX Handling
    index_XFirst = edges_Run_Mode['index'][edges_Run_Mode.index[1]]
    # print('First:',index_XFirst)            #---index of first edge
    index_XLast = edges_F1_Load['index'][edges_F1_Load.index[-2]] - 50
    # print('Last:',index_XLast)              #---index of last edge => -500 means 5 seconds before the last edge
    interval = (index_XLast - index_XFirst) // nbr_of_slices
    # print('interval',interval)

    #--- Build the dictinary which the keys are on standard part0_
    x = {}

    for i in range(1, nbr_of_slices + 1):
        str_part = 'part0' + str(i)
        x[str_part] = pdf[index_XFirst + (i - 1) * interval:index_XFirst +
                          i * interval][sig]
        #print( x[str_part]); print("=>",len(x[str_part]))

    #--- Evaluates the difference between parts or the avg difference
    for c in args['parts']:
        # reset index to make subtraction valid and drop the old index column
        x_diff = x[c[0]].reset_index(drop=True) - x[c[1]].reset_index(
            drop=True)
        if c[2] == "mean":
            if args['abs'] == True:
                alarm = rel_ops[c[3]](np.absolute(x_diff.mean()), c[4])
                #print(sig,np.absolute(x_diff.mean()),' ', c[3],' ',c[4])
            else:
                alarm = rel_ops[c[3]](x_diff.mean(), c[4])
        else:
            if args['abs'] == True:
                alarm = rel_ops[c[3]](np.absolute(x_diff), c[4])
            else:
                alarm = rel_ops[c[3]](x_diff, c[4])

    #---
    if args['how'] == 'any':
        alarm = alarm.any()
    else:
        alarm = alarm.all()

    return alarm

# COMMAND ----------

def OLD_evaluate_parts(pdf, sig, args):
    """ Slice the dataframe in pieces """
    nbr_of_slices = 6

    #edges_F1_Load = get_edges(df=pdf,target=args['add_signal'][0],threshold=args['trim_value'][0],op='>=',index=args['add_signal'][2]);
    #edges_Run_Mode = get_edges(df=pdf,target=args['add_signal'][1],threshold=args['trim_value'][1],op='>=',index=args['add_signal'][2]);
    #index_XFirst = edges_Run_Mode['index'][edges_Run_Mode['type']==True][edges_Run_Mode.index[1]];    #---index of first raising edge
    #index_XLast = edges_F1_Load['index'][edges_F1_Load['type']==False][edges_F1_Load.index[-2]] -500; #---index of last falling edge => -500 means 5 seconds before the last edge

    trim_sig = (pdf[args['add_signal'][0]][pdf[args['add_signal'][0]] == True]
                & pdf[args['add_signal'][0]][pdf[args['add_signal'][1]] ==
                                             args['trim_value'][1]])
    index_XFirst = trim_sig[trim_sig == True].first_valid_index()
    index_XLast = trim_sig[trim_sig == True].last_valid_index() - (500)
    interval = (index_XLast - index_XFirst) // nbr_of_slices
    # print('interval',interval)

    #--- Build the dictinary which the keys are on standard part0_
    x = {}

    for i in range(1, nbr_of_slices + 1):
        str_part = 'part0' + str(i)
        x[str_part] = pdf[index_XFirst + (i - 1) * interval : index_XFirst +i * interval][sig]
        #print( x[str_part]); print("=>",len(x[str_part]))

    area_interest = pd.Series([])
    #--- Evaluates the condition in the parts
    for c in args['parts']:
        # Iterates over every specified part in the args['parts']
        for idx in range(0, len(c)):
            # Evaluates a combination of parts
            area_interest = area_interest.append(x[c[idx]])  #print(idx)
            if ((args['multiple_eval'] == True) & (idx >= len(c) - 1)):
                #print('len area of int',len(area_interest))
                if args['modifier'] == "mean":
                    #print(area_interest.mean())
                    alarm = rel_ops[args['op'][0]](
                        area_interest.mean(),
                        args['upper_threshold']) | rel_ops[args['op'][1]](
                            area_interest.mean(), args['lower_threshold'])
                else:
                    alarm = rel_ops[args['op'][0]](
                        area_interest,
                        args['upper_threshold']) | rel_ops[args['op'][1]](
                            area_interest, args['lower_threshold'])
            elif (args['multiple_eval'] == False):
                # Check every single part
                if args['modifier'] == "mean":
                    alarm = rel_ops[args['op'][0]](
                        x[c[idx]].mean(),
                        args['upper_threshold']) | rel_ops[args['op'][1]](
                            x[c[idx]].mean(), args['lower_threshold'])
                else:
                    alarm = rel_ops[args['op'][0]](
                        x[c[idx]],
                        args['upper_threshold']) | rel_ops[args['op'][1]](
                            x[c[idx]], args['lower_threshold'])
                    #print(idx)

    #---
    if args['how'] == 'any':
        alarm = alarm.any()
    else:
        alarm = alarm.all()

    return alarm

# COMMAND ----------

def diff_sides(pdf, sig, args):
    sides_diff = lambda x, y, w, z: ((x + y) - (w + z)) / 2

    if args['abs'] == True:
        res = np.absolute(
            sides_diff(pdf[sig[0]], pdf[sig[1]], pdf[sig[2]], pdf[sig[3]]))
    else:
        res = sides_diff(pdf[sig[0]], pdf[sig[1]], pdf[sig[2]], pdf[sig[3]])

    alarm = rel_ops[args['threshold_op']](res, args['threshold'])
    if args['how'] == 'any':
        alarm = alarm.any()
    else:
        alarm = alarm.all()
    return alarm

# COMMAND ----------

def diff_in_areas(pdf, sig, args):

    #  print(len(sig))

    pdf.loc[0:1500, args['add_signal'][0]] = 0
    pdf.loc[35000:, args['add_signal'][0]] = 0
    #pdf[args['add_signal'][0]]
    #print(pdf[args['add_signal'][0]])

    edges = get_edges(
        #df=pdf[(pdf[args['add_signal'][1]]>50.0) & (pdf[args['add_signal'][1]]<350.0)] ,
        df=pdf,
        target=args['add_signal'][0],
        threshold=args['threshold'],
        op='>=',
        index=args['add_signal'][1],
    )
    #NOTE: that edges[0] -> start of dataframe and edges[-1] -> end of dataframe
    #Thus, first edge = index[1], last edge = index[-2]
    # print(edges)
    #print(edges[args['add_signal'][1]][edges['type']==True])

    t0 = edges[args['add_signal'][1]][edges.index[1]]
    t1 = edges[args['add_signal'][1]][edges.index[-2]]
    #print('t0', t0)
    #print('t1', t1)

    x = dict()
    x.clear()
    x = {}
    for idx in range(0, 2):
        tail = 'tail' + str(idx)
        head = 'head' + str(idx)
        body = 'body' + str(idx)
        bite = 'bite' + str(idx)
        x[tail] = pdf[(pdf[args['add_signal'][1]] >= t1)][sig[idx]]
        x[head] = pdf[(pdf[args['add_signal'][1]] <= t0)][sig[idx]]
        x[body] = pdf[((pdf[args['add_signal'][1]] > t0) &
                       (pdf[args['add_signal'][1]] < t1))][sig[idx]]
        x[bite] = pdf[(pdf[args['add_signal'][1]] <= (t0 + args['dt01'])) & (
            pdf[args['add_signal'][1]] >= (t0 + args['dt02']))][sig[idx]]

    for c in args['region']:

        x_diff = x[c[0] + '0'] - x[c[0] + '1']
        x_diff = x_diff.abs()
        #print('max:',x_diff.max())
        alarm = rel_ops[c[2]](x_diff, c[1]).any()

        if args['how'] == 'any':
            alarm = alarm.any()
        else:
            alarm = alarm.all()
    return alarm

# COMMAND ----------

def peak_detection_mod(pdf, sig, args):
    """Calculate the Z-score within a restrict period -> trimmer = True """
    mean_amp = pdf[sig].rolling(window=200).mean()
    #mean_amp = pdf[sig].mean()
    std_amp = pdf[sig].std()
    if std_amp != 0:
        z_scores_mod = (pdf[sig] - mean_amp) / std_amp
        #print("Z_score Max: {} , Z_score Min: {} ".format(z_scores_mod.max(),z_scores_mod.min()))
    else:
        print("ERROR: standard deviation is null !")
    z_scores_mod = (z_scores_mod > args['high_threshold']) | (
        z_scores_mod < args['low_threshold'])
    if args['how'] == 'any':
        alarm = z_scores_mod.any()
    else:
        alarm = z_scores_mod.all()
    return alarm

# COMMAND ----------

def alarm_disabled(pdf, sig, args):
    return None

# COMMAND ----------

def signal_peak_to_peak(pdf, sig, args):
    res = pdf[sig].max() - pdf[sig].min()
    #  print("Max: {} ,  Min: {} ,res: {}".format(pdf[sig].max(),pdf[sig].min(),res))
    alarm = rel_ops[args['threshold_op']](res, args['threshold'])
    if args['how'] == 'any':
        alarm = alarm.any()
    else:
        alarm = alarm.all()
    return alarm

# COMMAND ----------

def diff_shift_signal(pdf, sig, args):
    #Find edges on signal
    ##    trim_sig= pdf[args['add_signal'][0]] & pdf[args['add_signal'][1]]
    ##    trim_shifted = trim_sig.shift(1)
    ##    trim_shifted[0] = trim_sig[0]
    ##    pdf_edges = trim_sig^trim_shifted;
    ##
    ##    for a in list(pdf_edges[pdf_edges==True].index):
    ##      if(trim_shifted[a]==False):
    ##        index_XFirst = a+(args['start_offset']*100);
    ##        print('r_edge',r_egde_index)
    ##      else:
    ##        index_XLast = a - (args['end_offset']*100);
    ##        print('faling',f_egde_index)

    # pdf['trim_sig']= pdf[args['add_signal'][0]] & pdf[args['add_signal'][1]]
    trim_sig = pdf[args['add_signal'][0]] & pdf[args['add_signal'][1]]
    index_XFirst = trim_sig[trim_sig == True].first_valid_index() + (
        args['start_offset'] * 100)
    index_XLast = trim_sig[trim_sig == True].last_valid_index() - (
        args['end_offset'] * 100)

    # Analysis: sig - signal_shifted_20ms > threshold ----> ALARM. Shift signal in 20 ms (default)
    n = 2
    n = int(args['time_shift_ms'] / 10)
    if n < 1:
        n = 1
    elif n > 10000:
        n = 10000
    else:
        n = n

    pdf_shifted = pdf[sig].shift(periods=int(n))
    pdf_shifted[0] = pdf[sig][0]
    pdf_shifted[1] = pdf[sig][1]

    # Trim the signals to make the analysis
    pdf_shifted_trim = pdf_shifted[index_XFirst:index_XLast].reset_index(
        drop=True)
    pdf_sig_trim = pdf[sig][index_XFirst:index_XLast].reset_index(drop=True)

    #Calculate the difference
    pdf_diff = (pdf_sig_trim - pdf_shifted_trim)
    res = pdf_diff.abs()
    res1 = (res > args['threshold'] * 0.625)
    #print(res1[res1==True].count())
    alarm = (
        rel_ops[args['threshold_op']](res, args['threshold'])
        & rel_ops[args['threshold_op']](res1[res1 == True].count(), 2)).any()
    if args['how'] == 'any':
        alarm = alarm
    else:
        alarm = alarm
    return alarm

# COMMAND ----------



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


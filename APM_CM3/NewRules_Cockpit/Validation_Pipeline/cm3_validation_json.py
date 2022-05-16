# Databricks notebook source
rule1 = '''
{
    "st1_gearboxes_temperature": {
                "rule_type": "independent",
                "function": "threshold_compare_mod",
                "input_signal": [
                                "dl_st1_gearbox_temperature_bt1_4_bottom_splindrer_os",
                                "dl_st1_gearbox_temperature_bt2_6_top_splindrer_os",
                                "dl_st1_gearbox_temperature_bt3_1_motor_drive_ds",
                                "dl_st1_gearbox_temperature_bt4_3_bottom_splindrer_ds",
                                "dl_st1_gearbox_temperature_bt5_5_top_splindrer_ds",
                                "dl_st1_gearbox_temperature_bt6_2_motor_drive_os"                                
                              ],
                "cond_par": {
                              "cond_thresholds": { "threshold": 115.0 },
                              "cond_op": [">"]
                },
                
                "scale": {
                          "scale_needed": "false"
                        },
                "trimmer": {
                            "trim_needed": "true",
                            "trim_sig": [  "eg_st2_millstatus" ],
                            "trim_op": ["=="],
                            "trim_thr": [2],
                            "trim_logic_op": ["&"],
                            "trim_offset_needed": "true",
                            "trim_offset_start":5, 
                            "trim_offset_end":5
                      },
                "how":"any",
                "process_id":"st1"
            }
}
'''

# COMMAND ----------

rule2 = '''
{
    "st2_gearboxes_temperature": {
                "rule_type": "independent",
                "function": "threshold_compare_mod",
                "input_signal": [
                                "dl_st2_gearbox_temperature_bt1_4_bottom_splindrer_os",
                                "dl_st2_gearbox_temperature_bt2_6_top_splindrer_os",
                                "dl_st2_gearbox_temperature_bt3_1_motor_drive_ds",
                                "dl_st2_gearbox_temperature_bt4_3_bottom_splindrer_ds",
                                "dl_st2_gearbox_temperature_bt5_5_top_splindrer_ds",
                                "dl_st2_gearbox_temperature_bt6_2_motor_drive_os"
                              ],
                "cond_par": {
                              "cond_thresholds": { "threshold": 115.0 },
                              "cond_op": [">"]
                },
                
                "scale": {
                          "scale_needed": "false"
                        },
                "trimmer": {
                            "trim_needed": "true",
                            "trim_sig": [  "eg_st2_millstatus" ],
                            "trim_op": ["=="],
                            "trim_thr": [2],
                            "trim_logic_op": ["&"],
                            "trim_offset_needed": "true",
                            "trim_offset_start":5, 
                            "trim_offset_end":5
                      },
                "how":"any",
                "process_id":"st2"
            }
}
'''

# COMMAND ----------

rule3 = '''
{
    "por_gearboxes_temperature": {
                "rule_type": "independent",
                "function": "threshold_compare_mod",
                "input_signal": [
                                "dl_por_gearbox_temperature_mandrel_os",
                                "dl_por_gearbox_temperature_mandrel_ds",
                                "dl_por_gearbox_temperature_pinion_os",
                                "dl_por_gearbox_temperature_pinion_ds"
                              ],
                "cond_par": {
                              "cond_thresholds": { "threshold": 115.0 },
                              "cond_op": [">"]
                },
                
                "scale": {
                          "scale_needed": "false"
                        },
                "trimmer": {
                            "trim_needed": "true",
                            "trim_sig": [  "eg_st2_millstatus" ],
                            "trim_op": ["=="],
                            "trim_thr": [2],
                            "trim_logic_op": ["&"],
                            "trim_offset_needed": "true",
                            "trim_offset_start":5, 
                            "trim_offset_end":5
                      },
                "how":"any",
                "process_id":"PayOffReel"
            }
}
'''

# COMMAND ----------

rule4 = '''
{
    "tr_gearboxes_temperature": {
                "rule_type": "independent",
                "function": "threshold_compare_mod",
                "input_signal": [
                                "dl_tr_gearbox_temperature_mandrel_os",
                                "dl_tr_gearbox_temperature_mandrel_ds",
                                "dl_tr_gearbox_temperature_pinion_os",
                                "dl_tr_gearbox_temperature_pinion_ds"
                              ],
                "cond_par": {
                              "cond_thresholds": { "threshold": 115.0 },
                              "cond_op": [">"]
                },
                
                "scale": {
                          "scale_needed": "false"
                        },
                "trimmer": {
                            "trim_needed": "true",
                            "trim_sig": [  "eg_st2_millstatus" ],
                            "trim_op": ["=="],
                            "trim_thr": [2],
                            "trim_logic_op": ["&"],
                            "trim_offset_needed": "true",
                            "trim_offset_start":5, 
                            "trim_offset_end":5
                      },
                "how":"any",
                "process_id":"TensionReel"
            }
}
'''

# COMMAND ----------

rule5 = '''
{
    "low_coolingtable_flow": {
                "rule_type": "independent",
                "function": "threshold_compare_mod",
                "input_signal": [
                                "dg_incooltable_flow"
                              ],
                "cond_par": {
                              "cond_thresholds": { "threshold": 2500.0 },
                              "cond_op": ["<"]
                },
                
                "scale": {
                          "scale_needed": "false"
                        },
                "trimmer": {
                            "trim_needed": "true",
                            "trim_sig": [  "eg_st2_millstatus","dg_incooltable_numberactiveslots" ],
                            "trim_op": ["==","=="],
                            "trim_thr": [2,5],
                            "trim_logic_op": ["&"],
                            "trim_offset_needed": "true",
                            "trim_offset_start":20, 
                            "trim_offset_end":20
                      },
                "how":"any",
                "process_id":"CoolingTable"
            }
}
'''

# COMMAND ----------

rule6 = '''{
"pressure_flow_variation": {
					"rule_type":"independent",
					"display": "no",
					"function": "fe_threshold_compare_mod", 
					"input_signal" : [["dg_incooltable_flow","dg_incooltable_pressure"]
									],

					"trimmer":{"trim_needed": "true",
											   "trim_sig": ["eg_st2_millstatus","dg_incooltable_numberactiveslots"],
											   "trim_op": ["==","=="],
											   "trim_thr":[2,5],
											   "trim_logic_op": ["and"],
											   "trim_offset_needed": "true",
											   "trim_offset_start_sec" : 60,
											   "trim_offset_end_sec" : 60,
											   "sample_time_ms": 1
												},
									"cond_par":{"cond_thresholds": {"thr1": 40,"thr2": 0.05},
                                                 "cond_op": [">",">"],
                                                 "cond_seq": ["std","std"],
                                                 "cond_op": [">",">"],
                                                 "cond_logic_op":["and"]
												},
									 "transform":{"to_boolean": "false"
													},								
                                     "process_id":"CoolingTable"
					}
}
'''

# COMMAND ----------

import json
rules = dict()
rs = [rule1, rule2,rule3,rule4,rule5,rule6]
rules['alarms'] = {}
for r in rs:
  if not rules:
    print(r)
  else:
    rules['alarms'].update(json.loads(r))
  

#r = json.loads(rule1)
#r2 = json.loads(rule2)
#r.update(r2)
#r

# COMMAND ----------

rules

# COMMAND ----------



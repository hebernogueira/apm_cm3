# Databricks notebook source
rule = '''{
"variation_pressure": {
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
											   "trim_offset_needed": "false",
											   "trim_offset_start_sec" : 5,
											   "trim_offset_end_sec" : 5,
											   "sample_time_ms": 1
												},
									"cond_par":{"cond_thresholds": {"thr1": 45,"thr2": 0.05},
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

dbutils.notebook.exit(rule)

# COMMAND ----------

rule = '''
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
                            "trim_offset_start":5, 
                            "trim_offset_end":5
                      },
                "how":"any",
                "process_id":"CoolingTable"
            }
}
'''

# COMMAND ----------

rule = '''
{
    "test": {
                "rule_type": "independent",
                "function": "threshold_compare_mod",
                "input_signal": [
                                "dl_st1_gearbox_temperature_bt1_4_bottom_splindrer_os",
                                "dl_st1_gearbox_temperature_bt2_6_top_splindrer_os",
                                "dl_st1_gearbox_temperature_bt3_1_motor_drive_ds",
                                "dl_st1_gearbox_temperature_bt4_3_bottom_splindrer_ds",
                                "dl_st1_gearbox_temperature_bt5_5_top_splindrer_ds",
                                "dl_st1_gearbox_temperature_bt6_2_motor_drive_os",
                                "dl_st2_gearbox_temperature_bt1_4_bottom_splindrer_os",
                                "dl_st2_gearbox_temperature_bt2_6_top_splindrer_os",
                                "dl_st2_gearbox_temperature_bt3_1_motor_drive_ds",
                                "dl_st2_gearbox_temperature_bt4_3_bottom_splindrer_ds",
                                "dl_st2_gearbox_temperature_bt5_5_top_splindrer_ds",
                                "dl_st2_gearbox_temperature_bt6_2_motor_drive_os",
                                "dl_por_gearbox_temperature_mandrel_os",
                                "dl_por_gearbox_temperature_mandrel_ds",
                                "dl_por_gearbox_temperature_pinion_os",
                                "dl_por_gearbox_temperature_pinion_ds",
                                "dl_tr_gearbox_temperature_mandrel_os",
                                "dl_tr_gearbox_temperature_mandrel_ds",
                                "dl_tr_gearbox_temperature_pinion_os",
                                "dl_tr_gearbox_temperature_pinion_ds"
                              ],
                "cond_par": {
                              "cond_thresholds": { "threshold": 100.0 },
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
                "process_id":"test"
            }
}
'''

# COMMAND ----------

rule = '''
{
    "test": {
                "rule_type": "independent",
                "function": "threshold_compare_mod",
                "input_signal": [
                                  ["dl_st2_gearbox_temperature_bt4_3_bottom_splindrer_ds" ]
                              ],
                "cond_par": {
                              "cond_thresholds": { "threshold": 100.0 },
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
                "process_id":"test"
            }
}
'''

# COMMAND ----------


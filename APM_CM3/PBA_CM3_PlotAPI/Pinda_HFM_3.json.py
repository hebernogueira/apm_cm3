# Databricks notebook source
# MAGIC %md
# MAGIC ## Head

# COMMAND ----------

# DBTITLE 1,Head
head = '''
  "head":{
        "plant": "Pinda",
        "initialization": {
                            "store_name": "gdaaproddls", 
                            "container": "plantdata", 
                            "plant": "pinda", 
                            "machine_center": "201_hfm1",
                            "path": "landing/pinda/iba/standard/material_time/201_hfm1/2022"
                          },
        "store_info":{
                      "database": "daaentdevg2",
                      "table":"pba_cpm_hfm_status_table",
                      "data_dict":{
                                  "Alarm_ID":	"string",
                                  "Coil_ID": "string",
                                  "Status": "int",
                                  "Status_Detail": "string",
                                  "Comment": "string",
                                  "Timestamp": "string"
                                }
                    }
	},
   '''

# COMMAND ----------

# MAGIC %md
# MAGIC ##Alarms

# COMMAND ----------

# MAGIC %md
# MAGIC ####al19 - Alarm 19:
# MAGIC al19 looks for spikes in the Magnescale Signals, both positive and negative peaks.

# COMMAND ----------

 al19_F1 = ''' "al19_F1":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F1_EnHydPosition_DS_Meas",
                      "dg_F1_ExHydPosition_DS_Meas",
                      "dg_F1_EnHydPosition_OS_Meas",
                      "dg_F1_ExHydPosition_OS_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 0.0000160,
                             "trim_value": [1,1],
                             "start_offset":25,
                             "end_offset":2,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F1"
                           }
                    },
      '''

# COMMAND ----------

 al19_F2 = ''' "al19_F2":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F2_EnHydPosition_DS_Meas",
                      "dg_F2_ExHydPosition_DS_Meas",
                      "dg_F2_EnHydPosition_OS_Meas",
                      "dg_F2_ExHydPosition_OS_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 0.0000160,
                             "trim_value": [1,1],
                             "start_offset":25,
                             "end_offset":2,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F2"
                           }
                    },
      '''

# COMMAND ----------

 al19_F3 = ''' "al19_F3":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F3_EnHydPosition_DS_Meas",
                      "dg_F3_ExHydPosition_DS_Meas",
                      "dg_F3_EnHydPosition_OS_Meas",
                      "dg_F3_ExHydPosition_OS_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 0.0000160,
                             "trim_value": [1,1],
                             "start_offset":25,
                             "end_offset":2,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F3"
                           }
                    },
      '''

# COMMAND ----------

 al19_F4 = ''' "al19_F4":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F1_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F4_EnHydPosition_DS_Meas",
                      "dg_F4_ExHydPosition_DS_Meas",
                      "dg_F4_EnHydPosition_OS_Meas",
                      "dg_F4_ExHydPosition_OS_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 0.0000160,
                             "trim_value": [1,1],
                             "start_offset":25,
                             "end_offset":2,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F4"
                           }
                    },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al21a - Alarm 21a: 
# MAGIC al21a checks if the difference of position on the Magnescales between Entry and Exit of each stand is greater than threshold

# COMMAND ----------

# DBTITLE 0,al21a - Alarm
al21a_F1 = ''' "al21a_F1":{
              "function": "operation_between_signals",
              "additional_signal": {
                                "add_signal_needed": false
                                },
              "input_signal" : [
                                ["dg_F1_EnHydPosition_DS_Meas","dg_F1_ExHydPosition_DS_Meas"],
                                ["dg_F1_EnHydPosition_OS_Meas","dg_F1_ExHydPosition_OS_Meas"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 5,
                                   "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F1_Loaded",
                                          "dg_F1_Loaded"
                                          ]
                        },
              "args": {
                       "threshold": 0.005,
                       "abs": true,
                       "signal_op": "-",
                       "scale_factor": 1.0,
                       "threshold_op": ">",
                       "how": "any",
                       "mach_id": "F1"
                     }
              },

'''

# COMMAND ----------

# DBTITLE 0,al21a - Alarm
al21a_F2 = ''' "al21a_F2":{
              "function": "operation_between_signals",
              "additional_signal": {
                                "add_signal_needed": false
                                },
              "input_signal" : [
                                ["dg_F2_EnHydPosition_DS_Meas","dg_F2_ExHydPosition_DS_Meas"],
                                ["dg_F2_EnHydPosition_OS_Meas","dg_F2_ExHydPosition_OS_Meas"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 5,
                                   "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F2_Loaded",
                                          "dg_F2_Loaded"
                                          ]
                        },
              "args": {
                       "threshold": 0.005,
                       "abs": true,
                       "signal_op": "-",
                       "scale_factor": 1.0,
                       "threshold_op": ">",
                       "how": "any",
                       "mach_id": "F2"
                     }
              },

'''

# COMMAND ----------

# DBTITLE 0,al21a - Alarm
al21a_F3 = ''' "al21a_F3":{
              "function": "operation_between_signals",
              "additional_signal": {
                                "add_signal_needed": false
                                },
              "input_signal" : [
                                ["dg_F3_EnHydPosition_DS_Meas","dg_F3_ExHydPosition_DS_Meas"],
                                ["dg_F3_EnHydPosition_OS_Meas","dg_F3_ExHydPosition_OS_Meas"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 5,
                                   "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F3_Loaded",
                                          "dg_F3_Loaded"
                                          ]
                        },
              "args": {
                       "threshold": 0.005,
                       "abs": true,
                       "signal_op": "-",
                       "scale_factor": 1.0,
                       "threshold_op": ">",
                       "how": "any",
                       "mach_id": "F3"
                     }
              },

'''

# COMMAND ----------

# DBTITLE 0,al21a - Alarm
al21a_F4 = ''' "al21a_F4":{
              "function": "operation_between_signals",
              "additional_signal": {
                                "add_signal_needed": false
                                },
              "input_signal" : [
                                ["dg_F4_EnHydPosition_DS_Meas","dg_F4_ExHydPosition_DS_Meas"],
                                ["dg_F4_EnHydPosition_OS_Meas","dg_F4_ExHydPosition_OS_Meas"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 5,
                                   "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F4_Loaded",
                                          "dg_F4_Loaded"
                                          ]
                        },
              "args": {
                       "threshold": 0.005,
                       "abs": true,
                       "signal_op": "-",
                       "scale_factor": 1.0,
                       "threshold_op": ">",
                       "how": "any",
                       "mach_id": "F4"
                     }
              },

'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al21b - Alarm 21b:
# MAGIC al21b: checks if the sum of the Magnescales of both Entry and Exit divided by 2(scale 0.5) from one side minus the sum of the the other side is greater than threshold
# MAGIC ######abs( ((ds entry + ds exit) - (os entry + os exit))/2) > threshold

# COMMAND ----------

# DBTITLE 0,al21b - Alarm
al21b_F1 = ''' "al21b_F1":{
                  "function": "diff_sides",
                  "additional_signal": {
                                        "add_signal_needed": false
                                              },
                  "input_signal" : [
                                    ["dg_F1_EnHydPosition_DS_Meas","dg_F1_ExHydPosition_DS_Meas","dg_F1_EnHydPosition_OS_Meas","dg_F1_ExHydPosition_OS_Meas"]
                    ],
                 "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 5,
                                   "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F1_Loaded"
                                          ]
                            },
                  "args": {
                           "abs":true,
                           "threshold":0.005,
                           "threshold_op":">",
                           "how": "any",
                           "mach_id": "F1"
                         }
                  },

'''

# COMMAND ----------

# DBTITLE 0,al21b - Alarm
al21b_F2 = ''' "al21b_F2":{
                  "function": "diff_sides",
                  "additional_signal": {
                                        "add_signal_needed": false
                                              },
                  "input_signal" : [
                                    ["dg_F2_EnHydPosition_DS_Meas","dg_F2_ExHydPosition_DS_Meas","dg_F2_EnHydPosition_OS_Meas","dg_F2_ExHydPosition_OS_Meas"]
                    ],
                 "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 5,
                                   "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F2_Loaded"
                                          ]
                            },
                  "args": {
                           "abs":true,
                           "threshold":0.005,
                           "threshold_op":">",
                           "how": "any",
                           "mach_id": "F2"
                         }
                  },

'''

# COMMAND ----------

# DBTITLE 0,al21b - Alarm
al21b_F3 = ''' "al21b_F3":{
                  "function": "diff_sides",
                  "additional_signal": {
                                        "add_signal_needed": false
                                              },
                  "input_signal" : [
                                    ["dg_F3_EnHydPosition_DS_Meas","dg_F3_ExHydPosition_DS_Meas","dg_F3_EnHydPosition_OS_Meas","dg_F3_ExHydPosition_OS_Meas"]
                    ],
                 "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 5,
                                   "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F3_Loaded"
                                          ]
                            },
                  "args": {
                           "abs":true,
                           "threshold":0.005,
                           "threshold_op":">",
                           "how": "any",
                           "mach_id": "F3"
                         }
                  },

'''

# COMMAND ----------

# DBTITLE 0,al21b - Alarm
al21b_F4 = ''' "al21b_F4":{
                  "function": "diff_sides",
                  "additional_signal": {
                                        "add_signal_needed": false
                                              },
                  "input_signal" : [
                                    ["dg_F4_EnHydPosition_DS_Meas","dg_F4_ExHydPosition_DS_Meas","dg_F4_EnHydPosition_OS_Meas","dg_F4_ExHydPosition_OS_Meas"]
                    ],
                 "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 5,
                                   "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F4_Loaded"
                                          ]
                            },
                  "args": {
                           "abs":true,
                           "threshold":0.005,
                           "threshold_op":">",
                           "how": "any",
                           "mach_id": "F4"
                         }
                  },

'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al22a - Alarm 22a:
# MAGIC al22a checks the mean voltage of servo valves Group 1 before bite (-2.5s < interval < - 0.5s) 

# COMMAND ----------

# DBTITLE 0,al22a - Alarm 22a
al22a_F1 = ''' "al22a_F1":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"],
                                              ["dg_F1_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                	"dl_F1_Servo_Valve_OS_Group1",
                    "dl_F1_Servo_Valve_DS_Group1"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -0.5,
                       "dt02": -2.5,
                       "mach_id": "F1"
                     }
              },
      '''

# COMMAND ----------

# DBTITLE 0,al22a - Alarm 22a
al22a_F2 = ''' "al22a_F2":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F2_Loaded","eg_TimeIndex"],
                                              ["dg_F2_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F2_Servo_Valve_OS_Group_1",
                    "dl_F2_Servo_Valve_DS_Group_1"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -0.5,
                       "dt02": -2.5,
                       "mach_id": "F2"
                     }
              },
      '''

# COMMAND ----------

# DBTITLE 0,al22a - Alarm 22a
al22a_F3 = ''' "al22a_F3":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F3_Loaded","eg_TimeIndex"],
                                              ["dg_F3_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F3_Servo_Valve_OS_Group_1",
                    "dl_F3_Servo_Valve_DS_Group_1"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -0.5,
                       "dt02": -2.5,
                       "mach_id": "F3"
                     }
              },
      '''

# COMMAND ----------

# DBTITLE 0,al22a - Alarm 22a
al22a_F4 = ''' "al22a_F4":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F4_Loaded","eg_TimeIndex"],
                                              ["dg_F4_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F4_Servo_Valve_OS_Group_1",
                    "dl_F4_Servo_Valve_DS_Group_1"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -0.5,
                       "dt02": -2.5,
                       "mach_id": "F4"
                     }
              },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al22b - Alarm 22b:
# MAGIC alarm22b checks the mean voltage difference between part 01 and part 06 in the servo valves of Group 01 during period of Mill Run Mode =2  until the Stand is Unloaded (falling edge on Load Sig)

# COMMAND ----------

# DBTITLE 0,al22b - Alarm 22b
al22b_F1 = '''  "al22b_F1":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F1_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F1_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                        "dl_F1_Servo_Valve_OS_Group1",
                        "dl_F1_Servo_Valve_DS_Group1"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":16,
                           "parts":[["part01","part016","mean",">",1]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id": "F1"
                         }
                  },
      '''

# COMMAND ----------

# DBTITLE 0,al22b - Alarm 22b
al22b_F2 = '''  "al22b_F2":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                        "dl_F2_Servo_Valve_OS_Group_1",
                        "dl_F2_Servo_Valve_DS_Group_1"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":16,
                           "parts":[["part01","part016","mean",">",1]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id": "F2"
                         }
                  },
      '''

# COMMAND ----------

# DBTITLE 0,al22b - Alarm 22b
al22b_F3 = '''  "al22b_F3":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                        "dl_F3_Servo_Valve_OS_Group_1",
                        "dl_F3_Servo_Valve_DS_Group_1"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":16,
                           "parts":[["part01","part016","mean",">",1]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id":"F3"
                         }
                  },
      '''

# COMMAND ----------

# DBTITLE 0,al22b - Alarm 22b
al22b_F4 = '''  "al22b_F4":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                        "dl_F4_Servo_Valve_OS_Group_1",
                        "dl_F4_Servo_Valve_DS_Group_1"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":16,
                           "parts":[["part01","part016","mean",">",1]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id":"F4"
                         }
                  },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al22c - Alarm 22c:
# MAGIC alarm22c checks if the signal of servo valves Group 01 from one side has stayed below -5 Volts for a while (1 seconds) and the other did not

# COMMAND ----------

al22c_F1 = ''' "al22c_F1":{
              "function": "two_signals_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                        ["dl_F1_Servo_Valve_OS_Group1","dl_F1_Servo_Valve_DS_Group1"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                              "offset_needed": false,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 0,
                             "offset_end": -60
                             },
                         "trim_signal": [
                                          "dg_F1_Loaded"
                                          ]
                        },
              "args": {
                       "how":"any",
                       "value_threshold": -5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 1,
                       "mach_id":"F1"
                     }
                },
'''

# COMMAND ----------

al22c_F2 = ''' "al22c_F2":{
              "function": "two_signals_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                        ["dl_F2_Servo_Valve_OS_Group_1","dl_F2_Servo_Valve_DS_Group_1"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                          "offset": {
                              "offset_needed": false,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 0,
                             "offset_end": 60
                             },
                         "trim_signal": [
                                          "dg_F2_Loaded"
                                          ]
                        },
              "args": {
                       "how":"any",
                       "value_threshold": -5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 1,
                       "mach_id":"F2"
              }
                     
             },
'''

# COMMAND ----------

al22c_F3 = '''"al22c_F3":{
              "function": "two_signals_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                        ["dl_F3_Servo_Valve_OS_Group_1","dl_F3_Servo_Valve_DS_Group_1"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                              "offset_needed": false,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 0,
                             "offset_end": 60
                             },
                         "trim_signal": [
                                          "dg_F3_Loaded"
                                          ]
                        },
              "args": {
                       "how":"any",
                       "value_threshold": -5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 1,
                       "mach_id":"F3"
                     }
                    },
'''

# COMMAND ----------

al22c_F4 = ''' "al22c_F4":{
              "function": "two_signals_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                        ["dl_F4_Servo_Valve_OS_Group_1","dl_F4_Servo_Valve_DS_Group_1"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                              "offset_needed": false,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 0,
                             "offset_end": 60
                             },
                         "trim_signal": [
                                          "dg_F4_Loaded"
                                          ]
                        },
              "args": {
                       "how":"any",
                       "value_threshold": -5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 1,
                       "mach_id":"F4"
                     }
                  },
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ###al22d - Alarm 22d: 
# MAGIC al22d checks the mean voltage of servo valves Group 2 before bite (-2.5s < interval < - 0.5s)

# COMMAND ----------

al22d_F1 = ''' "al22d_F1":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"],
                                              ["dg_F1_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F1_Servo_Valve_OS_Group2",
                    "dl_F1_Servo_Valve_DS_Group2"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -0.5,
                       "dt02": -2.5,
                       "mach_id": "F1"
                     }
              },
      '''

# COMMAND ----------

al22d_F2 = ''' "al22d_F2":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F2_Loaded","eg_TimeIndex"],
                                              ["dg_F2_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F2_Servo_Valve_OS_Group_2",
                    "dl_F2_Servo_Valve_DS_Group_2"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -0.5,
                       "dt02": -2.5,
                       "mach_id": "F2"
                     }
              },
      '''

# COMMAND ----------

al22d_F3 = ''' "al22d_F3":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F3_Loaded","eg_TimeIndex"],
                                              ["dg_F3_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F3_Servo_Valve_OS_Group_2",
                    "dl_F3_Servo_Valve_DS_Group_2"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -0.5,
                       "dt02": -2.5,
                       "mach_id": "F3"
                     }
              },
      '''

# COMMAND ----------

al22d_F4 = ''' "al22d_F4":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F4_Loaded","eg_TimeIndex"],
                                              ["dg_F4_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F4_Servo_Valve_OS_Group_2",
                    "dl_F4_Servo_Valve_DS_Group_2"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -0.5,
                       "dt02": -2.5,
                       "mach_id": "F4"
                     }
              },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ###al22e - Alarm 22e:
# MAGIC alarm22e checks the mean voltage difference between part 01 and part 06 in the servo valves of Group 02 during period of Mill Run Mode =2  until the Stand is Unloaded (falling edge on Load Sig)

# COMMAND ----------

al22e_F1 = '''  "al22e_F1":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F1_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F1_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                      "dl_F1_Servo_Valve_OS_Group2",
                      "dl_F1_Servo_Valve_DS_Group2"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":16,
                           "parts":[["part01","part016","mean",">",1.0]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id": "F1"
                         }
                  },
      '''

# COMMAND ----------

al22e_F2 = '''  "al22e_F2":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                      "dl_F2_Servo_Valve_OS_Group_2",
                      "dl_F2_Servo_Valve_DS_Group_2"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":16,
                           "parts":[["part01","part016","mean",">",1.0]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id": "F2"
                         }
                  },
      '''

# COMMAND ----------

al22e_F3 = '''  "al22e_F3":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                      "dl_F3_Servo_Valve_OS_Group_2",
                      "dl_F3_Servo_Valve_DS_Group_2"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":16,
                           "parts":[["part01","part016","mean",">",1.0]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id": "F3"
                         }
                  },
      '''

# COMMAND ----------

al22e_F4 = '''  "al22e_F4":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                      "dl_F4_Servo_Valve_OS_Group_2",
                      "dl_F4_Servo_Valve_DS_Group_2"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":16,
                           "parts":[["part01","part016","mean",">",1.0]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id": "F4"
                         }
                  },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al22f - Alarm 22f:
# MAGIC alarm22f checks if the signal of servo valves Group 02 from one side has stayed below -5 Volts for a while (1 seconds) and the other did not

# COMMAND ----------

al22f_F1 = ''' "al22f_F1":{
              "function": "two_signals_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                        ["dl_F1_Servo_Valve_OS_Group2","dl_F1_Servo_Valve_DS_Group2"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": false
                             },
                         "trim_signal": [
                                          "dg_F1_Loaded"
                                          ]
                        },
              "args": {
                       "how":"any",
                       "value_threshold": -5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 1,
                       "mach_id":"F1"
                     }
                },
'''

# COMMAND ----------

al22f_F2 = ''' "al22f_F2":{
              "function": "two_signals_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                        ["dl_F2_Servo_Valve_OS_Group_2","dl_F2_Servo_Valve_DS_Group_2"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": false
                             },
                         "trim_signal": [
                                          "dg_F2_Loaded"
                                          ]
                        },
              "args": {
                       "how":"any",
                       "value_threshold": -5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 1,
                       "mach_id":"F2"
                     }
                },
'''

# COMMAND ----------

al22f_F3 = ''' "al22f_F3":{
              "function": "two_signals_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                        ["dl_F3_Servo_Valve_OS_Group_2","dl_F3_Servo_Valve_DS_Group_2"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": false
                             },
                         "trim_signal": [
                                          "dg_F3_Loaded"
                                          ]
                        },
              "args": {
                       "how":"any",
                       "value_threshold": -5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 1,
                       "mach_id":"F3"
                     }
                },
'''

# COMMAND ----------

al22f_F4 = ''' "al22f_F4":{
              "function": "two_signals_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                        ["dl_F4_Servo_Valve_OS_Group_2","dl_F4_Servo_Valve_DS_Group_2"]
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": false
                             },
                         "trim_signal": [
                                          "dg_F4_Loaded"
                                          ]
                        },
              "args": {
                       "how":"any",
                       "value_threshold": -5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 1,
                       "mach_id":"F4"
                     }
                },
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al23a - Alarm 23a:
# MAGIC alarm23a checks if the difference between the pressure piston OS and DS during Bite is greater thatn threshold

# COMMAND ----------

al23a_F1 = '''   "al23a_F1":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dl_F1_Pressure_Piston_DS","dl_F1_Pressure_Piston_OS"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["bite",5,">"]],
                       "offset_start": 0,
                       "offset_end":0,
                       "dt01": -19,
                       "dt02": -20,
                       "how": "any",
                       "mach_id": "F1"
                     }
              },
      '''

# COMMAND ----------

al23a_F2 = '''   "al23a_F2":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dl_F2_Pressure_Piston_DS","dl_F2_Pressure_Piston_OS"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["bite",5,">"]],
                       "offset_start": 0,
                       "offset_end":0,
                       "dt01": -19,
                       "dt02": -20,
                       "how": "any",
                       "mach_id": "F2"
                     }
              },
      '''

# COMMAND ----------

al23a_F3 = '''   "al23a_F3":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dl_F3_Pressure_Piston_DS","dl_F3_Pressure_Piston_OS"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["bite",5,">"]],
                       "offset_start": 0,
                       "offset_end":0,
                       "dt01": -19,
                       "dt02": -20,
                       "how": "any",
                       "mach_id": "F3"
                     }
              },
      '''

# COMMAND ----------

al23a_F4 = '''   "al23a_F4":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dl_F4_Pressure_Piston_DS","dl_F4_Pressure_Piston_OS"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["bite",8,">"]],
                       "offset_start": 0,
                       "offset_end":0,
                       "dt01": -19,
                       "dt02": -20,
                       "how": "any",
                       "mach_id": "F4"
                     }
              },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al23b - Alarm 23b:
# MAGIC al23b looks for spikes in the Pressure Signals, both positive and negative peaks.
# MAGIC 
# MAGIC Offset the signal load +5 sec in the beginning and -5 seconds at the end. The algorithm uses a 2 sec moving average, thus in the beginning only 3sec is set to result 5 sec offset.

# COMMAND ----------

al23b_F1 = ''' "al23b_F1":{
              "function": "peak_detection_mod",
              "additional_signal": {
                                    "add_signal_needed": false
                                          },
              "input_signal" : [
                "dl_F1_Pressure_Piston_DS",
                "dl_F1_Pressure_Piston_OS"
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": true,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 3,
                             "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F1_Loaded",
                                          "dg_F1_Loaded"
                                          ]
                        },
              "args": {
                       "high_threshold": 3,
                       "low_threshold": -3,
                       "how": "any",
                       "mach_id": "F1"
                     }
              },
'''

# COMMAND ----------

al23b_F2 = ''' "al23b_F2":{
              "function": "peak_detection_mod",
              "additional_signal": {
                                    "add_signal_needed": false
                                          },
              "input_signal" : [
                "dl_F2_Pressure_Piston_DS",
                "dl_F2_Pressure_Piston_OS"
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": true,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 3,
                             "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F2_Loaded",
                                          "dg_F2_Loaded"
                                          ]
                        },
              "args": {
                       "high_threshold": 3,
                       "low_threshold": -3,
                       "how": "any",
                       "mach_id": "F2"
                     }
              },
'''

# COMMAND ----------

al23b_F3 = ''' "al23b_F3":{
              "function": "peak_detection_mod",
              "additional_signal": {
                                    "add_signal_needed": false
                                          },
              "input_signal" : [
                "dl_F3_Pressure_Piston_DS",
                "dl_F3_Pressure_Piston_OS"
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": true,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 3,
                             "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F3_Loaded",
                                          "dg_F3_Loaded"
                                          ]
                        },
              "args": {
                       "high_threshold": 3,
                       "low_threshold": -3,
                       "how": "any",
                       "mach_id": "F3"
                     }
              },
'''

# COMMAND ----------

al23b_F4 = ''' "al23b_F4":{
              "function": "peak_detection_mod",
              "additional_signal": {
                                    "add_signal_needed": false
                                          },
              "input_signal" : [
                "dl_F4_Pressure_Piston_DS",
                "dl_F4_Pressure_Piston_OS"
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": true,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 3,
                             "offset_end": -5
                             },
                         "trim_signal": [
                                          "dg_F4_Loaded",
                                          "dg_F4_Loaded"
                                          ]
                        },
              "args": {
                       "high_threshold": 3,
                       "low_threshold": -3,
                       "how": "any",
                       "mach_id": "F4"
                     }
              },
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al24 - Alarm 24:
# MAGIC al24 is triggerd if the setpoint and actual value of signal is greater than 150 Ton. 

# COMMAND ----------

al24_F1 = '''   "al24_F1":{
                "function": "operation_between_signals",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  ["dg_F1_RollForce_DS_Meas", "dg_F1_RollForce_DS_Ref"],
                  ["dg_F1_RollForce_OS_Meas", "dg_F1_RollForce_OS_Ref"]
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                             "offset_needed": false
                             },
                           "trim_signal": [
                                            "dg_F1_Loaded",
                                            "dg_F1_Loaded"
                                            ]
                          },
                "args": {
                         "threshold": 150,
                         "abs": false,
                         "signal_op": "-",
                         "threshold_op": ">",
                         "scale_factor": 1.0,
                         "how": "any",
                         "mach_id": "F1"
                       }
              },
'''

# COMMAND ----------

al24_F2 = '''   "al24_F2":{
                "function": "operation_between_signals",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  ["dg_F2_RollForce_DS_Meas", "dg_F2_RollForce_DS_Ref"],
                  ["dg_F2_RollForce_OS_Meas", "dg_F2_RollForce_OS_Ref"]
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                             "offset_needed": false
                             },
                           "trim_signal": [
                                            "dg_F2_Loaded",
                                            "dg_F2_Loaded"
                                            ]
                          },
                "args": {
                         "threshold": 150,
                         "abs": false,
                         "signal_op": "-",
                         "threshold_op": ">",
                         "scale_factor": 1.0,
                         "how": "any",
                         "mach_id": "F2"
                       }
              },
'''

# COMMAND ----------

al24_F3 = '''   "al24_F3":{
                "function": "operation_between_signals",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  ["dg_F3_RollForce_DS_Meas", "dg_F3_RollForce_DS_Ref"],
                  ["dg_F3_RollForce_OS_Meas", "dg_F3_RollForce_OS_Ref"]
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                             "offset_needed": false
                             },
                           "trim_signal": [
                                            "dg_F3_Loaded",
                                            "dg_F3_Loaded"
                                            ]
                          },
                "args": {
                         "threshold": 150,
                         "abs": false,
                         "signal_op": "-",
                         "threshold_op": ">",
                         "scale_factor": 1.0,
                         "how": "any",
                         "mach_id": "F3"
                       }
              },
'''

# COMMAND ----------

al24_F4 = '''   "al24_F4":{
                "function": "operation_between_signals",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  ["dg_F4_RollForce_DS_Meas", "dg_F4_RollForce_DS_Ref"],
                  ["dg_F4_RollForce_OS_Meas", "dg_F4_RollForce_OS_Ref"]
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                             "offset_needed": false
                             },
                           "trim_signal": [
                                            "dg_F4_Loaded",
                                            "dg_F4_Loaded"
                                            ]
                          },
                "args": {
                         "threshold": 150,
                         "abs": false,
                         "signal_op": "-",
                         "threshold_op": ">",
                         "scale_factor": 1.0,
                         "how": "any",
                         "mach_id": "F4"
                       }
              },
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al25 - Alarm 25:
# MAGIC al25 detects a change in operation mode. Normally the mill should run in absolute mode. If it occurs a change to relative mode, then alarm!

# COMMAND ----------

al25_F1 = ''' "al25_F1":{
                "function": "count_edges",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  "dl_F1_AGC_Relative"
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 30,
                                   "offset_end": -30
                             },
                           "trim_signal": [
                                            "dg_F1_Loaded"
                                            ]
                          },
                "args": {
                         "how": "any",
                         "mach_id": "F1"
                       }
            },
'''

# COMMAND ----------

al25_F2 = ''' "al25_F2":{
                "function": "count_edges",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  "dl_F2_AGC_Relative"
                  ],
               "trimmer": {
                          "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 30,
                                   "offset_end": -30
                             },
                           "trim_signal": [
                                            "dg_F2_Loaded"
                                            ]
                          },
                "args": {
                         "how": "any",
                         "mach_id": "F2"
                       }
            },
'''

# COMMAND ----------

al25_F3 = ''' "al25_F3":{
                "function": "count_edges",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  "dl_F3_AGC_Relative"
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 30,
                                   "offset_end": -30
                             },
                           "trim_signal": [
                                            "dg_F3_Loaded"
                                            ]
                          },
                "args": {
                         "how": "any",
                         "mach_id": "F3"
                       }
            },
'''

# COMMAND ----------

al25_F4 = ''' "al25_F4":{
                "function": "count_edges",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  "dl_F4_AGC_Relative"
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                                   "offset_needed": true,
                                   "offset_index_signal": "eg_TimeIndex",
                                   "offset_start": 30,
                                   "offset_end": -30
                             },
                           "trim_signal": [
                                            "dg_F4_Loaded"
                                            ]
                          },
                "args": {
                         "how": "any",
                         "mach_id": "F4"
                       }
            },
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ###al26 - Alarm 26:
# MAGIC al26 Evaluate if the pressure in the middle (part03 and part04) of rolling is between a range. Alarm is triggered if > upper threshold or < lower

# COMMAND ----------

al26_F1 = ''' "al26_F1":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F1_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F1_Pressure_Rod_Side"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":16,
                           "lower_threshold":11,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part03","part04"]],
                           "modifier": "mean",
                           "multiple_eval": true,
                           "mach_id": "F1"
                         }
                  },
      '''

# COMMAND ----------

al26_F2 = ''' "al26_F2":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F2_Pressure_Rod_Side"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":16,
                           "lower_threshold":11,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part03","part04"]],
                           "modifier": "mean",
                           "multiple_eval": true,
                           "mach_id": "F2"
                         }
                  },
      '''

# COMMAND ----------

al26_F3 = ''' "al26_F3":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F3_Pressure_Rod_Side"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":16,
                           "lower_threshold":11,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part03","part04"]],
                           "modifier": "mean",
                           "multiple_eval": true,
                           "mach_id": "F3"
                         }
                  },
      '''

# COMMAND ----------

al26_F4 = ''' "al26_F4":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F4_Pressure_Rod_Side"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":16,
                           "lower_threshold":11,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part03","part04"]],
                           "modifier": "mean",
                           "multiple_eval": true,
                           "mach_id": "F4"
                         }
                  },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### DISABLED al27 - Alarm 27
# MAGIC 
# MAGIC Diferentes limites inferiores e superiores

# COMMAND ----------

al27 = ''' "al27":{
              "function": "alarm_disabled",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"],
                                              ["dg_F2_Loaded","eg_TimeIndex"],
                                              ["dg_F3_Loaded","eg_TimeIndex"],
                                              ["dg_F4_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                	"dg_F1_ForwardSlip_Calc",
                    "dg_F2_ForwardSlip_Calc",
                    "dg_F3_ForwardSlip_Calc",
                    "dg_F4_ForwardSlip_Calc"
                ],
              "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "how": "any",
                       "region": [["body",10,"mean",">"]],
                       "dt01": -2,
                       "dt02": -6
                     }
              },
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al28a - Alarm 28a:
# MAGIC Checks if voltage before bite is not greater than 1V

# COMMAND ----------

al28a_F1 = ''' "al28a_F1":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"],
                                              ["dg_F1_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                	"dl_F1_Servo_Bend_Positive",
                    "dl_F1_Servo_Bend_Negative"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1.0,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -2,
                       "dt02": -6,
                       "mach_id": "F1"
                     }
              },
'''

# COMMAND ----------

al28a_F2 = ''' "al28a_F2":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F2_Loaded","eg_TimeIndex"],
                                              ["dg_F2_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F2_Servo_Bend_Positive",
                    "dl_F2_Servo_Bend_Negative"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1.0,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -2,
                       "dt02": -6,
                       "mach_id":"F2"
                     }
              },
'''

# COMMAND ----------

al28a_F3 = ''' "al28a_F3":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F3_Loaded","eg_TimeIndex"],
                                              ["dg_F3_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F3_Servo_Bend_Positive",
                    "dl_F3_Servo_Bend_Negative"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1.0,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -2,
                       "dt02": -6,
                       "mach_id": "F3"
                     }
              },
'''

# COMMAND ----------

al28a_F4 = ''' "al28a_F4":{
              "function": "slice_by_time",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F4_Loaded","eg_TimeIndex"],
                                              ["dg_F4_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                    "dl_F4_Servo_Bend_Positive",
                    "dl_F4_Servo_Bend_Negative"
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1.0,
                       "abs": true,
                       "how": "any",
                       "region": [["bite",1.4,"mean",">"]],
                       "dt01": -2,
                       "dt02": -6,
                       "mach_id": "F4"
                     }
              },
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al28b - Alarm 28b:
# MAGIC Checks if the difference between part 01 and part 06 is greater than 1V

# COMMAND ----------

al28b_F1 = '''  "al28b_F1":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F1_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F1_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F1_Servo_Bend_Positive",
                    "dl_F1_Servo_Bend_Negative"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",                           
                           "nbr_of_parts":6,
                           "parts":[["part01","part06","mean",">",1.4]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id": "F1"
                         }
                  },
      '''

# COMMAND ----------

al28b_F2 = '''  "al28b_F2":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F2_Servo_Bend_Positive",
                    "dl_F2_Servo_Bend_Negative"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":6,
                           "parts":[["part01","part06","mean",">",1.4]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id":"F2"
                         }
                  },
      '''

# COMMAND ----------

al28b_F3 = '''  "al28b_F3":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F3_Servo_Bend_Positive",
                    "dl_F3_Servo_Bend_Negative"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":6,
                           "parts":[["part01","part06","mean",">",1.4]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id":"F3"
                         }
                  },
      '''

# COMMAND ----------

al28b_F4 = '''  "al28b_F4":{
                  "function": "slice_in_six",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F4_Servo_Bend_Positive",
                    "dl_F4_Servo_Bend_Negative"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "trim_value": [1,2],
                           "abs": true,
                           "how": "any",
                           "nbr_of_parts":6,
                           "parts":[["part01","part06","mean",">",1.4]],
                           "start_offset":0,
                           "end_offset":0.5,
                           "mach_id": "F4"
                         }
                  },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al30a - Alarm 30:
# MAGIC Function: diff_in_areas
# MAGIC 
# MAGIC Equipment: Tensiometer 
# MAGIC 
# MAGIC Machine part: F1/F2/F3/F4
# MAGIC 
# MAGIC Description: Difference in positive before bite and in the middle of body greater than threshold

# COMMAND ----------

al30a_F1 = '''   "al30a_F1":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F1_PositiveBending_Meas","dg_F1_PositiveBending_Ref"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["bite",7.5,">"],["body",7.5,">"]],
                       "offset_start": 60,
                       "offset_end":-60,
                       "dt01": -1,
                       "dt02": -2,
                       "how": "any",
                       "mach_id": "F1"
                     }
              },
      '''

# COMMAND ----------

al30a_F2 = '''   "al30a_F2":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F2_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F2_PositiveBending_Meas","dg_F2_PositiveBending_Ref"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["bite",7.5,">"],["body",7.5,">"]],
                       "offset_start": 60,
                       "offset_end":-60,
                       "dt01": -1,
                       "dt02": -2,
                       "how": "any",
                       "mach_id": "F2"
                     }
              },
      '''

# COMMAND ----------

al30a_F3 = '''   "al30a_F3":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F3_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F3_PositiveBending_Meas","dg_F3_PositiveBending_Ref"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                      "threshold": 1,
                       "abs": true,
                       "region": [["bite",7.5,">"],["body",7.5,">"]],
                       "offset_start": 60,
                       "offset_end":-60,
                       "dt01": -1,
                       "dt02": -2,
                       "how": "any",
                       "mach_id": "F3"
                     }
              },
      '''

# COMMAND ----------

al30a_F4 = '''   "al30a_F4":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F4_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F4_PositiveBending_Meas","dg_F4_PositiveBending_Ref"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                      "threshold": 1,
                       "abs": true,
                       "region": [["bite",7.5,">"],["body",7.5,">"]],
                       "offset_start": 60,
                       "offset_end":-60,
                       "dt01": -1,
                       "dt02": -2,
                       "how": "any",
                       "mach_id": "F4"
                     }
              },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al30b - Alarm 30:
# MAGIC Function: diff_in_areas
# MAGIC 
# MAGIC Equipment: Tensiometer 
# MAGIC 
# MAGIC Machine part: F1/F2/F3/F4

# COMMAND ----------

al30b_F1 = '''   "al30b_F1":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F1_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F1_NegativeBending_Meas","dg_F1_NegativeBending_Ref"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                     "threshold": 1,
                       "abs": true,
                       "region": [["bite",7.5,">"],["body",7.5,">"]],
                       "offset_start": 60,
                       "offset_end":-60,
                       "dt01": -1,
                       "dt02": -2,
                       "how": "any",
                       "mach_id": "F1"
                     }
              },
      '''

# COMMAND ----------

al30b_F2 = '''   "al30b_F2":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F2_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F2_NegativeBending_Meas","dg_F2_NegativeBending_Ref"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["bite",7.5,">"],["body",7.5,">"]],
                       "offset_start": 60,
                       "offset_end":-60,
                       "dt01": -1,
                       "dt02": -2,
                       "how": "any",
                       "mach_id": "F2"
                     }
              },
      '''

# COMMAND ----------

al30b_F3 = '''   "al30b_F3":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F3_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F3_NegativeBending_Meas","dg_F3_NegativeBending_Ref"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["bite",7.5,">"],["body",7.5,">"]],
                       "offset_start": 60,
                       "offset_end":-60,
                       "dt01": -1,
                       "dt02": -2,
                       "how": "any",
                       "mach_id": "F3"
                     }
              },
      '''

# COMMAND ----------

al30b_F4 = '''   "al30b_F4":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F4_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F4_NegativeBending_Meas","dg_F4_NegativeBending_Ref"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["bite",7.5,">"],["body",7.5,">"]],
                       "offset_start": 60,
                       "offset_end":-60,
                       "dt01": -1,
                       "dt02": -2,
                       "how": "any",
                       "mach_id": "F4"
                     }
              },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al35 - Alarm 35:
# MAGIC Evaluates if the average of every single peace is in a range

# COMMAND ----------

al35_F1 =''' "al35_F1":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F1_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F1_TBR_Balance_Pressure"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":150,
                           "lower_threshold":100,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part01","part02","part03","part04","part05","part06"]],
                           "modifier": "mean",
                           "multiple_eval": false,
                           "mach_id": "F1"
                         }
                  },
'''

# COMMAND ----------

al35_F2 =''' "al35_F2":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F2_TBR_Balance_Pressure"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":150,
                           "lower_threshold":100,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part01","part02","part03","part04","part05","part06"]],
                           "modifier": "mean",
                           "multiple_eval": false,
                           "mach_id": "F2"
                         }
                  },
'''

# COMMAND ----------

al35_F3 =''' "al35_F3":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dl_F3_TBR_Balance_Pressure"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":150,
                           "lower_threshold":100,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part01","part02","part03","part04","part05","part06"]],
                           "modifier": "mean",
                           "multiple_eval": false,
                           "mach_id": "F3"
                         }
                  },
'''

# COMMAND ----------

al35_F4 =''' "al35_F4":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
       
                    "dl_F4_TBR_Balance_Pressure"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":150,
                           "lower_threshold":100,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part01","part02","part03","part04","part05","part06"]],
                           "modifier": "mean",
                           "multiple_eval": false,
                           "mach_id": "F4"
                         }
                  },
'''

# COMMAND ----------

# MAGIC %md 
# MAGIC ### al43 - Alarm 43:
# MAGIC Check if the Coolant Pressure has stayed below threshold for a while

# COMMAND ----------

al43_F1 = ''' "al43_F1":{
              "function": "signal_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                	"dg_F1_EnCoolant_Pressure"
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": false
                             },
                         "trim_signal": [
                                          "dg_F1_Loaded"
                                          ]
                        },
              "args": {
                       "value_threshold": 8.5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 5,
                       "mach_id": "F1"
                     }
              },
              
'''

# COMMAND ----------

al43_F2 = ''' "al43_F2":{
              "function": "signal_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                    "dg_F2_EnCoolant_Pressure"
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": false
                             },
                         "trim_signal": [
                                          "dg_F2_Loaded"
                                          ]
                        },
              "args": {
                       "value_threshold": 8.5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 5,
                       "mach_id": "F2"
                     }
              },
              
'''

# COMMAND ----------

al43_F3 = ''' "al43_F3":{
              "function": "signal_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                    "dg_F3_EnCoolant_Pressure"
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": false
                             },
                         "trim_signal": [
                                          "dg_F3_Loaded"
                                          ]
                        },
              "args": {
                       "value_threshold": 8.5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 5,
                       "mach_id": "F3"
                     }
              },
              
'''

# COMMAND ----------

al43_F4 = ''' "al43_F4":{
              "function": "signal_threshold_compare",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              "eg_TimeIndex"
                                            ]
                                          },
              "input_signal" : [
                    "dg_F4_EnCoolant_Pressure"
                ],
             "trimmer": {
                         "trim_needed": true,
                         "trim_value": 1,
                         "op": "==",
                         "offset": {
                             "offset_needed": false
                             },
                         "trim_signal": [
                                          "dg_F4_Loaded"
                                          ]
                        },
              "args": {
                       "value_threshold": 8.5,
                       "area": "below_threshold",
                       "time_op": ">=",
                       "time_threshold": 5,
                       "mach_id":"F4"
                     }
              },
              
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ###DISABLED al44 - Alarm 44
# MAGIC Checks if the coolant flow is between range during rolling 

# COMMAND ----------

al44a = ''' "al44a":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F1_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dg_F1_EnCoolant_Flow",
                    "dg_F2_EnCoolant_Flow",
                    "dg_F3_EnCoolant_Flow",
                    "dg_F4_EnCoolant_Flow"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":120,
                           "lower_threshold":80,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part01","part02","part03","part04","part05","part06"]],
                           "modifier": "",
                           "multiple_eval": false
                         }
                  }
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al47 - Alarm 47 -> disabled -> Top Brushes not in GC
# MAGIC Checks if brushes pressures are within a range during rolling
# MAGIC 
# MAGIC ##### Function: evaluate_parts 
# MAGIC ##### Equipment: Coolant 
# MAGIC ##### Machine part: F2/F3/F4

# COMMAND ----------

al47_F2 = ''' "al47_F2":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F2_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dg_F2_TopBrush_Pressure",
                    "dg_F2_BotBrush_Pressure"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":2.5,
                           "lower_threshold":1.5,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part03","part04"]],
                           "modifier": "mean",
                           "multiple_eval": true,
                           "mach_id": "F2"
                         }
                  },
      '''

# COMMAND ----------

al47_F4 = ''' "al47_F4":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F4_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dg_F4_TopBrush_Pressure",
                    "dg_F4_BotBrush_Pressure"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":2.5,
                           "lower_threshold":1.5,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part03","part04"]],
                           "modifier": "mean",
                           "multiple_eval": true,
                           "mach_id": "F4"
                         }
                  },
      '''

# COMMAND ----------

al47_F3 = ''' "al47_F3":{
                  "function": "evaluate_parts",
                  "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal": [
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"],
                                                  ["dg_F3_Loaded","dl_Gen_MillRunMode","eg_TimeIndex"]
                                                ]
                                              },
                  "input_signal" : [
                    "dg_F3_TopBrush_Pressure",
                    "dg_F3_BotBrush_Pressure"
                    ],
                 "trimmer": {
                             "trim_needed": false
                            },
                  "args": {
                           "upper_threshold":2.5,
                           "lower_threshold":1.5,
                           "op":[">","<"],
                           "trim_value": [1,2],
                           "how": "any",
                           "parts":[["part03","part04"]],
                           "modifier": "mean",
                           "multiple_eval": true,
                           "mach_id": "F4"
                         }
                  },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al48 - Alarm 48 -> disabled -> Top Brushes not in GC
# MAGIC Checks if brushes pressure difference between top and bottom brushes
# MAGIC 
# MAGIC ##### Function: diff_in_areas | Equipment: Coolant | Machine part: F2/F3/F4

# COMMAND ----------

al48_F2 = '''   "al48_F2":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F2_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F2_TopBrush_Pressure","dg_F2_BotBrush_Pressure"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["body",0.5,">"]],
                       "dt01": 0,
                       "dt02": 0,
                       "how": "any",
                       "mach_id": "F2"
                     }
              },
      '''

# COMMAND ----------

al48_F3 = '''   "al48_F3":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F3_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F3_TopBrush_Pressure","dg_F3_BotBrush_Pressure"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["body",0.5,">"]],
                       "dt01": 0,
                       "dt02": 0,
                       "how": "any",
                       "mach_id": "F3"
                     }
              },
      '''

# COMMAND ----------

al48_F4 = '''   "al48_F4":{
              "function": "diff_in_areas",
              "additional_signal": {
                                    "add_signal_needed": true,
                                    "add_signal": [
                                              ["dg_F4_Loaded","eg_TimeIndex"]
                                            ]
                                          },
              "input_signal" : [
                ["dg_F4_TopBrush_Pressure","dg_F4_BotBrush_Pressure"]
                ],
             "trimmer": {
                         "trim_needed": false
                        },
              "args": {
                       "threshold": 1,
                       "abs": true,
                       "region": [["body",0.5,">"]],
                       "dt01": 0,
                       "dt02": 0,
                       "how": "any",
                       "mach_id": "F4"
                     }
              },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al51a - Spikes in Tensiometer speeds

# COMMAND ----------

al51a_F1 = ''' "al51a_F1":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F2_Loaded", "dg_F2_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F12_Tensiometer_RotationalSpeed_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 3,
                             "trim_value": [1,1],
                             "start_offset":25,
                             "end_offset":5,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F1"
                           }
                    },
      '''

# COMMAND ----------

al51a_F2 = ''' "al51a_F2":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F3_Loaded", "dg_F3_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F23_Tensiometer_RotationalSpeed_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 3,
                             "trim_value": [1,1],
                             "start_offset":25,
                             "end_offset":5,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F2"
                           }
                    },
      '''

# COMMAND ----------

al51a_F3 = ''' "al51a_F3":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F4_Loaded", "dg_F4_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F34_Tensiometer_RotationalSpeed_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 3,
                             "trim_value": [1,1],
                             "start_offset":25,
                             "end_offset":5,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F3"
                           }
                    },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al52 - Spikes in the LoadCell interstand

# COMMAND ----------

 al52_F1 = ''' "al52_F1":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F2_Loaded", "dg_F2_Loaded","eg_TimeIndex"],
                                        ["dg_F2_Loaded", "dg_F2_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F12_Tensiometer_LoadCell_Force_OS_Meas",
                      "dg_F12_Tensiometer_LoadCell_Force_DS_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 1500,
                             "trim_value": [1,1],
                             "start_offset":10,
                             "end_offset":10,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F1"
                           }
                    },
      '''

# COMMAND ----------

 al52_F2 = ''' "al52_F2":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F3_Loaded", "dg_F3_Loaded","eg_TimeIndex"],
                                        ["dg_F3_Loaded", "dg_F3_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F23_Tensiometer_LoadCell_Force_OS_Meas",
                      "dg_F23_Tensiometer_LoadCell_Force_DS_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 1500,
                             "trim_value": [1,1],
                             "start_offset":10,
                             "end_offset":10,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F2"
                           }
                    },
      '''

# COMMAND ----------

 al52_F3 = ''' "al52_F3":{
                    "function": "diff_shift_signal",
                    "additional_signal": {
                                        "add_signal_needed": true,
                                        "add_signal":[ 
                                        ["dg_F4_Loaded", "dg_F4_Loaded","eg_TimeIndex"],
                                        ["dg_F4_Loaded", "dg_F4_Loaded","eg_TimeIndex"]
                                      ]
                                      },
                    "input_signal" : [
                      "dg_F34_Tensiometer_LoadCell_Force_OS_Meas",
                      "dg_F34_Tensiometer_LoadCell_Force_DS_Meas"
                      ],
                   "trimmer": {
                               "trim_needed": false
                              },
                    "args": {
                             "threshold": 1500,
                             "trim_value": [1,1],
                             "start_offset":10,
                             "end_offset":10,
                             "time_shift_ms":20,
                             "threshold_op": ">",
                             "how": "any",
                             "mach_id": "F3"
                           }
                    },
      '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### al53 - Difference of actual and tension reference in interstands

# COMMAND ----------

al53_F1 = '''   "al53_F1":{
                "function": "operation_between_signals",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  ["dg_F12_Tensiometer_SpecificTension_Meas", "dg_F12_Tensiometer_SpecificTension_Ref"]
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                             "offset_needed": true,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 8,
                             "offset_end": -10
                             },
                           "trim_signal": [
                                            "dg_F2_Loaded"
                                            ]
                          },
                "args": {
                         "threshold": 5,
                         "abs": true,
                         "signal_op": "-",
                         "threshold_op": ">",
                         "scale_factor": 1.0,
                         "how": "any",
                         "mach_id": "F1"
                       }
              },
'''

# COMMAND ----------

al53_F2 = '''   "al53_F2":{
                "function": "operation_between_signals",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  ["dg_F23_Tensiometer_SpecificTension_Meas", "dg_F23_Tensiometer_SpecificTension_Ref"]
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                             "offset_needed": true,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 6,
                             "offset_end": -10
                             },
                           "trim_signal": [
                                            "dg_F3_Loaded"
                                            ]
                          },
                "args": {
                         "threshold": 10,
                         "abs": true,
                         "signal_op": "-",
                         "threshold_op": ">",
                         "scale_factor": 1.0,
                         "how": "any",
                         "mach_id": "F2"
                       }
              },
'''

# COMMAND ----------

al53_F3 = '''   "al53_F3":{
                "function": "operation_between_signals",
                "additional_signal": {
                                  "add_signal_needed": false
                                  },
                "input_signal" : [
                  ["dg_F34_Tensiometer_SpecificTension_Meas", "dg_F34_Tensiometer_SpecificTension_Ref"]
                  ],
               "trimmer": {
                           "trim_needed": true,
                           "trim_value": 1,
                           "op": "==",
                           "offset": {
                             "offset_needed": true,
                             "offset_index_signal": "eg_TimeIndex",
                             "offset_start": 6,
                             "offset_end": -10
                             },
                           "trim_signal": [
                                            "dg_F4_Loaded"
                                            ]
                          },
                "args": {
                         "threshold": 10,
                         "abs": true,
                         "signal_op": "-",
                         "threshold_op": ">",
                         "scale_factor": 1.0,
                         "how": "any",
                         "mach_id": "F3"
                       }
              }
'''

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md ######NOTE: The last alarm enabled shall not end with comma after curly brackets (as all the others) => },    
# MAGIC Otherwise it will return ERROR

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount "Json" with strings concatenation

# COMMAND ----------

alarms = '''"alarms":{ \n''' + al21a_F1+ al21a_F2 + al21a_F3 + al21a_F4 + \
                               al21b_F1+ al21b_F2 + al21b_F3 + al21b_F4 + \
                               al22a_F1+ al22a_F2 + al22a_F3 + al22a_F4 + \
                               al22b_F1+ al22b_F2 + al22b_F3 + al22b_F4 + \
                               al22c_F1+ al22c_F2 + al22c_F3 + al22c_F4 + \
                               al22d_F1 + al22d_F2 + al22d_F3 + al22d_F4 + \
                               al22e_F1 + al22e_F2 + al22e_F3 + al22e_F4 + \
                               al22f_F1 + al22f_F2 + al22f_F3 + al22f_F4 + \
                               al23a_F1+ al23a_F2 + al23a_F3 + al23a_F4 + \
                               al23b_F1 + al23b_F2 + al23b_F3 + al23b_F4 +  \
                               al24_F1 + al24_F2 + al24_F3 + al24_F4 + \
                               al25_F1 + al25_F2 + al25_F3 + al25_F4 + \
                               al26_F1 + al26_F2 + al26_F3 + al26_F4 + \
                               al28a_F1 + al28a_F2 + al28a_F3 + al28a_F4 + \
                               al28b_F1 + al28b_F2 + al28b_F3 + al28b_F4 + \
                               al30a_F1 + al30a_F2 + al30a_F3 + al30a_F4 + \
                               al30b_F1 + al30b_F2 + al30b_F3 + al30b_F4 + \
                               al35_F1 + al35_F2 + al35_F3 + al35_F4 + \
                               al43_F1 + al43_F2 + al43_F3 + al43_F4 +  \
                               al51a_F1 + al51a_F2 + al51a_F3 + \
                               al52_F1 + al52_F2 + al52_F3 + \
                               al53_F1 + al53_F2 + al53_F3 + "}\n"

# COMMAND ----------

#al19_F1 + al19_F2 + al19_F3 + al19_F4 + \

#alarms = '''"alarms":{ \n''' + al23a_F1+ al23a_F2 + al23a_F3 + al23a_F4 + "}\n"

# COMMAND ----------

rules = "{" + head + alarms + "}"
print(rules)

# COMMAND ----------



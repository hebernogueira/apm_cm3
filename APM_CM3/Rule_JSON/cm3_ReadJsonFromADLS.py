# Databricks notebook source
# import libraries
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd

# COMMAND ----------

# MAGIC %run
# MAGIC /config/secret_gen2

# COMMAND ----------

#define details of store and container, change name to sandbox for container if doing some development work
container = 'sandbox'
store_name = 'gdaaproddls'
#define base path to access data lake
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

#intializing file system 
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://"+container+"@"+store_name+".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")


#create file system client to access the files from gen2
#define service client 
try:  
    global service_client        
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", store_name), credential=key)    
except Exception as e:
    print(e)

file_system_client = service_client.get_file_system_client(file_system=container)

path = 'heber_nogueira/cm3_cpm/json files/gc_rules/'
#path = 'heber_nogueira/PlotAPI/'
#path = 'landing/pinda/use_cases/ballTOR/ball/'
paths = file_system_client.get_paths(path)
input_path = [p.name for p in paths]
input_path

# COMMAND ----------

alarms = {}
rootpath='/dbfs/mnt/sandbox/'
for file in input_path[1:]:
    print(file)
    ##--- commented code retrieves error with DS_DS_General cluster
    #directory_client = file_system_client.get_directory_client("/")
    #file_client = directory_client.get_file_client(file)
    #download = file_client.download_file()
    #downloaded_bytes = download.readall()
    #json_as_dict = pd.read_json(downloaded_bytes,keep_default_dates=False).to_dict()
    json_as_dict = pd.read_json(rootpath+file,keep_default_dates=False).to_dict()
    alarms.update(json_as_dict)
 
# Create rules dictionary
rules = {}
rules['alarms']=alarms

# COMMAND ----------



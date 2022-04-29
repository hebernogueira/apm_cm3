# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook is to show how to connect to gen-2 data lake storage and access data, accessing HFM-Material-Time data.

# COMMAND ----------

# DBTITLE 1,Connecting to gen2
# MAGIC %run
# MAGIC /config/secret_gen2

# COMMAND ----------

# import libraries
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime
from datetime import timedelta
from pyspark.sql.types import *
import pytz
 

# COMMAND ----------

# DBTITLE 1,Adding store name and container name from which data needs to be read
#define details of store and container, change name to sandbox for container if doing some development work
coilid             = 'cg_gen_coilid'
alloy              = 'cg_gen_alloy'

#store_name = "novelisadlsg2"
store_name = "gdaaproddls"
container = "plantdata"
read_table =  "opsentprodg2.pba_cm3_time"


#intializing file system 
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://"+container+"@"+store_name+".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

DB = 'daaentdevg2'
table_coil_status = DB+'.'+'pba_cpm_cm3_queue'
table_status_table = DB+'.'+'pba_cpm_cm3_test'

VERSION_FILE = "dbfs:/FileStore/shared_uploads/heber.nogueira@novelis.com/apm_cm3/last_version.json"
VERSION_FILE2= "dbfs:/mnt/sandbox/heber_nogueira/cm3_cpm/table_version/last_version.json"

# COMMAND ----------

#("dbfs:/mnt/landing/pinda/iba_archive/test_thickheber_nogueira/cm3_cpm/table_version/

# COMMAND ----------

#read the last version from the json file
last_version = spark.read.json(VERSION_FILE).first()['version']
#last_version = 82750
print(last_version)

# COMMAND ----------

version_read = spark.sql("""DESCRIBE HISTORY {}""".format(read_table))#.select('version').first()[0]
#display(version_read)

# COMMAND ----------

#last_version=19280
# get today's date in EST timezone
today = datetime.now(pytz.timezone('Brazil/East'))

# create partition by coilid
w1 = Window.partitionBy(coilid)

# Steps
## read data from the delta table
## select coilid and time column
## find the min of the time column for each coilid, then convert the format and then store it to Production date
## select coilid and production date
## select unique combination of coilid and production date
## rename the coilid to Coil_ID
## add three columns Status, Comment and Timestamp columns
coil_status = spark.read \
                  .format("delta") \
                  .option("readChangeData", True) \
                  .option("startingVersion", last_version) \
                  .table(read_table) \
                  .filter(col('_change_type')=='insert') \
                  .select(coilid, 'time') \
                  .withColumn('Production_date', date_format(min('time').over(w1), 'MM/dd/yyyy HH:mm:ss')) \
                  .withColumn('Production_date',to_timestamp(col('Production_date'),"MM/dd/yyyy HH:mm:ss"))\
                  .select(coilid, 'Production_date') \
                  .distinct() \
                  .withColumnRenamed(coilid, 'Coil_ID') \
                  .withColumn("Status", lit('pending')) \
                  .withColumn("Comment",lit('auto')) \
                  .withColumn("Timestamp", lit(today.strftime('%m/%d/%Y %H:%M:%S')))

display(coil_status)

# COMMAND ----------

#dfq =  spark.sql('''SELECT * FROM daaentdevg2.pba_cpm_cm3_queue''')
#dfq = dfq.withColumnRenamed("Queue_Status", "Status") \
#          .withColumn('Production_date',to_timestamp(col('Production_Date'),"MM/dd/yyyy HH:mm:ss"))\
#          .withColumn("Comment",lit('auto')) \
#          .withColumn("Timestamp", lit(today.strftime('%m/%d/%Y %H:%M:%S')))\
#          .orderBy('Production_date',ascending=False)
#                  
#display(dfq)

#dfq.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(table_coil_status)

# COMMAND ----------

from delta.tables import *
# location of the table table_coil_status: 'daaentdevg2.osw_hfm_coil_status_test'
deltaTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/daaentdevg2.db/pba_cpm_cm3_queue")
deltaTable.alias("old").merge(
    coil_status.alias("new"),
    "old.Coil_ID = new.Coil_ID") \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

# DBTITLE 1,Code to access all the files present at a path (similar to os.walk)
# Manage data path according to the actual date
today = datetime.now(pytz.timezone('Brazil/East'))
yesterday = today - timedelta(days=1)
days_ago = today - timedelta(days=5)

#Select info from today and yesterday
DD, MM, YYYY, hh, mm,ss = today.day, today.month, today.year, today.hour, today.minute,today.second
yest_DD, yest_MM, yest_YYYY = yesterday.day, yesterday.month, yesterday.year


##Considering cluster timezone
##First day of month before 04:00 keep as yesterday
if(DD== 1) and (hh<4):
  year = str(yest_YYYY)
  month = str(yest_MM).zfill(2)
  day = str(yest_DD).zfill(2)
else:
  # Afterwards day 01/mm/yyyy 04:00:00 keep actual year and month
  year = str(YYYY)
  month =str(MM).zfill(2)
  day = str(DD).zfill(2)
#

select_date = "'{}-{}-{}'".format(str(days_ago.year), str(days_ago.month).zfill(2),str(days_ago.day).zfill(2))
select_date

# COMMAND ----------

# find out the last version from the delta table
version_read = spark.sql("""DESCRIBE HISTORY {}""".format(read_table)).select('version').first()[0]
#version_read = 81617
# print the version
print(version_read)

from datetime import datetime
import pytz
today_ts = datetime.now(pytz.timezone('Brazil/East')).strftime('%Y-%m-%d %H:%M:%S')

# create the dictionary to store the last version as a column version
version_dict= {"version": version_read, "timestamp": today_ts }


# delete existing file
try:
  dbutils.fs.rm(VERSION_FILE)
  dbutils.fs.rm(VERSION_FILE2)
except:
  pass

# write the last version read to the json file
dbutils.fs.put(VERSION_FILE, """{}""".format(version_dict), True)
dbutils.fs.put(VERSION_FILE2, """{}""".format(version_dict), True)

# COMMAND ----------

# DBTITLE 1,Show Coil Process Status
#sel_query = '''SELECT * FROM '''+ table_coil_status+ ''' WHERE TO_DATE(Production_date,"MM/dd/yyyy HH:mm:ss") >= cast({} as DATE)'''.format(select_date)
sel_query = '''SELECT * FROM '''+ table_coil_status+ \
            ''' WHERE Production_date >=cast({} as DATE)'''.format(select_date) +\
            ''' ORDER BY Production_date DESC LIMIT(10)'''
print(sel_query)
display(spark.sql(sel_query))

# COMMAND ----------

# DBTITLE 1,BATCH MODE
def run_with_retry(notebook, timeout, args = {}, max_retries = 3):
  num_retries = 0
  while True:
    try:
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if num_retries >= max_retries:
        print('Maximum number of retries reached')
        break
      else:
        print("ERROR! Retrying coil: ")#,coil[0])
        num_retries += 1	

# Query to define coils needs to be processed
sel_query = '''SELECT Coil_ID, Production_date FROM '''+ table_coil_status+ ''' 
               WHERE Status = 'pending' 
               AND Production_date >= cast({} as DATE)'''.format(select_date) 
coils_to_process = spark.sql(sel_query)
print(sel_query,'\n')

# Number of coils in the batch processing
max_coils_to_process = 5

# Extract list of coil ids and its production_dates
coil_ids = [x[0] for x in coils_to_process.select('Coil_ID').take(max_coils_to_process) if x is not None]
production_dates = [x[0].strftime("%m/%d/%Y %H:%M:%S") for x in coils_to_process.select('Production_date').take(max_coils_to_process) if x is not None]

# run only if list is not empty. Otherwise will give an error
if coil_ids:
  coil_ids_str = str([coil_ids])[2:-2]
  print(coil_ids_str)
  spark.sql("UPDATE {} SET Status = 'processing' Where Status='pending' and Coil_ID IN ({})".format(table_coil_status, coil_ids_str))
  
  # RUN RULE ENGINE
  run_with_retry("cm3_alarm_batch_gen", 600, {"coil_id": ",".join(coil_ids), "production_date": ",".join(production_dates)}, max_retries=1)
  
  # RUN AD
else:
  print('List of coils is empty. There are 0 new coils to analyze. ')



# COMMAND ----------

dbutils.notebook.exit("Finished!")

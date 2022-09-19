# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion and Data Processing made easy!

# COMMAND ----------

display_slide('1vT0TaSyAq7LjR_NiuI2n9Ow8lJ6upN6ciJpZ8mXFZZI',2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Let's look at the raw incoming data

# COMMAND ----------

def display_slide(slide_id, slide_number):
  displayHTML(f'''
  <div style="width:2000px; margin:auto; align:left">
  <iframe
    src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}"
    frameborder="0"
    width="1150"
    height="683"
  ></iframe></div>
  ''')

# COMMAND ----------

dataPath = f"{bucket_name}/turbine/incoming-data-json"
display(dbutils.fs.ls(dataPath))

# COMMAND ----------

df = spark.read.json(dataPath)
df.createOrReplaceTempView("incoming_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC --TODO : Select and display the entire incoming json data using a simple SQL SELECT query
# MAGIC select * from incoming_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Bronze layer: Ingest data stream 
# MAGIC ### Autoloader!

# COMMAND ----------

# DBTITLE 1,Stream landing files from cloud storage
#                                  Autoloader
#                     (Use cloudFiles to initiate Autoloader)
#                                      |
#                                      |
bronzeDF = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "json")\
            .option("cloudFiles.schemaLocation", f"{path}\schema")\
            .option("cloudFiles.maxFilesPerTrigger", 2)\
            .option("mergeSchema", "true").load(dataPath)

# Write Stream as Delta Table
bronzeDF.writeStream.format("delta").outputMode("append").trigger(once=True).option('checkpointLocation', path+"/bronze_checkpoint").toTable("turbine_bronze")

# COMMAND ----------

# DBTITLE 1,Our raw data is now available in a Delta table
# MAGIC %sql
# MAGIC select * from turbine_bronze;

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE turbine_bronze SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Silver layer: cleanup data and remove unecessary column
# MAGIC 
# MAGIC Delta tables themselves can act as Streaming sources!

# COMMAND ----------

#Cleanup the silver table
#Our bronze silver should have TORQUE with mostly NULL value and the _rescued column should be empty.
#drop the TORQUE column, filter on _rescued to select only the rows without json error from the autoloader, filter on ID not null as you'll need it for your join later

silverDF = spark.readStream.table('turbine_bronze').drop('TORQUE').filter('_rescued_data is null').filter('id is not null')

#Write it back to your "turbine_silver" table
silverDF.writeStream.format("delta").outputMode("append").trigger(once=True).option('checkpointLocation', path+"/silver_checkpoint").toTable("turbine_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from turbine_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Gold layer: join information on Turbine status to add a label to our dataset

# COMMAND ----------

spark.read.format("parquet").load(f"{bucket_name}/turbine/status").display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists turbine_status;
# MAGIC create or replace table turbine_status (ID int, STATUS string)

# COMMAND ----------

# --Save the status data as our turbine_status table
# --Use databricks COPY INTO COMMAND https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-copy-into.html
spark.sql(f"""COPY INTO turbine_status FROM "{bucket_name}/turbine/status" FILEFORMAT = PARQUET""").show()

# COMMAND ----------

# DBTITLE 1,Join data with turbine status (Damaged or Healthy)
turbine_stream = spark.readStream.table('turbine_silver')
turbine_status = spark.read.table("turbine_status")

#TODO: do a left join between turbine_stream and turbine_status on the 'id' key and save back the result as the "turbine_gold" table
turbine_stream.join(turbine_status, turbine_stream.ID == turbine_status.ID, how="left").drop(turbine_status.ID).writeStream.format("delta").outputMode("append").trigger(once=True).option('checkpointLocation', path+"/gold_checkpoint").toTable("turbine_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### We have Gold!

# COMMAND ----------

# MAGIC %sql
# MAGIC --Our turbine gold table should be up and running!
# MAGIC select TIMESTAMP, AN3, SPEED, status from turbine_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC # Unique Delta Features

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Run DELETE/UPDATE/MERGE with DELTA ! 
# MAGIC We just realized that something is wrong in the data before 2020! Let's DELETE all this data from our gold table as we don't want to have wrong value in our dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE FROM turbine_gold where timestamp < '2020-00-01';
# MAGIC select count(*) from turbine_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM turbine_gold where timestamp < '2020-07-01';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history turbine_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE turbine_gold TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant Access to Database
# MAGIC If on a Table-ACLs enabled High-Concurrency Cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GRANT SELECT ON DATABASE turbine_demo TO `amitoz.sidhu@databricks.com`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Don't forget to Cancel all the streams once your demo is over

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# Databricks notebook source
# MAGIC %sh
# MAGIC ls /dbfs/FileStore/shared_uploads/workshop_demo

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/FileStore/shared_uploads/workshop_demo
# MAGIC unzip drive_download_20220918T153434Z_001.zip -d unzipped_workshop_data

# COMMAND ----------

unzipped_workshop_data = 'dbfs:/FileStore/shared_uploads/workshop_demo/unzipped_workshop_data'
formatted_workshop_data = 'dbfs:/FileStore/shared_uploads/workshop_demo/formatted_workshop_data'

# COMMAND ----------

#convert incoming data back to json files

incomingJson = spark.read.format('csv').option('header','true').option('inferSchema','true').load(f'{unzipped_workshop_data}/incoming_data_json.csv')
incomingJson.repartition(500).write.mode('overwrite').json(f'{formatted_workshop_data}/turbine/incoming-data-json')

# COMMAND ----------

turbineStatus = spark.read.format('csv').option('header','true').option('inferSchema','true').load(f'{unzipped_workshop_data}/turbine_status_parquet.csv')
turbineStatus.write.mode('overwrite').parquet(f'{formatted_workshop_data}/turbine/status')

# COMMAND ----------

turbineB = spark.read.format('csv').option('header','true').option('inferSchema','true').load(f'{unzipped_workshop_data}/turbine_bronze_delta.csv')
turbineB.write.mode('overwrite').format('delta').save(f'{formatted_workshop_data}/turbine/bronze/data')

# COMMAND ----------

turbineS = spark.read.format('csv').option('header','true').option('inferSchema','true').load(f'{unzipped_workshop_data}/turbine_silver_delta.csv')
turbineS.write.mode('overwrite').format('delta').save(f'{formatted_workshop_data}/turbine/silver/data')

# COMMAND ----------

turbineG = spark.read.format('csv').option('header','true').option('inferSchema','true').load(f'{unzipped_workshop_data}/turbine_gold_delta.csv')
turbineG.write.mode('overwrite').format('delta').save(f'{formatted_workshop_data}/turbine/gold/data')

# COMMAND ----------

turbineGML = spark.read.format('csv').option('header','true').option('inferSchema','true').load(f'{unzipped_workshop_data}/turbine_gold_for_ml_delta.csv')
turbineGML.write.mode('overwrite').format('delta').save(f'{formatted_workshop_data}/turbine/gold-data-for-ml')

# COMMAND ----------

turbinePP = spark.read.format('csv').option('header','true').option('inferSchema','true').load(f'{unzipped_workshop_data}/turbine_power_prediction_delta.csv')
turbinePP.write.mode('overwrite').format('delta').save(f'{formatted_workshop_data}/turbine/power/prediction/data')

# COMMAND ----------

turbinePBJ = spark.read.format("csv").option("header", "true").option('inferSchema','true').load(f"{unzipped_workshop_data}/turbine_power_bronze_json.csv")
turbinePBJ.write.mode('overwrite').format('json').save(f'{formatted_workshop_data}/turbine/power/raw')

# COMMAND ----------

turbinePB = spark.read.format('csv').option('header','true').option('inferSchema','true').load(f'{unzipped_workshop_data}/turbine_power_bronze_delta.csv')
turbinePB.write.mode('overwrite').format('delta').save(f'{formatted_workshop_data}/turbine/power/bronze/data')

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/shared_uploads/workshop_demo/unzipped_workshop_data

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/shared_uploads/workshop_demo/formatted_workshop_data/turbine

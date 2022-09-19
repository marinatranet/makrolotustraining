-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Introducing Delta Live Table
-- MAGIC ### A simple way to build and manage data pipelines for fresh, high quality data!

-- COMMAND ----------

-- DBTITLE 0,Delta Live Tables - Managed Service for ETL 
-- MAGIC %python 
-- MAGIC display_slide('1vT0TaSyAq7LjR_NiuI2n9Ow8lJ6upN6ciJpZ8mXFZZI',5)

-- COMMAND ----------

-- # %python
-- #TODO
-- #In python or SQL, rewrite the transformations from notebook 01-Megacorp-data-ingestion using DLT, and building the DLT graph
-- #Add some expectations
-- #go in Job => DLT, create your DLT pointing to your notebook
-- #help: https://docs.databricks.com/data-engineering/delta-live-tables/index.html
-- #Remember: you can use the retail DLT as example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Ingest Raw data into Bronze Layer 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display_slide('1vT0TaSyAq7LjR_NiuI2n9Ow8lJ6upN6ciJpZ8mXFZZI',4)

-- COMMAND ----------

create incremental live table turbine_bronze_dlt(
  constraint correct_schema expect (_rescued_data IS NULL)
)
comment "raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
as select * from cloud_files("dbfs:/FileStore/shared_uploads/workshop_demo/formatted_workshop_data/turbine/incoming-data-json", "json")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Cleanse and Filter data and load the Silver Layer

-- COMMAND ----------

create incremental live table turbine_silver_dlt(
  constraint valid_id expect (id is not null)
)
comment "cleanup incoming turbine data and drop unnecessary columns"
as select 
  an10,
  an3,
  an4,
  an5,
  an6,
  an7,
  an8,
  an9,
  cast(id as int) as id, 
  speed,
  timestamp,
  _rescued_data
from stream(live.turbine_bronze_dlt)
where id is not null 
and _rescued_data is null 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Enrich data and load the Gold Layer - ready for Analytics

-- COMMAND ----------

create incremental live table turbine_status_dlt (id int, status string)
as select id, status from cloud_files("dbfs:/FileStore/shared_uploads/workshop_demo/formatted_workshop_data/turbine/status", "parquet", map("schema", "id int, status string"))

-- COMMAND ----------

create incremental live table turbine_gold_dlt(
  constraint valid_id expect (status is not null) on violation drop row 
)
comment "Final table with all the sensor data from Turbines for Analysis/ML"
as 
select sd.*, s.status
from stream(live.turbine_silver_dlt) sd
  left join live.turbine_status_dlt s
  on sd.id = s.id 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Checking your data quality metrics with Delta Live Table
-- MAGIC Delta Live Tables tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. This let you build the following dashboards:
-- MAGIC 
-- MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">

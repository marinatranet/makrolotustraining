# Databricks notebook source
# MAGIC %md #Turbine 

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists demo_turbine_workshop;
# MAGIC CREATE TABLE if not exists demo_turbine_workshop.turbine_bronze USING delta LOCATION 'dbfs:/FileStore/shared_uploads/workshop_demo/formatted_workshop_data/turbine/bronze/data';
# MAGIC CREATE TABLE if not exists demo_turbine_workshop.turbine_silver USING delta LOCATION 'dbfs:/FileStore/shared_uploads/workshop_demo/formatted_workshop_data/turbine/silver/data';
# MAGIC CREATE TABLE if not exists demo_turbine_workshop.turbine_gold   USING delta LOCATION 'dbfs:/FileStore/shared_uploads/workshop_demo/formatted_workshop_data/turbine/gold/data' ;
# MAGIC 
# MAGIC CREATE TABLE if not exists demo_turbine_workshop.turbine_power_prediction USING delta 
# MAGIC LOCATION 'dbfs:/FileStore/shared_uploads/workshop_demo/formatted_workshop_data/turbine/power/prediction/data';
# MAGIC 
# MAGIC CREATE TABLE if not exists demo_turbine_workshop.turbine_power_bronze USING delta LOCATION 'dbfs:/FileStore/shared_uploads/workshop_demo/formatted_workshop_data/turbine/power/bronze/data';

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from demo_turbine_workshop.turbine_gold

# COMMAND ----------

# MAGIC %sql 
# MAGIC select date(TIMESTAMP), avg(speed)
# MAGIC from demo_turbine_workshop.turbine_gold
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from demo_turbine_workshop.turbine_power_prediction

# COMMAND ----------

# MAGIC %md 
# MAGIC Looking for inspiration ? Check this:
# MAGIC 
# MAGIC 
# MAGIC ![turbine-demo-dashboard](https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/turbine-demo-dashboard1.png)

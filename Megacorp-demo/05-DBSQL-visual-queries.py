# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./resources/00-setup $reset_all=$reset_all_data

# COMMAND ----------

# MAGIC %sql with forecast_acc as (
# MAGIC   select
# MAGIC     status,
# MAGIC     status_forecast,
# MAGIC     case
# MAGIC       when status_forecast = '1.0' then 'damaged'
# MAGIC       else 'healthy'
# MAGIC     end as forecast_string
# MAGIC   from
# MAGIC     turbine_ml_predictions
# MAGIC )
# MAGIC select
# MAGIC   (
# MAGIC     count(
# MAGIC       case
# MAGIC         when status = forecast_string then 1
# MAGIC         else Null
# MAGIC       end
# MAGIC     ) / count(*)
# MAGIC   ) * 100 as accuracy
# MAGIC from
# MAGIC   forecast_acc

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   4130 as max_potential,
# MAGIC   to_date(date) date,
# MAGIC   sum(power) as power
# MAGIC from
# MAGIC   demo_turbine_workshop.turbine_power_bronze
# MAGIC group by
# MAGIC   date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select direction, sum(power) as power_sum, sum(wind_speed) wind_speed_sum, avg(wind_speed) wind_speed_avg from (
# MAGIC     select 
# MAGIC     CASE
# MAGIC         WHEN wind_direction >=349 and wind_direction < 011 THEN 'N'
# MAGIC         WHEN wind_direction >=012 and wind_direction < 033 THEN 'NNE'
# MAGIC         WHEN wind_direction >=034 and wind_direction < 056 THEN 'NE'
# MAGIC         WHEN wind_direction >=057 and wind_direction < 078 THEN 'ENE'
# MAGIC         WHEN wind_direction >=079 and wind_direction < 101 THEN 'E'
# MAGIC         WHEN wind_direction >=102 and wind_direction < 123 THEN 'ESE'
# MAGIC         WHEN wind_direction >=124 and wind_direction < 146 THEN 'SE'
# MAGIC         WHEN wind_direction >=147 and wind_direction < 168 THEN 'SSE'
# MAGIC         WHEN wind_direction >=169 and wind_direction < 191 THEN 'S'
# MAGIC         WHEN wind_direction >=192 and wind_direction < 213 THEN 'SSW'
# MAGIC         WHEN wind_direction >=214 and wind_direction < 236 THEN 'SW'
# MAGIC         WHEN wind_direction >=237 and wind_direction < 258 THEN 'WSW'
# MAGIC         WHEN wind_direction >=259 and wind_direction < 281 THEN 'W'
# MAGIC         WHEN wind_direction >=282 and wind_direction < 303 THEN 'WNW'
# MAGIC         WHEN wind_direction >=304 and wind_direction < 326 THEN 'NW'
# MAGIC         WHEN wind_direction >=327 and wind_direction < 348 THEN 'NNW'
# MAGIC         ELSE 'S'
# MAGIC     END as direction, wind_speed, power from demo_turbine_workshop.turbine_power_bronze)
# MAGIC     group by direction;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   count(distinct id) as c,
# MAGIC   status,
# MAGIC   387 as instant_energy,
# MAGIC   387.0 / 501.0 as instant_energy_per_turbine,
# MAGIC   3.2 as efficiency_impact,
# MAGIC   502 as total
# MAGIC from
# MAGIC   demo_turbine_workshop.turbine_gold
# MAGIC group by
# MAGIC   status

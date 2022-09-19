# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./resources/00-setup $reset_all=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### For the Demo, we would be analyzing sensor data to train a ML model and deploy it for ongoing scoring
# MAGIC 
# MAGIC - **Data**: 502 turbines across 12 parameters
# MAGIC - **Analytics Approach**: Supervised; multi-classification
# MAGIC - **Hyper-params**: 3 (bins, iters, depths)
# MAGIC - **Model eval**: precision, f1, accuracy
# MAGIC 
# MAGIC #### Typical steps in an end-to-end ML lifecycle:
# MAGIC 1. Data Exploration
# MAGIC 2. Model Training & Experiment Tracking
# MAGIC 4. Select best model and save to Registry
# MAGIC 5. Inference
# MAGIC 6. Monitoring

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 1: Data Exploration & Visualization

# COMMAND ----------

gold_turbine_df = spark.read.table("turbine_gold_for_ml")
display(gold_turbine_df)

# COMMAND ----------

gold_turbine_df = spark.read.table("turbine_gold_for_ml")
display(gold_turbine_df)

# COMMAND ----------

# DBTITLE 1,Visualize Feature Distributions using open source python libraries
gold_turbine_dfp = spark.read.table("turbine_gold_for_ml").toPandas()
g = sn.PairGrid(gold_turbine_dfp[['AN3','SPEED','status']], diag_sharey=False)

g.map_lower(sn.kdeplot)
g.map_diag(sn.kdeplot, lw=3)
g.map_upper(sn.regplot)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 2: Train Model and Track Experiments

# COMMAND ----------

#once the data is ready, we can train a model
import mlflow
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.mllib.evaluation import MulticlassMetrics

mlflow.spark.autolog()

with mlflow.start_run() as run:
  
  training, test = gold_turbine_df.limit(1000).randomSplit([0.9, 0.1], seed = 5)

  gbt = GBTClassifier(labelCol="label", featuresCol="features").setMaxIter(5)
  grid = ParamGridBuilder().addGrid(gbt.maxDepth, [3,4,5,10,15,25,30]).build()
  metrics = MulticlassClassificationEvaluator(metricName="f1")
  cv = CrossValidator(estimator=gbt, estimatorParamMaps=grid, evaluator=metrics, numFolds=2)

  featureCols = ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10"]
  stages = [VectorAssembler(inputCols=featureCols, outputCol="va"), StandardScaler(inputCol="va", outputCol="features"), StringIndexer(inputCol="status", outputCol="label"), cv]
  pipeline = Pipeline(stages=stages)

  pipelineTrained = pipeline.fit(training)
  predictions = pipelineTrained.transform(test)

  metrics = MulticlassMetrics(predictions.select(['prediction', 'label']).rdd)

  mlflow.log_metric("precision", metrics.precision(1.0))
  mlflow.log_metric("accuracy", metrics.accuracy)
  mlflow.log_metric("f1", metrics.fMeasure(0.0, 2.0))
  mlflow.spark.log_model(pipelineTrained, artifact_path = "gbt_classifier")
  mlflow.set_tag("model", "turbine_gbt")
run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 3: Save to the model registry
# MAGIC Get the model having the best metrics.AUROC from the registry

# COMMAND ----------

#get the best model from the registry
best_model = mlflow.search_runs(filter_string='tags.model="turbine_gbt" and attributes.status = "FINISHED" and metrics.f1 > 0.8', max_results=3).iloc[0]
model_registered = mlflow.register_model("runs:/"+best_model.run_id+"/gbt_classifier", "turbine_health_gbt")

# COMMAND ----------

# DBTITLE 1,Flag version as staging/production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "turbine_health_gbt", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 4: Deploying & using our model in production
# MAGIC 
# MAGIC Now that our model is in our MLFlow registry, we can start to use it in a production pipeline.

# COMMAND ----------

get_status_udf = mlflow.pyfunc.spark_udf(spark, "models:/turbine_health_gbt/production", "string")
spark.udf.register("get_turbine_status", get_status_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, get_turbine_status(struct(AN3, AN4, AN5, AN6, AN7, AN8, AN9, AN10)) as status_forecast from turbine_gold_for_ml

# COMMAND ----------

_sqldf.write.format("delta").mode("overwrite").saveAsTable("turbine_ml_predictions")

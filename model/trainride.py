import mlflow
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("RideHailingDemandPrediction") \
    .getOrCreate()


df=spark.read.csv("../data/ride_data.csv", header=True, inferSchema=True)

feature_cols = ["distance_km", "driver_speed"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df = assembler.transform(df)

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("RideHailingDemandPrediction")

with mlflow.start_run():
    rf = RandomForestRegressor(featuresCol="features", labelCol="ride_duration")
    rf_model = rf.fit(train_df)
    mlflow.spark.log_model(rf_model, "RideHailingModel")

    predictions = rf_model.transform(test_df)

    evaluator = RegressionEvaluator(labelCol="ride_duration", predictionCol="prediction", metricName="mae")
    mae = evaluator.evaluate(predictions)
    mlflow.log_metric("mae", mae)

    rf_model.save("RideHailingModel")

spark.stop()

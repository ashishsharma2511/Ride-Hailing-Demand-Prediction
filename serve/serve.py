from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from feast import FeatureStore
from pyspark.sql import SparkSession
import mlflow.pyfunc
import pandas as pd

# Initialize Spark
spark = SparkSession.builder.appName("RideHailingAPI").getOrCreate()

# Initialize Feast
store = FeatureStore(repo_path="../model_shark/feature_repo")

# Load MLflow Spark model
mlflow.set_tracking_uri("http://127.0.0.1:5000")
model =  mlflow.spark.load_model("runs:/0b76e545c4b64041a2c3194f05136cb0/RideHailingModel")

app = FastAPI(title="Ride Duration Prediction API")

# Request body
class RideRequest(BaseModel):
    ride_id: int

@app.post("/predict")
def predict_ride_duration(request: RideRequest):
    try:
        # 1. Fetch features from Feast online store
        features = store.get_online_features(
            features=[
                "ride_features:distance_km",
                "ride_features:driver_speed"
            ],
            entity_rows=[{"ride_id": request.ride_id}]
        ).to_dict()

        # 2. Check if features exist
        if features["distance_km"][0] is None or features["driver_speed"][0] is None:
            raise HTTPException(status_code=404, detail=f"Features not found for ride_id {request.ride_id}")

        # 3. Create Spark DataFrame directly
        spark_df = spark.createDataFrame([
            {
                "distance_km": features["distance_km"][0],
                "driver_speed": features["driver_speed"][0]
            }
        ])

        # 4. Predict using Spark ML model
        prediction_df = model.transform(spark_df)

        # 5. Extract scalar from Spark DataFrame
        predicted_duration = float(prediction_df.collect()[0]["prediction"])

        return {"ride_id": request.ride_id, "predicted_duration": predicted_duration}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
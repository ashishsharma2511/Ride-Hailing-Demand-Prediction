from fastapi import FastAPI
from pydantic import BaseModel
from feast import FeatureStore
import mlflow.pyfunc

# Load Feast store
store = FeatureStore(repo_path="../model_shark/feature_repo")

# Load MLflow model
# model = mlflow.pyfunc.load_model("models/ride_model")
mlflow.set_tracking_uri("http://127.0.0.1:5000")
model= mlflow.pyfunc.load_model("runs:/35c82da50f8d4b02897cfc86ba1bca40/RideHailingModel")
app = FastAPI(title="Ride Duration Prediction")

# Request body
class RideRequest(BaseModel):
    ride_id: int

@app.post("/predict")
def predict_ride_duration(request: RideRequest):
    # 1. Fetch features from Redis (online store)
    features = store.get_online_features(
        features=[
            "ride_features:distance_km",
            "ride_features:driver_speed"
        ],
        entity_rows=[{"ride_id": request.ride_id}]
    ).to_dict()

    # 2. Convert features dict to model input
    # Assuming the model expects pandas DataFrame
    import pandas as pd
    df = pd.DataFrame({
        "distance_km": [features["distance_km"][0]],
        "driver_speed": [features["driver_speed"][0]]
    })

    # 3. Predict
    prediction = model.predict(df)[0]

    # 4. Return result
    return {"ride_id": request.ride_id, "predicted_duration": float(prediction)}
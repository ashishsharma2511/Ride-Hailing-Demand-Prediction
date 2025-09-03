from feast import Entity, FeatureView, Field
from feast.types import Int64, Float32
from feast import FileSource

ride_entity = Entity(name="ride_id", join_keys=["ride_id"])

# 2. Point to your raw CSV
rides_source = FileSource(
    path="../data/ride_data.csv",
    timestamp_field="event_timestamp",   
    created_timestamp_column="created_ts"
)

# 3. Define features
ride_feature_view = FeatureView(
    name="ride_features",
    entities=[ride_entity],
    ttl=None,
    schema=[
        Field(name="distance_km", dtype=Float32),
        Field(name="driver_speed", dtype=Float32),
        Field(name="ride_duration", dtype=Float32),  # target
    ],
    online=True,
    source=rides_source,
)
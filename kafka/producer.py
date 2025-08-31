from kafka import KafkaProducer
import json
import time
import random   

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


status = ["requested","accepted","completed","cancelled"]
payment_methods = ["credit_card", "debit_card", "paypal", "cash"]
surge_multiplier = round(random.choice([1.0, 1.2, 1.5, 2.0]), 1)

def generate_transaction():
    return {
            
        "ride_id": random.randint(1000, 200000),
        "user_id": random.randint(1000, 2000),
        "driver_id": random.randint(5000, 8000),
        "pickup_lat": random.uniform(-90, 90),
        "pickup_lng":  random.uniform(-180, 180),
        "dropoff_lat": random.uniform(-90, 90),
        "dropoff_lng": random.uniform(-180, 180),
        "request_time": random.randint(0, 23),
        "distance_km": random.randint(1, 100),
        "payment_method": random.choice(payment_methods),
        "surge_multiplier": surge_multiplier,
        "status": random.choice(status)

    }

while True:
    txn = generate_transaction()
    producer.send('ride_requests', txn)
    time.sleep(1)
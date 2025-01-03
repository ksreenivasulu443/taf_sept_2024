import pandas as pd
import numpy as np
from faker import Faker
import random

# Initialize Faker instance
fake = Faker()

# Function to generate test data
def generate_test_data(num_records):
    data = []
    for _ in range(num_records):
        # Generate realistic data
        record = {
            "VendorID": random.choice([1, 2]),
            "tpep_pickup_datetime": fake.date_time_this_year(),
            "tpep_dropoff_datetime": fake.date_time_this_year(),
            "passenger_count": random.randint(1, 6),
            "trip_distance": round(random.uniform(0.5, 30.0), 2),
            "RatecodeID": random.choice([1, 2, 3, 4, 5, 6]),
            "store_and_fwd_flag": random.choice(["Y", "N"]),
            "PULocationID": random.randint(1, 265),
            "DOLocationID": random.randint(1, 265),
            "payment_type": random.choice([1, 2, 3, 4]),
            "fare_amount": round(random.uniform(3.0, 500.0), 2),
            "extra": round(random.uniform(0.0, 10.0), 2),
            "mta_tax": round(random.uniform(0.0, 1.0), 2),
            "tip_amount": round(random.uniform(0.0, 50.0), 2),
            "tolls_amount": round(random.uniform(0.0, 20.0), 2),
            "improvement_surcharge": round(random.uniform(0.0, 1.0), 2),
            "total_amount": 0.0,  # Calculated later
            "congestion_surcharge": round(random.uniform(0.0, 2.5), 2),
            "Airport_fee": round(random.uniform(0.0, 2.5), 2),
        }
        record["total_amount"] = round(
            record["fare_amount"] + record["extra"] + record["mta_tax"] + record["tip_amount"] +
            record["tolls_amount"] + record["improvement_surcharge"] + record["congestion_surcharge"] + record["Airport_fee"], 2
        )
        data.append(record)

    # Introduce negative test data
    for _ in range(int(num_records * 0.1)):  # 10% negative data
        record = {
            "VendorID": random.choice([None, "", -1, 3]),  # Invalid VendorID
            "tpep_pickup_datetime": pd.Timestamp(fake.date_time_this_year()) if random.choice([True, False]) else None,
            "tpep_dropoff_datetime": None,  # Missing dropoff datetime
            "passenger_count": random.choice([0, -1, None]),  # Invalid passenger count
            "trip_distance": random.choice([-5.0, None, float('nan')]),  # Invalid trip distance
            "RatecodeID": random.choice([None, -1, 10]),  # Invalid RatecodeID
            "store_and_fwd_flag": random.choice(["", None, "X"]),  # Invalid flag
            "PULocationID": random.choice([-5, None, float('nan')]),  # Invalid location
            "DOLocationID": random.choice([-5, None, float('nan')]),  # Invalid location
            "payment_type": random.choice([0, -1, None]),  # Invalid payment type
            "fare_amount": random.choice([-100.0, None, float('nan')]),  # Invalid fare amount
            "extra": random.choice([-10.0, None, float('nan')]),  # Invalid extra
            "mta_tax": random.choice([-1.0, None, float('nan')]),  # Invalid MTA tax
            "tip_amount": random.choice([-50.0, None, float('nan')]),  # Invalid tip amount
            "tolls_amount": random.choice([-20.0, None, float('nan')]),  # Invalid tolls amount
            "improvement_surcharge": random.choice([-1.0, None, float('nan')]),  # Invalid surcharge
            "total_amount": random.choice([-200.0, None, float('nan')]),  # Invalid total amount
            "congestion_surcharge": random.choice([-2.5, None, float('nan')]),  # Invalid surcharge
            "Airport_fee": random.choice([-2.5, None, float('nan')]),  # Invalid fee
        }
        data.append(record)

    return pd.DataFrame(data)

# Generate data
data = generate_test_data(25)

# Convert date columns to timestamp format
data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"], errors='coerce')
data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"], errors='coerce')

# Define output filename
year = "2024"
month = "12"
output_filename = f"yellow_tripdata_{year}-{month}.csv"

# Save data to CSV format
data.to_csv(output_filename, index=False)

print(f"Test data generated and saved to {output_filename}")

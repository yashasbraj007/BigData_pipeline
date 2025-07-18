import pandas as pd
import redis
import time

# Wait in case Redis is starting
time.sleep(5)

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Load CSV
csv_file = "C:\\Users\\HARSHITA\\new_project\\data\\E commerce.csv"
df = pd.read_csv(csv_file)

# Batch size
BATCH_SIZE = 1000

print("Starting batch ingestion...")

# Process in batches
for start_idx in range(0, len(df), BATCH_SIZE):
    end_idx = start_idx + BATCH_SIZE
    chunk = df.iloc[start_idx:end_idx]

    pipe = r.pipeline()

    for idx, row in chunk.iterrows():
        key = f"order:{row['Customer_Id']}:{row['Order_Date']}:{idx}"
        data = {
            "Time": str(row["Time"]),
            "Aging": str(row["Aging"]),
            "Gender": str(row["Gender"]),
            "Device_Type": str(row["Device_Type"]),
            "Customer_Login_type": str(row["Customer_Login_type"]),
            "Product_Category": str(row["Product_Category"]),
            "Product": str(row["Product"]),
            "Sales": str(row["Sales"]),
            "Quantity": str(row["Quantity"]),
            "Discount": str(row["Discount"]),
            "Profit": str(row["Profit"]),
            "Shipping_Cost": str(row["Shipping_Cost"]),
            "Order_Priority": str(row["Order_Priority"]),
            "Payment_method": str(row["Payment_method"])
        }
        pipe.hset(key, mapping=data)

    # Execute all commands in this batch
    pipe.execute()
    print(f"Inserted records {start_idx + 1} to {end_idx} into Redis.")

print("Batch ingestion into Redis completed.")

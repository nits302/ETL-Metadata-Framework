import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

# Global variables
NUM_RECORDS = 45
DIRTY_DATA_RATIO = 0.1
NUM_DIRTY_RECORDS = int(NUM_RECORDS * DIRTY_DATA_RATIO)
DATETIME_START = datetime(2025, 1, 1)
DATETIME_END = datetime.now() - timedelta(days=1)

# Tạo tên file với timestamp
current_time = datetime.now().strftime("%Y%m%d")
OUTPUT_DIR = f"sample_data/raw/{current_time}"

# Tạo thư mục nếu chưa tồn tại
os.makedirs(OUTPUT_DIR, exist_ok=True)

# File paths với timestamp
CUSTOMER_FILE_PATH = f"{OUTPUT_DIR}/customers.json"
ORDER_FILE_PATH = f"{OUTPUT_DIR}/orders.json"


def create_customers(num_records=NUM_RECORDS):
    print(f"Creating {num_records} customer records...")
    customer_ids = [fake.uuid4() for _ in range(num_records)]
    customers = pd.DataFrame(
        {
            "customer_id": customer_ids,
            "name": [fake.name() for _ in range(num_records)],
            "email": [fake.email() for _ in range(num_records)],
            "phone": [fake.phone_number() for _ in range(num_records)],
            "address": [fake.address().replace("\n", ", ") for _ in range(num_records)],
            "created_at": [
                int(
                    fake.date_time_between_dates(
                        datetime_start=DATETIME_START, datetime_end=DATETIME_END
                    ).timestamp()
                )
                for _ in range(num_records)
            ],
        }
    )
    return customers


def create_orders(num_records=NUM_RECORDS, customer_ids=None):
    print(f"Creating {num_records} order records...")
    orders = pd.DataFrame(
        {
            "order_id": [fake.uuid4() for _ in range(num_records)],
            "customer_id": [random.choice(customer_ids) for _ in range(num_records)],
            "product_name": [fake.word() for _ in range(num_records)],
            "quantity": [random.randint(1, 10) for _ in range(num_records)],
            "price": [round(random.uniform(5, 500), 2) for _ in range(num_records)],
            "order_date": [
                int(
                    fake.date_time_between_dates(
                        datetime_start=DATETIME_START, datetime_end=DATETIME_END
                    ).timestamp()
                )
                for _ in range(num_records)
            ],
        }
    )
    return orders


def create_dirty_data_orders(df, num_dirty_records=NUM_DIRTY_RECORDS):
    print(f"Creating dirty data for {num_dirty_records} records...")
    for i in range(num_dirty_records):
        idx = random.randint(0, len(df) - 1)
        # Missing Data
        if random.random() < DIRTY_DATA_RATIO:
            df.at[idx, "product_name"] = None
        if random.random() < DIRTY_DATA_RATIO:
            df.at[idx, "quantity"] = None
        if random.random() < DIRTY_DATA_RATIO:
            df.at[idx, "price"] = None
        # Duplicate Data
        if random.random() < DIRTY_DATA_RATIO:
            df = df._append(df.iloc[idx])
    return df


def save_to_json(df, file_path):
    print(f"Saving data to {file_path}...")
    df.to_json(file_path, orient="records", lines=True)


def main():
    print(f"Generating sample data with timestamp: {current_time}")

    customers = create_customers()
    save_to_json(customers, CUSTOMER_FILE_PATH)

    orders = create_orders(customer_ids=customers["customer_id"].tolist())
    orders = create_dirty_data_orders(
        orders, num_dirty_records=int(NUM_RECORDS * DIRTY_DATA_RATIO)
    )
    save_to_json(orders, ORDER_FILE_PATH)


if __name__ == "__main__":
    main()

import pandas as pd
import os
from glob import glob


def read_parquet_files(directory):
    """Đọc tất cả các file Parquet trong thư mục"""
    # Tìm tất cả file parquet trong thư mục
    parquet_files = glob(os.path.join(directory, "*.parquet"))

    if not parquet_files:
        print(f"Không tìm thấy file Parquet trong thư mục: {directory}")
        return None

    # Đọc và kết hợp tất cả các file
    dfs = []
    for file in parquet_files:
        print(f"Đang đọc file: {os.path.basename(file)}")
        df = pd.read_parquet(file)
        dfs.append(df)

    # Kết hợp tất cả DataFrame
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    return None


def main():
    # Đường dẫn gốc
    base_dir = os.path.dirname(os.path.abspath(__file__))
    date_dir = "20250324"  # Thư mục chứa dữ liệu

    # Đọc dữ liệu customers
    customers_dir = os.path.join(base_dir, date_dir, "customers")
    customers_df = read_parquet_files(customers_dir)
    if customers_df is not None:
        print("\nThông tin dữ liệu Customers:")
        print(customers_df.info())
        print("\n5 dòng đầu tiên của dữ liệu Customers:")
        print(customers_df.head())

    # Đọc dữ liệu orders
    orders_dir = os.path.join(base_dir, date_dir, "orders")
    orders_df = read_parquet_files(orders_dir)
    if orders_df is not None:
        print("\nThông tin dữ liệu Orders:")
        print(orders_df.info())
        print("\n5 dòng đầu tiên của dữ liệu Orders:")
        print(orders_df.head())


if __name__ == "__main__":
    main()

# ETL Metadata Framework - dbt Project

Dự án dbt này quản lý việc chuyển đổi dữ liệu từ bronze đến silver và gold layer, tương ứng với các bước sạch sẽ hóa dữ liệu.

## Cấu trúc dự án

```
dbt_project/
├── models/
│   ├── bronze/         # Models cho layer đầu tiên (đọc từ raw/public schemas)
│   ├── silver/         # Models cho layer chuyển đổi (đọc từ bronze)
│   ├── gold/           # Models cho layer cuối cùng (đọc từ silver)
│   ├── sources.yml     # Định nghĩa nguồn dữ liệu cho toàn dự án
│   └── schema.yml      # Tham chiếu toàn cục (tránh được định nghĩa)
├── macros/             # Hàm tái sử dụng
├── seeds/              # Dữ liệu tĩnh (.csv)
├── snapshots/          # SCD Type 2
├── tests/              # Custom tests
├── dbt_project.yml     # Cấu hình dự án
└── profiles.yml        # Cấu hình kết nối
```

## Các model

### Bronze Layer

- `bro_customers` - Dữ liệu khách hàng layer đầu tiên
- `bro_orders` - Dữ liệu đơn hàng layer đầu tiên

### Silver Layer

- `sil_customers` - Dữ liệu khách hàng đã làm sạch (email lowercase, tên đã trim)
- `sil_orders` - Dữ liệu đơn hàng đã làm sạch (order_date đã chuyển thành timestamp)

### Gold Layer

- `dim_customers` - Bảng dimension cho khách hàng
- `fct_orders_full_load` - Bảng fact cho đơn hàng (full load)
- `fct_orders_incremental` - Bảng fact cho đơn hàng (incremental load)

## Luồng dữ liệu

```
[S3 Data] → [Python ETL] → [Bronze] → [Silver] → [Gold]
```

## Cách chạy dbt

### Chạy toàn bộ dbt models

```bash
cd dbt_project
dbt run
```

### Chạy theo layer

```bash
# Chạy bronze layer
dbt run --select bronze.*

# Chạy silver layer
dbt run --select silver.*

# Chạy gold layer
dbt run --select gold.*
```

### Chạy model cụ thể

```bash
# Chạy bronze model
dbt run --select bronze.bro_customers

# Chạy silver model
dbt run --select silver.sil_customers

# Chạy gold model
dbt run --select gold.dim_customers
```

### Tùy chọn thêm

```bash
# Full refresh (xóa dữ liệu hiện có)
dbt run --select bronze.bro_customers --full-refresh

# Chạy với biến
dbt run --select gold.fct_orders_incremental --vars '{"source_table":"orders", "date":"2024-03-25"}'

# Sử dụng profile cụ thể
dbt run --target prod
```

### Chạy tests

```bash
# Chạy tất cả tests
dbt test

# Chạy test cho model cụ thể
dbt test --select bronze.bro_customers
```

## Lưu ý quan trọng

- Tên model trong file SQL phải khớp với tên file .sql (không bao gồm phần mở rộng)
- Tham chiếu giữa các model dùng `ref()`, tham chiếu đến source dùng `source()`
- Mỗi model chỉ được định nghĩa một lần trong schema.yml
- Khi thay đổi cấu trúc, cần chạy `dbt run --full-refresh` để tái tạo bảng

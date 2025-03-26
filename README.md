# ETL Metadata Framework

## 1. Giới thiệu

ETL Metadata Framework là Framework dùng để quản lý, theo dõi và thực thi các pipeline ETL (Extract, Transform, Load) dữ liệu. Dự án này giúp tự động hóa việc trích xuất dữ liệu từ các nguồn khác nhau (như file Parquet trên S3), tải vào cơ sở dữ liệu PostgreSQL, và sử dụng dbt (data build tool) để biến đổi dữ liệu qua nhiều layer (bronze, silver, gold).

### Yêu cầu của bài toán

Dự án này được xây dựng để thể hiện năng lực sử dụng các công nghệ Big Data, với các yêu cầu cụ thể như sau:

1. **Ingest dữ liệu**:

   - Sử dụng Apache Spark để ingest dữ liệu từ nguồn bất kỳ
   - Lưu trữ dữ liệu dưới dạng file Parquet
   - Có thể sử dụng Minio hoặc Hadoop làm storage

2. **Transform dữ liệu**:

   - Sử dụng DBT để transform dữ liệu từ Data Lakehouse
   - Đổ dữ liệu đã transform vào Data Warehouse (PostgreSQL)
   - Tạo ít nhất 2 bảng trong Data Warehouse với 2 phương thức load:
     - Full Load: Tải toàn bộ dữ liệu mới
     - Incremental Load: Tải dữ liệu mới và cập nhật (dedup/upsert)

3. **Metadata-Driven Framework**:
   - Bảng Controller: Lưu metadata về source và destination
   - Bảng Audit: Ghi trạng thái và số lượng record đã xử lý
   - Hệ thống thông báo qua email khi pipeline hoàn thành

### Những khó khăn và thách thức

1. **Kiến trúc hệ thống**:

   - Thiết kế framework linh hoạt, phải đảm bảo khả năng mở rộng để hỗ trợ nhiều nguồn dữ liệu
   - Tối ưu hiệu suất khi xử lý khối lượng dữ liệu lớn
   - Đảm bảo tính nhất quán dữ liệu khi chuyển qua các layer khác nhau
   - Xử lý các trường hợp lỗi và cơ chế tự động khôi phục

2. **Quản lý metadata**:

   - Thiết kế cấu trúc metadata đủ linh hoạt cho nhiều loại pipeline khác nhau
   - Đảm bảo tính chính xác và đồng bộ giữa metadata và dữ liệu thực tế
   - Phát triển hệ thống audit trail hoàn chỉnh để theo dõi và gỡ lỗi
   - Cân bằng giữa đơn giản hóa metadata và cung cấp đủ thông tin cho quản lý pipeline

3. **Xử lý dữ liệu phức tạp**:

   - Xử lý hiệu quả các kiểu dữ liệu đặc biệt và phi cấu trúc
   - Tối ưu chiến lược upsert/deduplication cho dữ liệu incremental
   - Quản lý phụ thuộc giữa các bảng trong quá trình transform
   - Xử lý schema evolution và data drift

4. **Vận hành và triển khai**:

   - Quản lý quy trình CI/CD cho các thay đổi về transform logic
   - Theo dõi và đảm bảo hiệu suất của hệ thống
   - Cung cấp công cụ debug hiệu quả cho các pipeline phức tạp
   - Phối hợp đồng bộ giữa các thành phần (Spark, DBT, PostgreSQL)

### Cải tiến và nâng cấp tiềm năng

1. **Mở rộng nguồn dữ liệu**:

   - Hỗ trợ thêm các nguồn dữ liệu như APIs, cơ sở dữ liệu NoSQL, message queues
   - Phát triển connectors cho các hệ thống ERP, CRM phổ biến
   - Hỗ trợ xử lý dữ liệu streaming thời gian thực

2. **Cải thiện hiệu suất**:

   - Triển khai xử lý song song cho các pipeline độc lập
   - Tối ưu hóa truy vấn SQL trong quá trình transform
   - Áp dụng partitioning và indexing strategies phù hợp
   - Caching thông minh cho dữ liệu thường xuyên truy cập

3. **Giao diện người dùng**:

   - Phát triển dashboard giám sát trực quan cho các pipeline
   - Cung cấp công cụ quản lý metadata qua web interface
   - Tích hợp trình soạn thảo SQL trực tuyến cho các biến đổi tùy chỉnh
   - Tạo báo cáo tự động về hiệu suất và chất lượng dữ liệu

4. **Quản lý chất lượng dữ liệu**:

   - Triển khai các kiểm tra chất lượng dữ liệu tự động
   - Theo dõi các chỉ số chất lượng dữ liệu theo thời gian
   - Tích hợp công cụ phát hiện anomaly và data drift
   - Áp dụng machine learning để dự đoán và ngăn chặn sự cố

5. **Mở rộng quy mô**:

   - Phát triển khả năng mở rộng ngang để xử lý khối lượng dữ liệu cực lớn
   - Tối ưu hóa chi phí bằng cách điều chỉnh tài nguyên theo nhu cầu
   - Hỗ trợ multi-region deployment cho khả năng phục hồi tốt hơn

6. **Tự động hóa và DevOps**:
   - Tự động hóa hoàn toàn quy trình CI/CD
   - Triển khai infrastructure-as-code cho toàn bộ stack
   - Tích hợp với các công cụ quản lý cấu hình như Ansible, Terraform
   - Phát triển bộ test tự động toàn diện cho các pipeline

## 2. Mục lục

- [Giới thiệu](#1-giới-thiệu)
- [Kiến trúc hệ thống](#3-kiến-trúc-hệ-thống)
- [Công nghệ sử dụng](#4-công-nghệ-sử-dụng)
- [Cài đặt](#5-cài-đặt)
- [Cấu hình](#6-cấu-hình)
- [Sử dụng](#7-sử-dụng)
- [Quản lý metadata](#8-quản-lý-metadata)
- [Lưu ý](#9-lưu-ý)

## 3. Kiến trúc hệ thống

Framework này được thiết kế theo kiến trúc metadata-driven ETL, cho phép định nghĩa các pipeline ETL thông qua cấu hình trong cơ sở dữ liệu thay vì hard-code trong ứng dụng.

### Các thành phần chính:

1. **Data Lake (S3)**: Lưu trữ dữ liệu thô dưới dạng file Parquet.
2. **Bronze Layer**: Dữ liệu thô được nạp từ Data Lake vào các bảng trong schema `public`.
3. **Silver Layer**: Dữ liệu được làm sạch và chuẩn hóa từ Bronze thông qua dbt.
4. **Gold Layer**: Dữ liệu được tổng hợp, phân tích từ Silver thông qua dbt.
5. **Metadata Framework**:
   - Bảng `controller`: Quản lý cấu hình các pipeline ETL
   - Bảng `audit`: Ghi lại lịch sử thực thi của các pipeline

### Luồng dữ liệu:

```
S3 (Parquet Files) → Bronze Layer (PostgreSQL) → Silver Layer (dbt) → Gold Layer (dbt)
```

## 4. Công nghệ sử dụng

- **Python**: Ngôn ngữ lập trình chính
- **PostgreSQL**: Cơ sở dữ liệu quan hệ
- **AWS S3**: Lưu trữ dữ liệu thô
- **Boto3**: Thư viện Python tương tác với AWS
- **Pandas**: Xử lý dữ liệu dạng bảng
- **dbt (data build tool)**: Công cụ biến đổi dữ liệu
- **SQLAlchemy**: ORM và công cụ làm việc với cơ sở dữ liệu
- **Psycopg2**: Driver PostgreSQL cho Python
- **Python-dotenv**: Quản lý biến môi trường

## 5. Cài đặt

### Yêu cầu hệ thống

- Python 3.8+
- PostgreSQL 12+
- Quyền truy cập vào bucket S3 (nếu sử dụng AWS)
- dbt CLI (nếu sử dụng công cụ dbt bên ngoài)

### Cài đặt môi trường

1. Clone repository

   ```bash
   git clone https://github.com/nits302/ETL-Metadata-Framework
   cd etl_metadata_framework
   ```

2. Tạo và kích hoạt môi trường ảo

   ```bash
   python -m venv venv
   source venv/bin/activate  # Trên Linux/Mac
   venv\Scripts\activate     # Trên Windows
   ```

3. Cài đặt các thư viện phụ thuộc
   ```bash
   pip install -r requirements.txt
   ```

## 6. Cấu hình

1. Tạo file `.env` từ file mẫu `.env.example`:

   ```bash
   cp .env.example .env
   ```

2. Cập nhật file `.env` với thông tin cấu hình của bạn:

   ```
   # PostgreSQL
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DATABASE=etl_metadata
   POSTGRES_USER=username
   POSTGRES_PASSWORD=password

   # AWS
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   AWS_BUCKET_NAME=your_bucket
   AWS_REGION=ap-southeast-1

   # Email
   EMAIL_HOST=smtp.example.com
   EMAIL_PORT=587
   EMAIL_USER=your_email@example.com
   EMAIL_PASSWORD=your_email_password
   EMAIL_RECIPIENTS=recipient1@example.com,recipient2@example.com

   # DBT
   DBT_PROJECT_DIR=./dbt_project
   ```

3. Khởi tạo metadata framework:

   ```bash
   python -m src.initialize_metadata_framework
   ```

4. Cấu hình dbt:
   - Chỉnh sửa file `dbt_project/profiles.yml` để kết nối với cơ sở dữ liệu

## 7. Sử dụng

### Tải dữ liệu lên S3

1. Đầu tiên, cần tải dữ liệu lên S3 thông qua script `ingest_to_lake`:

   ```bash
   python -m src.ingest_to_lake.py
   ```

- Load data lên AWS S3

### Thêm table controller và audit và Database

- Đầu tiên, chạy script khởi tạo metadata framework để tạo các bảng cần thiết:

  ```bash
  python -m src.initialize_metadata_framework.py
  ```

  Script này sẽ tạo 2 bảng trong cơ sở dữ liệu:

  - Bảng `controller`: Lưu trữ cấu hình của các pipeline
  - Bảng `audit`: Ghi lại lịch sử thực thi của các pipeline

### Chạy pipeline

1. Chạy với ngày cụ thể (cho dữ liệu phân vùng theo ngày):

   ```bash
   python -m src.etl.py --date 20250323
   ```

   ```bash
   python -m src.etl.py --date 20250324
   ```

   - Chỉnh sửa phần data_source trong table controller thành \_test (vd: customers_test, orders_test)

   ```bash
   python -m src.etl.py --date 20250325
   ```

   ```bash
   python -m src.etl.py --date 20250326
   ```

2. Bỏ qua bước load vào Database (chỉ transform dữ liệu):
   ```bash
   python -m src.etl.py --skip-load
   ```

## 8. Quản lý metadata

### Cấu trúc bảng metadata

1. **Bảng controller**: Cấu hình pipeline

   - `id`: ID của pipeline
   - `source_table`: Tên bảng nguồn/tên dataset trên S3
   - `destination_table`: Tên bảng đích
   - `schema_name`: Schema của bảng đích
   - `data_source`: Đường dẫn đến dữ liệu nguồn
   - `load_type`: Kiểu tải dữ liệu (full/incremental)
   - `active`: Trạng thái của pipeline
   - `created_at`, `updated_at`: Thời gian tạo/cập nhật

2. **Bảng audit**: Theo dõi lịch sử thực thi
   - `audit_id`: ID của bản ghi audit
   - `pipeline_id`: ID của pipeline
   - `status`: Trạng thái thực thi (running/success/failed)
   - `start_time`, `end_time`: Thời gian bắt đầu/kết thúc
   - `records_processed`: Số lượng bản ghi được xử lý
   - `error_message`: Thông báo lỗi (nếu có)

## 9. Lưu ý

- Đảm bảo tính nhất quán giữa các bảng trong cơ sở dữ liệu
- Kiểm tra và xử lý các trường hợp lỗi trong quá trình thực thi pipeline

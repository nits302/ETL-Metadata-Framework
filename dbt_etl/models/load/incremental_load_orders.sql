{{
  config(
    materialized='incremental',
    schema='load',
    unique_key='order_id',
    incremental_strategy='merge'
  )
}}

with source as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    -- Nếu chạy ở chế độ incremental, chỉ lấy dữ liệu mới hoặc cập nhật
    where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),

transformed as (
    select
        order_id,
        customer_id,
        product_name,
        quantity,
        price,
        order_date,
        order_total,
        current_timestamp as loaded_at
    from source
)

select * from transformed 
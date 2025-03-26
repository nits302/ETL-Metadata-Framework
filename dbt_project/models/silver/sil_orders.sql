{{
  config(
    materialized='incremental',
    unique_key='order_id',
    schema='silver'
  )
}}

with source as (
    select * from {{ ref('bro_orders') }}
),

sil_orders as (
    select
        order_id,
        customer_id,
        product_name,
        quantity,
        price,
        TO_TIMESTAMP(order_date) as order_date,
        quantity * price as order_total,
        loaded_at
    from source
)

{% if is_incremental() %}
    -- Nếu là incremental load, chỉ lấy dữ liệu mới
    SELECT * FROM sil_orders
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% else %}
    -- Nếu là full load, lấy tất cả dữ liệu
    SELECT * FROM sil_orders
{% endif %} 
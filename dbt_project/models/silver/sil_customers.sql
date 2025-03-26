{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    schema='silver'
  )
}}

with source as (
    select * from {{ ref('bro_customers') }}
),

sil_customers as (
    select
        customer_id,
        trim(name) as customer_name,
        lower(email) as email,
        phone,
        address,
        TO_TIMESTAMP(created_at) as created_at,
        loaded_at
    from source
)

{% if is_incremental() %}
    -- Nếu là incremental load, chỉ lấy dữ liệu mới
    SELECT * FROM sil_customers
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% else %}
    -- Nếu là full load, lấy tất cả dữ liệu
    SELECT * FROM sil_customers
{% endif %}
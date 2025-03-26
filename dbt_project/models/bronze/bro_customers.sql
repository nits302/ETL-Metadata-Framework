{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        schema='bronze'
    )
}}


WITH source_data AS (
    SELECT
        customer_id,
        name,
        email,
        phone,
        address,
        created_at,
        current_timestamp as loaded_at
    FROM {{ source('postgres_raw', 'customers') }}
)

{% if is_incremental() %}
    -- Nếu là incremental load, chỉ lấy dữ liệu mới
    SELECT * FROM source_data
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% else %}
    -- Nếu là full load, lấy tất cả dữ liệu
    SELECT * FROM source_data
{% endif %} 
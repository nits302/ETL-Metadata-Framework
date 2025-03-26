{{
    config(
        materialized='incremental',
        unique_key='order_id',
        schema='bronze'
    )
}}


WITH source_data AS (
    SELECT
        order_id,
        customer_id,
        product_name,
        quantity,
        price,
        order_date,
        current_timestamp as loaded_at
    FROM {{ source('postgres_raw', 'orders') }}
)

{% if is_incremental() %}
    SELECT * FROM source_data
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% else %}
    SELECT * FROM source_data
{% endif %} 
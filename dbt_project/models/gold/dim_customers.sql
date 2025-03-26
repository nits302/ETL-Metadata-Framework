{{
  config(
    materialized='table',
    schema='gold'
  )
}}

with customers as (
    select * from {{ ref('sil_customers') }}
),

-- Dimension model for customers
dim_customers as (
    SELECT
        customer_id,
        customer_name,
        email,
        phone,
        address,
        created_at,
        loaded_at
    FROM customers
)

select * from dim_customers 
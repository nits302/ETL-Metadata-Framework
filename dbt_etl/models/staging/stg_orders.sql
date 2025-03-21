with source as (
    select * from "etl_metadata"."public"."orders"
),

staged as (
    select
        order_id,
        customer_id,
        product_name,
        coalesce(quantity, 0) as quantity, 
        coalesce(price, 0.0) as price,
        TO_TIMESTAMP(order_date / 1000) as order_date,
        -- Tính tổng giá trị đơn hàng
        coalesce(quantity, 0) * coalesce(price, 0.0) as order_total
    from source
)

select * from staged
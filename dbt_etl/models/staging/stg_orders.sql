with source as (
    select * from "etl_metadata"."public"."orders"
),

staged as (
    select
        order_id,
        coalesce(customer_id, 'Unknown') as customer_id,
        coalesce(product_name, 'Unknown') as product_name,
        coalesce(quantity, 0) as quantity, 
        coalesce(price, 0.0) as price,
        case 
            when order_date is null then null
            else TO_TIMESTAMP(order_date)
        end as order_date,
        -- Tính tổng giá trị đơn hàng
        coalesce(quantity, 0) * coalesce(price, 0.0) as order_total
    from source
)

select * from staged
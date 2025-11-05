with customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(orders.amount) as lifetime_value

    from orders
    group by 1

),

sum_debug as (select sum(lifetime_value) from customer_orders),

sum_debug as (select sum(amount) from orders),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        lifetime_value

    from customers

    left join customer_orders using (customer_id)

),

sum_debug as (select sum(lifetime_value) from final)


select * from final

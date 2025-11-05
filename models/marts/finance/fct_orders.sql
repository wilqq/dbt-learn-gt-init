with orders as (
  select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
  select * from {{ ref('stg_stripe__payments') }}
),

order_payments as (
  select
    order_id,
    sum (case when status = 'success' then amount else 0 end) as amount
  from payments
  group by order_id
),

final as (
  select
    order_id,
    customer_id,
    order_date,
    coalesce (order_payments.amount, 0) as amount
  from
    orders
  left join order_payments using (order_id)
),

sum_debug as (select sum(amount) from final)


select * from final

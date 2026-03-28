{{ config(
    materialized='table',
    tags=['gold']
) }}

with s as (
  select * from {{ ref('gold_user_funnel_daily') }}
)

select event_date, 1 as step_order, 'Page View' as step_name, users_pv as users from s
union all
select event_date, 2 as step_order, 'Product View', users_product from s
union all
select event_date, 3 as step_order, 'Add To Cart', users_cart from s
union all
select event_date, 4 as step_order, 'Purchase', users_purchase from s
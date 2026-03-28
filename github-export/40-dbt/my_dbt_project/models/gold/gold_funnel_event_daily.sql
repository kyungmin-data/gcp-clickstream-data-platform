{{ config(
    materialized='table',
    tags=['gold']
) }}

with base as (
    select
        event_date,
        session_id,
        event_type
    from {{ ref('slv_clickstream') }}
    where event_type in (
        'page_view',
        'product_view',
        'add_to_cart',
        'purchase'
    )
),

session_funnel as (
    select
        event_date,
        session_id,
        max(event_type = 'page_view') as has_page_view,
        max(event_type = 'product_view') as has_product_view,
        max(event_type = 'add_to_cart') as has_add_to_cart,
        max(event_type = 'purchase') as has_purchase
    from base
    group by event_date, session_id
),

agg as (
    select
        event_date,

        countif(has_page_view) as page_view_sessions,
        countif(has_page_view and has_product_view) as product_view_sessions,
        countif(has_page_view and has_product_view and has_add_to_cart) as add_to_cart_sessions,
        countif(has_page_view and has_product_view and has_add_to_cart and has_purchase) as purchase_sessions
    from session_funnel
    group by event_date
)

select
    event_date,
    step_order,
    step_name,
    sessions
from agg,
unnest([
    struct(1 as step_order, 'Page View' as step_name, page_view_sessions as sessions),
    struct(2, 'Product View', product_view_sessions),
    struct(3, 'Add To Cart', add_to_cart_sessions),
    struct(4, 'Purchase', purchase_sessions)
])
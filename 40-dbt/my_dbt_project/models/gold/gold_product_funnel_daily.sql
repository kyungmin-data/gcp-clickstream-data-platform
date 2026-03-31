{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "event_date", "data_type": "date"},
    cluster_by=["product_id"],
    on_schema_change='sync_all_columns',
    tags=['gold']
) }}

with base as (
  select
    event_date,
    cast(product_id as string) as product_id,
    cast(user_id as string) as user_id,
    cast(session_id as string) as session_id,
    event_type
  from {{ ref('slv_clickstream') }}
  where event_date is not null
    and product_id is not null
    and user_id is not null
    and session_id is not null
    and event_type in ('page_view','product_view','add_to_cart','purchase')

  {% if is_incremental() %}
    and event_date = date('{{ var("batch_date") }}')
  {% endif %}
),

agg as (
  select
    event_date,
    product_id,

    -- sessions
    count(distinct session_id) as sessions_with_product,
    count(distinct case when event_type = 'product_view' then session_id end) as product_view_sessions,
    count(distinct case when event_type = 'add_to_cart' then session_id end) as add_to_cart_sessions,
    count(distinct case when event_type = 'purchase' then session_id end) as purchase_sessions,

    -- users  ✅ 여기 추가
    count(distinct user_id) as users_with_product,
    count(distinct case when event_type = 'page_view' then user_id end) as page_view_users,
    count(distinct case when event_type = 'product_view' then user_id end) as product_view_users,
    count(distinct case when event_type = 'add_to_cart' then user_id end) as add_to_cart_users,
    count(distinct case when event_type = 'purchase' then user_id end) as purchase_users

  from base
  group by 1,2
)

select
  event_date,
  product_id,

  sessions_with_product,
  product_view_sessions,
  add_to_cart_sessions,
  purchase_sessions,

  users_with_product,
  page_view_users,
  product_view_users,
  add_to_cart_users,
  purchase_users,

  -- 기존 “세션 기반 rate”도 유지 가능
  safe_divide(add_to_cart_sessions, nullif(product_view_sessions, 0)) as product_to_cart_rate,
  safe_divide(purchase_sessions, nullif(add_to_cart_sessions, 0)) as cart_to_purchase_rate,
  safe_divide(purchase_sessions, nullif(product_view_sessions, 0)) as pview_to_purchase_rate,

  -- ✅ 유저 기반 rate (Looker에서 더 “사람 기준”으로 설득력 있음)
  safe_divide(add_to_cart_users, nullif(product_view_users, 0)) as product_to_cart_user_rate,
  safe_divide(purchase_users, nullif(add_to_cart_users, 0)) as cart_to_purchase_user_rate,
  safe_divide(purchase_users, nullif(product_view_users, 0)) as pview_to_purchase_user_rate

from agg
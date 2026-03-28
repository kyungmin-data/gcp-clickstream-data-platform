{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['event_date'],
    tags=['gold'],
    on_schema_change='sync_all_columns'
) }}

-- ✅ 가정:
-- 1) silver 레이어에 event_date(date), user_id, event_type 가 있다
-- 2) event_type 값이 'page_view','product_view','add_to_cart','purchase' 중 하나로 정규화돼 있다

with base as (
  select
    event_date,
    user_id,
    event_type
  from {{ ref('slv_clickstream') }}
  where user_id is not null
    and event_type in ('page_view','product_view','add_to_cart','purchase')

  {% if is_incremental() %}
    -- 증분일 때는 보통 최근 며칠만 재계산 (원하면 기간 조정)
    and event_date >= date_sub(current_date(), interval 7 day)
  {% endif %}
),

per_user_day as (
  select
    event_date,
    user_id,
    max(case when event_type = 'page_view' then 1 else 0 end) as did_pv,
    max(case when event_type = 'product_view' then 1 else 0 end) as did_product,
    max(case when event_type = 'add_to_cart' then 1 else 0 end) as did_cart,
    max(case when event_type = 'purchase' then 1 else 0 end) as did_purchase
  from base
  group by 1,2
),

agg as (
  select
    event_date,

    -- ✅ 퍼널은 “이전 단계 충족” 조건을 붙여야 진짜 퍼널
    countif(did_pv = 1) as users_pv,

    countif(did_pv = 1 and did_product = 1) as users_product,

    countif(did_pv = 1 and did_product = 1 and did_cart = 1) as users_cart,

    countif(did_pv = 1 and did_product = 1 and did_cart = 1 and did_purchase = 1) as users_purchase

  from per_user_day
  group by 1
)

select
  event_date,
  users_pv,
  users_product,
  users_cart,
  users_purchase,
  safe_divide(users_product, users_pv) as cvr_pv_to_product,
  safe_divide(users_cart, users_product) as cvr_product_to_cart,
  safe_divide(users_purchase, users_cart) as cvr_cart_to_purchase,
  safe_divide(users_purchase, users_pv) as cvr_pv_to_purchase
from agg
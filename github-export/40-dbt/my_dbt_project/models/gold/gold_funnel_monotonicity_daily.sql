{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "event_date", "data_type": "date"},
    tags=['gold', 'dq']
) }}

with base as (
  select
    event_date,
    page_view_sessions,
    product_view_sessions,
    add_to_cart_sessions,
    purchase_sessions,
    proc_ts
  from {{ ref('gold_funnel_daily_overall') }}

  {% if is_incremental() %}
    where event_date = date('{{ var("batch_date") }}')
  {% endif %}
)

select
  event_date,

  -- monotonic 조건
  page_view_sessions >= product_view_sessions as pv_ge_pr,
  product_view_sessions >= add_to_cart_sessions as pr_ge_cart,
  add_to_cart_sessions >= purchase_sessions as cart_ge_purchase,

  -- “역전 케이스” 강조용 (구매가 뷰보다 많다? 그건 데이터가 미쳤다는 뜻…)
  cast(purchase_sessions > page_view_sessions as int64) as purchase_gt_pv_flag,
  cast(purchase_sessions as int64) as purchase_sessions,
  cast(page_view_sessions as int64) as page_view_sessions,

  -- 최종 pass
  (page_view_sessions >= product_view_sessions
   and product_view_sessions >= add_to_cart_sessions
   and add_to_cart_sessions >= purchase_sessions) as monotonic_pass,

  -- 점수화(0~100)
  cast(100 * (
    cast(page_view_sessions >= product_view_sessions as int64)
    + cast(product_view_sessions >= add_to_cart_sessions as int64)
    + cast(add_to_cart_sessions >= purchase_sessions as int64)
  ) / 3 as int64) as monotonic_score,

  proc_ts
from base

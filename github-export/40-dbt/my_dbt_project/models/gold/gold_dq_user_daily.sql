{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "event_date", "data_type": "date"},
    cluster_by=["event_date"],
    on_schema_change='sync_all_columns',
    tags=['gold']
) }}

with src as (
  select
    user_id,
    session_id,
    timestamp,
    event_date,
    event_type
  from {{ ref('slv_clickstream') }}
  where user_id is not null
    and event_date is not null
    and event_type in ('page_view','product_view','add_to_cart','purchase')
    and event_date between date('{{ var("start_date","2024-01-01") }}')
                        and date('{{ var("end_date","2024-12-31") }}')
),

filtered as (
  select *
  from src
  {% if is_incremental() %}
    where event_date = date('{{ var("batch_date") }}')
  {% endif %}
),

-- 유저-일 단위로 각 단계의 "최초 발생 시각"을 잡음 (순서 검증 가능)
user_day as (
  select
    event_date,
    user_id,

    min(if(event_type='page_view',    timestamp, null)) as first_pv_ts,
    min(if(event_type='product_view', timestamp, null)) as first_pr_ts,
    min(if(event_type='add_to_cart',  timestamp, null)) as first_cart_ts,
    min(if(event_type='purchase',     timestamp, null)) as first_pur_ts

  from filtered
  group by 1,2
),

flags as (
  select
    event_date,
    user_id,

    (first_pv_ts   is not null) as has_pv,
    (first_pr_ts   is not null) as has_pr,
    (first_cart_ts is not null) as has_cart,
    (first_pur_ts  is not null) as has_purchase,

    -- "구매는 있는데, 해당일에 prior 단계가 아예 없다" (유저 관점 이상치)
    (first_pur_ts is not null and first_pv_ts   is null) as pur_without_pv,
    (first_pur_ts is not null and first_pr_ts   is null) as pur_without_pr,
    (first_pur_ts is not null and first_cart_ts is null) as pur_without_cart,

    -- 순서 역전(해당일 기준): 구매 시각보다 PV/PR/Cart 최초 시각이 뒤에 있으면 이상
    (first_pur_ts is not null and first_pv_ts   is not null and first_pv_ts   > first_pur_ts) as pv_after_purchase,
    (first_pur_ts is not null and first_pr_ts   is not null and first_pr_ts   > first_pur_ts) as pr_after_purchase,
    (first_pur_ts is not null and first_cart_ts is not null and first_cart_ts > first_pur_ts) as cart_after_purchase

  from user_day
),

agg as (
  select
    event_date,

    count(distinct user_id) as total_users,

    countif(has_pv) as users_pv,
    countif(has_pr) as users_pr,
    countif(has_cart) as users_cart,
    countif(has_purchase) as users_purchase,

    countif(pur_without_pv) as users_purchase_without_pv,
    countif(pur_without_pr) as users_purchase_without_pr,
    countif(pur_without_cart) as users_purchase_without_cart,

    countif(pv_after_purchase or pr_after_purchase or cart_after_purchase) as users_time_inversion,

    -- 유저 기준 Monotonicity (해당일): PV ≥ PR ≥ Cart ≥ Purchase
    -- 여기서는 "카운트 역전"을 체크 (이상적 funnel은 단계가 내려갈수록 줄어야 함)
    -- (주의) user-level에서 각 단계 정의가 독립이면 100% strict는 어려울 수 있어, 그래서 rate로 본다.
    case
      when count(distinct user_id) = 0 then 1.0
      else
        (
          case when countif(has_pv) >= countif(has_pr)
                 and countif(has_pr) >= countif(has_cart)
                 and countif(has_cart) >= countif(has_purchase)
               then 1.0 else 0.0 end
        )
    end as monotonicity_ok_flag,

    round( safe_divide(countif(pur_without_pv), nullif(countif(has_purchase),0)) * 100, 2 ) as pur_without_pv_rate_pct,
    round( safe_divide(countif(pur_without_cart), nullif(countif(has_purchase),0)) * 100, 2 ) as pur_without_cart_rate_pct

  from flags
  group by 1
)

select
  event_date,
  total_users,
  users_pv,
  users_pr,
  users_cart,
  users_purchase,
  users_purchase_without_pv,
  users_purchase_without_pr,
  users_purchase_without_cart,
  users_time_inversion,
  monotonicity_ok_flag,
  pur_without_pv_rate_pct,
  pur_without_cart_rate_pct,
  current_timestamp() as proc_ts
from agg

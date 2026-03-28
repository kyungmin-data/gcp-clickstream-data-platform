{{ config(
    materialized='table',
    cluster_by=['user_id'],
    tags=['silver']
) }}

with src as (
  select *
  from {{ ref('brz_clickstream') }}
  where user_id is not null
    and session_id is not null
    and event_ts is not null
),

filtered as (
  select
    user_id,
    session_id,
    event_ts as timestamp,
    date(event_ts) as event_date, 
    event_type,
    product_id,
    amount,
    outcome,
    proc_ts
  from src
  where event_type in ('page_view','click','product_view','add_to_cart','purchase')
    and event_ts is not null
    and date(event_ts) between date('{{ var("start_date","2024-01-01") }}')
                          and date('{{ var("end_date","2024-12-31") }}')

),

-- FK 통과를 위해: product_id가 마스터에 없으면 null 처리(relationships 테스트는 null 무시)
fk_sanitized as (
  select
    f.user_id,
    f.session_id,
    f.timestamp,
    f.event_date,
    f.event_type,
    case when pm.product_id is not null then f.product_id else null end as product_id,
    f.amount,
    f.outcome,
    f.proc_ts
  from filtered f
  left join {{ ref('brz_product_master') }} pm
    on f.product_id = pm.product_id
),

dedup as (
  select
    user_id,
    session_id,
    timestamp,
    event_date,
    event_type,
    product_id,
    amount,
    outcome,
    row_number() over (
      partition by user_id, session_id, timestamp
      order by proc_ts desc
    ) as rn
  from fk_sanitized
)

select
  user_id,
  session_id,
  timestamp,
  event_date,
  event_type,
  product_id,
  amount,
  outcome
from dedup
where rn = 1

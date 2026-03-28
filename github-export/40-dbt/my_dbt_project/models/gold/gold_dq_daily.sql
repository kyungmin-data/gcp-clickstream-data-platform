{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "event_date", "data_type": "date"},
    tags=["gold","dq"],
    on_schema_change='sync_all_columns'
) }}

with base as (
  select
    date(`timestamp`) as event_date,

    user_id,
    session_id,

    -- event_ts는 그대로 유지 (혹은 타입 캐스팅)
    `timestamp` as event_ts,

    event_type,
    product_id,
    amount,
    outcome
  from {{ ref('slv_clickstream') }}

  where date(`timestamp`) between date('{{ var("start_date", "2024-01-01") }}')
                          and date('{{ var("end_date",   "2024-12-31") }}')
  {% if is_incremental() %}
    and date(`timestamp`) = date('{{ var("batch_date") }}')
  {% endif %}

),

dim as (
  select distinct product_id
  from {{ ref('brz_product_master') }}
),

dups as (
  select
    event_date,
    sum(cnt - 1) as duplicate_rows
  from (
    select
      event_date, user_id, session_id, event_ts,
      count(*) as cnt
    from base
    group by 1,2,3,4
    having cnt > 1
  )
  group by 1
),

agg as (
  select
    b.event_date,
    count(*) as total_rows,

    countif(user_id is null) as null_user_id,
    countif(session_id is null) as null_session_id,
    countif(event_type is null) as null_event_type,

    countif(event_type not in ('page_view','click','product_view','add_to_cart','purchase')) as invalid_event_type,

    countif(event_type in ('product_view','add_to_cart','purchase') and b.product_id is null) as null_product_required,
    countif(event_type = 'purchase' and amount is null) as null_purchase_amount,

    countif(b.product_id is not null and d.product_id is null) as product_not_in_master,

    safe_divide(countif(event_type in ('product_view','add_to_cart','purchase') and b.product_id is null), count(*)) as null_product_required_rate,
    safe_divide(countif(event_type = 'purchase' and amount is null), nullif(countif(event_type='purchase'),0)) as null_purchase_amount_rate,

    current_timestamp() as proc_ts
  from base b
  left join dim d
    on b.product_id = d.product_id
  group by 1
),

final as (
  select
    a.*,
    coalesce(d.duplicate_rows, 0) as duplicate_rows,

    -- 지금 운영상 “반드시 0이어야 하는” 코어 품질
    (
      null_user_id = 0
      and null_session_id = 0
      and null_event_type = 0
      and invalid_event_type = 0
      and product_not_in_master = 0
      and coalesce(d.duplicate_rows,0) = 0
    ) as dq_pass_core,

    -- 이상적(최종 목표) 품질: 아직은 fail 시켜도 좋지만 "차트로" 보여줄 값
    (
      null_product_required = 0
      and null_purchase_amount = 0
    ) as dq_pass_strict,

    -- 발표용 DQ 점수(가중치 마음대로 조정 가능)
    greatest(
      0,
      100
      - 40 * if(product_not_in_master > 0, 1, 0)
      - 30 * if(coalesce(d.duplicate_rows,0) > 0, 1, 0)
      - 20 * if(null_product_required > 0, 1, 0)
      - 10 * if(null_purchase_amount > 0, 1, 0)
    ) as dq_score
  from agg a
  left join dups d
    using (event_date)
)

select * from final

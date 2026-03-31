{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "event_date", "data_type": "date"},
    cluster_by=["event_date"],
    tags=["gold"],
    on_schema_change='sync_all_columns'
) }}

with base as (
  select
    date(`timestamp`) as event_date,
    
    cast(user_id as string) as user_id,
    cast(session_id as string) as session_id,

    -- event_ts는 그대로 유지 (혹은 타입 캐스팅)
    `timestamp` as event_ts,

    event_type
  from {{ ref('slv_clickstream') }}

  where date(`timestamp`) between date('{{ var("start_date", "2024-01-01") }}')
                          and date('{{ var("end_date",   "2024-12-31") }}')
  {% if is_incremental() %}
    and date(`timestamp`) = date('{{ var("batch_date") }}')
  {% endif %}

),

session_level as (
  select
    event_date,
    concat(user_id, '|', session_id) as session_key,

    max(if(event_type='page_view', 1, 0)) as has_page_view,
    max(if(event_type='click', 1, 0)) as has_click,
    max(if(event_type='product_view', 1, 0)) as has_product_view,
    max(if(event_type='add_to_cart', 1, 0)) as has_add_to_cart,
    max(if(event_type='purchase', 1, 0)) as has_purchase,

    min(if(event_type='page_view', event_ts, null)) as first_pv_ts,
    min(if(event_type='purchase', event_ts, null)) as first_purchase_ts
  from base
  group by 1,2
),

final as (
  select
    event_date,
    count(*) as sessions,
    countif(has_page_view=1) as page_view_sessions,
    countif(has_product_view=1) as product_view_sessions,
    countif(has_add_to_cart=1) as add_to_cart_sessions,
    countif(has_purchase=1) as purchase_sessions,

    safe_divide(countif(has_product_view=1), nullif(countif(has_page_view=1),0)) as page_to_product_rate,
    safe_divide(countif(has_add_to_cart=1), nullif(countif(has_product_view=1),0)) as product_to_cart_rate,
    safe_divide(countif(has_purchase=1), nullif(countif(has_add_to_cart=1),0)) as cart_to_purchase_rate,
    safe_divide(countif(has_purchase=1), nullif(countif(has_product_view=1),0)) as product_to_purchase_rate,
    safe_divide(countif(has_purchase=1), nullif(countif(has_product_view=1),0)) as page_to_purchase_rate,

    avg(timestamp_diff(first_purchase_ts, first_pv_ts, second)) as sec_page_to_purchase_avg,

    current_timestamp() as proc_ts

  from session_level
  group by 1
)

select * from final

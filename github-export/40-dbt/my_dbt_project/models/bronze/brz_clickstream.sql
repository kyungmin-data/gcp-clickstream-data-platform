{{ config(
    materialized='incremental',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['user_id', 'session_id'],
    incremental_strategy='insert_overwrite',
    tags=['bronze']
) }}

with src as (
  select
    -- NaN 같은 비정상 토큰을 JSON 파싱 가능하게 치환
    regexp_replace(value, r'(?i)\bNaN\b', 'null') as v
  from `intrepid-craft-483112-c1.test.clickstream_ext`
),

parsed as (
  select
    safe_cast(json_value(v, '$.UserID') as int64) as user_id,
    safe_cast(json_value(v, '$.SessionID') as int64) as session_id,
    json_value(v, '$.Timestamp') as timestamp_str,

    -- 이벤트 타입 표준화(소문자 + 공백->언더스코어)
    lower(replace(json_value(v, '$.EventType'), ' ', '_')) as event_type,

    -- ProductID 키가 여러 형태일 수 있어 coalesce
    coalesce(
      json_value(v, '$.ProductID'),
      json_value(v, '$.product_id'),
      json_value(v, '$.productId')
    ) as product_id_raw,

    safe_cast(json_value(v, '$.Amount') as float64) as amount,
    safe_cast(json_value(v, '$.Outcome') as float64) as outcome
  from src
),

with_ts as (
  select
    *,
    coalesce(
      safe_cast(timestamp_str as timestamp),
      safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S', timestamp_str),
      safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp_str),
      safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S%Ez', timestamp_str),
      safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', timestamp_str)
    ) as event_ts
  from parsed
),

final as (
  select
    -- 이벤트 유니크키(테스트용/추적용)
    to_hex(md5(concat(
      cast(user_id as string), '|',
      cast(session_id as string), '|',
      ifnull(timestamp_str, ''), '|',
      ifnull(event_type, ''), '|',
      ifnull(product_id_raw, '')
    ))) as event_id,

    user_id,
    session_id,
    event_ts,
    date(event_ts) as event_date,
    event_type,

    product_id_raw,

    -- 정규화: prod_8199 -> 8199 / 8199 -> 8199
    regexp_extract(product_id_raw, r'(\d{1,4})$') as product_id,

    amount,
    outcome,
    current_timestamp() as proc_ts
  from with_ts
  where event_ts is not null

  {% if is_incremental() %}
    and date(event_ts) = date('{{ var("batch_date") }}')
  {% endif %}
)

select * from final

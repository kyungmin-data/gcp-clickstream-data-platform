{{ config(materialized='view', tags=['bronze']) }}

with src as (
  select *
  from {{ ref('product_master') }}
)

select
  -- seed 원본 보존
  product_id as product_id_raw,

  -- FK/테스트용 정규화 키: prod_8199 -> 8199
  regexp_extract(product_id, r'(\d{1,4})$') as product_id,

  manufacturer,
  management_group
from src
where product_id is not null
  and regexp_extract(product_id, r'(\d{1,4})$') is not null

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['product_id'],
    tags=['gold', 'clickstream']
) }}

{% set batch_date = var('batch_date', '1970-01-01') %}

with src as (
  select
    date(`timestamp`) as event_date,
    product_id,
    event_type
  from {{ ref('slv_clickstream') }}
  where product_id is not null
  {% if is_incremental() %}
    and date(`timestamp`) = date('{{ batch_date }}')
  {% endif %}
),

agg as (
  select
    event_date,
    product_id,
    countif(event_type = 'product_view') as product_views,
    countif(event_type = 'add_to_cart')  as cart_adds,
    countif(event_type = 'purchase')     as purchases
  from src
  group by 1, 2
)

select * from agg

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id'
    )
}}

select
    order_id,
    customer_id,
    order_date,
    status

from {{ ref('stg_orders') }}

{% if is_incremental() %}

  -- pick up any orders on or after the latest date already loaded;
  -- the merge unique_key ensures same-date records already present are updated
  -- rather than duplicated.
  where order_date >= (select max(order_date) from {{ this }})

{% endif %}

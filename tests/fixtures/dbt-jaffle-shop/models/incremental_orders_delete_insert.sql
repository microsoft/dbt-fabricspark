{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
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

  -- pick up any orders on or after the latest date already loaded; the
  -- delete+insert unique_key removes the matching order_ids already present and
  -- re-inserts them so same-id records are replaced rather than duplicated.
  where order_date >= (select max(order_date) from {{ this }})

{% endif %}

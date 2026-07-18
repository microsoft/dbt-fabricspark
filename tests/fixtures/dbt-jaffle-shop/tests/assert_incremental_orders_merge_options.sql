{{ config(tags=['merge_options']) }}

{#-- Correctness check for the advanced-merge CDC model. After the incremental
     run the table must contain exactly the expected post-merge rows: order 1
     updated, order 2 deleted (not matched by source), order 3 kept (condition
     false), order 4 inserted. Returns rows only on divergence (0 rows = pass). --#}

with actual as (
    select cast(order_id as bigint) as order_id, status
    from {{ ref('incremental_orders_merge_options') }}
),

expected as (
    select cast(1 as bigint) as order_id, 'shipped' as status
    union all
    select cast(3 as bigint) as order_id, 'archived' as status
    union all
    select cast(4 as bigint) as order_id, 'placed' as status
),

missing as (
    select * from expected
    except
    select * from actual
),

unexpected as (
    select * from actual
    except
    select * from expected
)

select 'missing' as issue, order_id, status from missing
union all
select 'unexpected' as issue, order_id, status from unexpected

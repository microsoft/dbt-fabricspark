{{ config(tags=['merge_options']) }}

{#-- Correctness check for the advanced-merge CDC model with schema evolution.
     The incremental batch introduces a new `tracking_number` column, which must
     be added to the target via merge schema evolution. Selecting that column also
     fails the test outright if evolution did not add it. Expected final state:
       order 1 -> updated to 'shipped'; tracking_number 'TRK-001' (`update set *`
                  carries the evolved column for the matched row)
       order 2 -> deleted (not matched by source)
       order 3 -> kept (condition false); tracking_number NULL (never touched by source)
       order 4 -> inserted via `insert *`; tracking_number 'TRK-004'
     Returns rows only on divergence (0 rows = pass). --#}

with actual as (
    select cast(order_id as bigint) as order_id, status, tracking_number
    from {{ ref('incremental_orders_merge_options') }}
),

expected as (
    select cast(1 as bigint) as order_id, 'shipped' as status, cast('TRK-001' as string) as tracking_number
    union all
    select cast(3 as bigint) as order_id, 'archived' as status, cast(null as string) as tracking_number
    union all
    select cast(4 as bigint) as order_id, 'placed' as status, cast('TRK-004' as string) as tracking_number
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

select 'missing' as issue, order_id, status, tracking_number from missing
union all
select 'unexpected' as issue, order_id, status, tracking_number from unexpected

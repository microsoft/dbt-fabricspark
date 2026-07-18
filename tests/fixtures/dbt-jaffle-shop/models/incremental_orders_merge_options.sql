{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        file_format='delta',
        unique_key='order_id',
        target_alias='t',
        source_alias='s',
        matched_condition='s.updated_at > t.updated_at',
        not_matched_by_source_condition="t.status <> 'archived'",
        not_matched_by_source_action='delete',
        merge_with_schema_evolution=true
    )
}}

{% if not is_incremental() %}

-- initial load
select cast(1 as bigint) as order_id, 'placed' as status, timestamp('2024-01-01 00:00:00') as updated_at
union all
select cast(2 as bigint) as order_id, 'placed' as status, timestamp('2024-01-01 00:00:00') as updated_at
union all
select cast(3 as bigint) as order_id, 'archived' as status, timestamp('2024-01-01 00:00:00') as updated_at

{% else %}

-- CDC-style incremental batch exercising the advanced merge options:
--   order 1: present with a newer updated_at -> matched_condition holds -> updated to 'shipped'
--   order 2: absent from the batch, status <> 'archived' -> when not matched by source -> deleted
--   order 3: absent from the batch, status = 'archived' -> condition false -> kept
--   order 4: brand new -> when not matched -> inserted
select cast(1 as bigint) as order_id, 'shipped' as status, timestamp('2024-02-01 00:00:00') as updated_at
union all
select cast(4 as bigint) as order_id, 'placed' as status, timestamp('2024-02-01 00:00:00') as updated_at

{% endif %}

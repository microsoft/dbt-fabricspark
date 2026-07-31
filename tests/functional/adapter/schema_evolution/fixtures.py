"""Fixtures for the Delta MERGE schema-evolution session-conf regression tests."""

base_seed_csv = """id,val
1,10
2,20
""".lstrip()

# Uses merge_with_schema_evolution, which flips the session conf on.
evolving_incremental_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
    merge_with_schema_evolution = true,
) }}

{% if not is_incremental() %}
select cast(1 as bigint) as id, 'alice' as name
union all
select cast(2 as bigint) as id, 'bob' as name
{% else %}
select cast(2 as bigint) as id, 'bob' as name, 99 as extra
union all
select cast(3 as bigint) as id, 'carol' as name, 7 as extra
{% endif %}
""".lstrip()

evolution_snapshot_sql = """
{% snapshot evo_snapshot %}
    {{ config(
        target_schema=schema,
        unique_key='id',
        strategy='check',
        check_cols=['val'],
        file_format='delta',
    ) }}
    select * from {{ ref('base_seed') }}
{% endsnapshot %}
""".lstrip()

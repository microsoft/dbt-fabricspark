base_seed_csv = """id,name
1,alice
2,bob
3,carol
""".lstrip()

optimized_table_sql = """
{{ config(materialized='table') }}
select * from {{ ref('base_seed') }}
""".lstrip()

optimized_incremental_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    file_format='delta',
) }}
select * from {{ ref('base_seed') }}
""".lstrip()

skipped_table_sql = """
{{ config(materialized='table', auto_optimize=false) }}
select * from {{ ref('base_seed') }}
""".lstrip()

env_table_sql = """
{{ config(materialized='table') }}
select * from {{ ref('base_seed') }}
""".lstrip()

optimized_snapshot_sql = """
{% snapshot optimized_snapshot %}
    {{ config(
        target_schema=schema,
        unique_key='id',
        strategy='check',
        check_cols=['name'],
        file_format='delta',
    ) }}
    select * from {{ ref('base_seed') }}
{% endsnapshot %}
""".lstrip()

#
# Models
#

default_append_sql = """
{{ config(
    materialized = 'incremental',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

#
# Bad Models
#

bad_file_format_sql = """
{{ config(
    materialized = 'incremental',
    file_format = 'something_else',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

bad_merge_not_delta_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'parquet',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

bad_strategy_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'something_else',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

#
# Delta Models
#

append_delta_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    file_format = 'delta',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

insert_overwrite_partitions_delta_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by='id',
    file_format='delta'
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
"""


delta_merge_no_key_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

delta_merge_unique_key_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

delta_merge_update_columns_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
    merge_update_columns = ['msg'],
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- msg will be updated, color will be ignored
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
""".lstrip()

delete_insert_unique_key_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    file_format = 'delta',
    unique_key = 'id',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

bad_delete_insert_no_key_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    file_format = 'delta',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

#
# Advanced merge option models
#

merge_skip_matched_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
    skip_matched_step = true,
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

select cast(1 as bigint) as id, 'hey' as msg, 'cyan' as color
union all
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
""".lstrip()

merge_skip_not_matched_sql = merge_skip_matched_sql.replace(
    "skip_matched_step = true", "skip_not_matched_step = true"
)

merge_matched_condition_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
    source_alias = 'src',
    target_alias = 't',
    matched_condition = 'src.v > t.v and hash(src.first_name, src.last_name) <> hash(t.first_name, t.last_name)',
    not_matched_condition = 'src.v > 0',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'Vasya' as first_name, 'Pupkin' as last_name, cast(1 as bigint) as v
union all
select cast(2 as bigint) as id, 'Paul' as first_name, 'Atreides' as last_name, cast(1 as bigint) as v
union all
select cast(3 as bigint) as id, 'Dunkan' as first_name, 'Aidaho' as last_name, cast(1 as bigint) as v

{% else %}

select cast(1 as bigint) as id, 'Jessica' as first_name, 'Atreides' as last_name, cast(2 as bigint) as v  -- should merge
union all
select cast(2 as bigint) as id, 'Paul' as first_name, 'Whiskas' as last_name, cast(1 as bigint) as v  -- v unchanged, no merge
union all
select cast(3 as bigint) as id, 'Dunkan' as first_name, 'Aidaho' as last_name, cast(2 as bigint) as v  -- hash same, no merge
union all
select cast(4 as bigint) as id, 'Baron' as first_name, 'Harkonnen' as last_name, cast(1 as bigint) as v  -- should append
union all
select cast(5 as bigint) as id, 'Raban' as first_name, 'Harkonnen' as last_name, cast(0 as bigint) as v  -- no append

{% endif %}
""".lstrip()

merge_not_matched_by_source_delete_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
    target_alias = 't',
    source_alias = 's',
    skip_matched_step = true,
    not_matched_by_source_condition = 't.v > 0',
    not_matched_by_source_action = 'delete',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'Vasya' as first_name, 'Pupkin' as last_name, cast(1 as bigint) as v
union all
select cast(2 as bigint) as id, 'Paul' as first_name, 'Atreides' as last_name, cast(0 as bigint) as v
union all
select cast(3 as bigint) as id, 'Dunkan' as first_name, 'Aidaho' as last_name, cast(1 as bigint) as v

{% else %}

-- id = 1 absent from source and t.v > 0 -> deleted
-- id = 2 absent from source but t.v = 0 -> kept
select cast(3 as bigint) as id, 'Dunkan' as first_name, 'Aidaho' as last_name, cast(2 as bigint) as v  -- matched, skipped
union all
select cast(4 as bigint) as id, 'Baron' as first_name, 'Harkonnen' as last_name, cast(1 as bigint) as v  -- appended

{% endif %}
""".lstrip()

merge_not_matched_by_source_update_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
    target_alias = 't',
    source_alias = 's',
    skip_matched_step = true,
    not_matched_by_source_condition = 't.v > 0',
    not_matched_by_source_action = "update set t.first_name = '--', t.last_name = '--', t.v = -1",
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'Vasya' as first_name, 'Pupkin' as last_name, cast(1 as bigint) as v
union all
select cast(2 as bigint) as id, 'Paul' as first_name, 'Atreides' as last_name, cast(0 as bigint) as v
union all
select cast(3 as bigint) as id, 'Dunkan' as first_name, 'Aidaho' as last_name, cast(1 as bigint) as v

{% else %}

-- id = 1 absent from source and t.v > 0 -> updated to sentinel
-- id = 2 absent from source but t.v = 0 -> kept
select cast(3 as bigint) as id, 'Dunkan' as first_name, 'Aidaho' as last_name, cast(2 as bigint) as v  -- matched, skipped
union all
select cast(4 as bigint) as id, 'Baron' as first_name, 'Harkonnen' as last_name, cast(1 as bigint) as v  -- appended

{% endif %}
""".lstrip()

merge_schema_evolution_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
    merge_with_schema_evolution = true,
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'Vasya' as first_name, 'Pupkin' as last_name
union all
select cast(2 as bigint) as id, 'Paul' as first_name, 'Atreides' as last_name
union all
select cast(3 as bigint) as id, 'Dunkan' as first_name, 'Aidaho' as last_name

{% else %}

-- new column `v` must be added to the target via merge schema evolution;
-- id = 2 is not in the source so its `v` stays NULL
select cast(1 as bigint) as id, 'Jessica' as first_name, 'Atreides' as last_name, cast(1 as bigint) as v
union all
select cast(3 as bigint) as id, 'Dunkan' as first_name, 'Aidaho' as last_name, cast(2 as bigint) as v

{% endif %}
""".lstrip()

#
# Full Refresh Models (no explicit file_format — reproduces TABLE_OR_VIEW_ALREADY_EXISTS bug)
#

merge_full_refresh_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
) }}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg
""".lstrip()

#
# Insert Overwrite
#

insert_overwrite_no_partitions_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    file_format = 'delta',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

insert_overwrite_partitions_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = 'id',
    file_format = 'delta',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
""".lstrip()

{% macro get_insert_overwrite_sql(source_relation, target_relation, existing_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}    
    insert overwrite table {{ target_relation }}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}


{% macro get_microbatch_delete_sql(source_relation, target_relation, partition_by) %}
    {#-- Delete existing rows whose partition values appear in the incoming batch.
         Uses MERGE ... WHEN MATCHED THEN DELETE because Delta Lake does not
         support subqueries in DELETE conditions. --#}
    {%- if partition_by is string -%}
      {%- set partition_by = [partition_by] -%}
    {%- endif -%}
    merge into {{ target_relation }} as DBT_INTERNAL_DEST
        using (select distinct
          {%- for col in partition_by %}
            {{ col }}{% if not loop.last %}, {% endif %}
          {%- endfor %}
          from {{ source_relation }}) as DBT_INTERNAL_SOURCE
        on
          {%- for col in partition_by %}
            DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %} and {% endif %}
          {%- endfor %}
        when matched then delete
{% endmacro %}


{% macro get_insert_into_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}


{% macro fabricspark__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
  {# need dest_columns for merge_exclude_columns, default to use "*" #}
  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}

  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

  {{ sql_header if sql_header is not none }}

  merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source }} as DBT_INTERNAL_SOURCE
      on {{ predicates | join(' and ') }}

      when matched then update set
        {% if update_columns -%}{%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
        {%- else %} * {% endif %}

      when not matched then insert *
{% endmacro %}


{% macro dbt_spark_get_incremental_sql(strategy, source, target, existing, unique_key, incremental_predicates) %}
  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target, existing) }}
  {%- elif strategy == 'microbatch' -%}
    {#-- microbatch is handled directly in the materialization via separate
         DELETE + INSERT statements (Fabric Spark cannot run them in one query). --#}
    {% set missing_partition_key_microbatch_msg -%}
      dbt-fabricspark 'microbatch' incremental strategy requires a `partition_by` config.
      Ensure you are using a `partition_by` column that is of grain {{ config.get('batch_size') }}.
    {%- endset %}

    {%- if not config.get('partition_by') -%}
      {{ exceptions.raise_compiler_error(missing_partition_key_microbatch_msg) }}
    {%- endif -%}
    {#-- This branch should not be reached; the materialization handles microbatch directly. --#}
    {{ get_insert_into_sql(source, target) }}
  {%- elif strategy == 'merge' -%}
  {#-- merge all columns for datasources which implement MERGE INTO (e.g. databricks, iceberg) - schema changes are handled for us #}
    {{ get_merge_sql(target, source, unique_key, dest_columns=none, incremental_predicates=incremental_predicates) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}

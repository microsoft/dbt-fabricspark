{% macro fabricspark__get_binding_char() %}
  {{ return('?' if target.method == 'odbc' else '%s') }}
{% endmacro %}


{#
  Override the default seed materialization for Fabric Spark.

  The core dbt seed materialization raises a hard error when the target
  relation already exists as a view:
      "Cannot seed to '...', it is a view"

  In Fabric Spark Lakehouse, all tests share a single schema per CI job.
  When `dbt build` runs, it may create a VIEW (e.g. `actual`) in a prior
  test, and a subsequent test may attempt to seed a table with a name that
  collides with an existing view in the same schema. The core macro does
  not allow this, so we override it here to drop the view first and
  proceed with table creation — which is safe because seeds should always
  be tables, never views.
#}
{% materialization seed, adapter='fabricspark' %}

  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set grant_config = config.get('grants') -%}
  {%- set agate_table = load_agate_table() -%}

  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% set create_table_sql = "" %}
  {% if exists_as_view %}
    {#-- Fabric Spark: drop the stale view so we can seed a table in its place --#}
    {{ log("Dropping view " ~ old_relation.render() ~ " to replace with seed table") }}
    {% do adapter.drop_relation(old_relation) %}
    {% set create_table_sql = create_csv_table(model, agate_table) %}
  {% elif exists_as_table %}
    {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table) %}
  {% else %}
    {% set create_table_sql = create_csv_table(model, agate_table) %}
  {% endif %}

  {% set code = 'CREATE' if full_refresh_mode else 'INSERT' %}
  {% set rows_affected = (agate_table.rows | length) %}
  {% set sql = load_csv_rows(model, agate_table) %}

  {% call noop_statement('main', code ~ ' ' ~ rows_affected, code, rows_affected) %}
    {{ get_csv_sql(create_table_sql, sql) }};
  {% endcall %}

  {% set target_relation = this.incorporate(type='table') %}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if full_refresh_mode or not exists_as_table %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro fabricspark__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% if old_relation %}
        {{ adapter.drop_relation(old_relation) }}
    {% endif %}
    {% set sql = create_csv_table(model, agate_table) %}
    {{ return(sql) }}
{% endmacro %}

{% macro calc_batch_size(num_columns) %}
    {#
        Spark SQL  allows for a max of ~11000 parameters in a single statement.
        Check if the max_batch_size fits with the number of columns, otherwise
        reduce the batch size so it fits.
    #}
    {% set max_batch_size = get_batch_size() %}
    {% set calculated_batch = (6000 / num_columns)-1|int %}
    {% set batch_size = [max_batch_size, calculated_batch] | min %}

    {{ return(batch_size) }}
{%  endmacro %}

{% macro fabricspark__get_batch_size() %}
  {{ return(500) }}
{% endmacro %}

{% macro fabricspark__load_csv_rows(model, agate_table) %}

  {% set batch_size = calc_batch_size(agate_table.column_names|length) %}
  {% set column_override = model['config'].get('column_types', {}) %}

  {% set statements = [] %}
  {{ log("Inserting batches of " ~ batch_size ~ " records") }}
  {% for chunk in agate_table.rows | batch(batch_size) %}
      {% set bindings = [] %}

      {% for row in chunk %}
          {% do bindings.extend(row) %}
      {% endfor %}

      {% set sql %}
          insert into {{ this.render() }} values
          {% for row in chunk -%}
              ({%- for col_name in agate_table.column_names -%}
                  {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
                  {%- set type = column_override.get(col_name, inferred_type) -%}
                    cast({{ get_binding_char() }} as {{type}})
                  {%- if not loop.last%},{%- endif %}
              {%- endfor -%})
              {%- if not loop.last%},{%- endif %}
          {%- endfor %}
      {% endset %}

      {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

      {% if loop.index0 == 0 %}
          {% do statements.append(sql) %}
      {% endif %}
  {% endfor %}

  {# Return SQL so we can render it out into the compiled files #}
  {{ return(statements[0]) }}
{% endmacro %}


{% macro fabricspark__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {#-- Ensure the database/schema exists before creating the seed table --#}
  {% do ensure_database_exists(model['schema'], database=model.get('database')) %}

  {#-- Drop any stale catalog entry first. This handles DELTA_METADATA_ABSENT_EXISTING_CATALOG_TABLE
       errors where a table exists in the catalog but its Delta log is missing. --#}
  {% call statement('drop_stale_seed', auto_begin=False) -%}
    drop table if exists {{ this.render() }}
  {%- endcall %}

  {% set sql %}
    create or replace table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
    {{ file_format_clause() }}
    {{ partition_cols(label="partitioned by") }}
    {{ clustered_cols(label="clustered by") }}
    {{ location_clause() }}
    {{ comment_clause() }}
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}

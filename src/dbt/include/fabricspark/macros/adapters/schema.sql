{% macro fabricspark__create_schema(relation) -%}
  {% if adapter.is_lakehouse_schemas_enabled() or adapter.is_local_mode() %}
    {%- call statement('create_schema') -%}
      create database if not exists {{ relation.without_identifier() }}
    {% endcall %}
  {% else %}
    {%- call statement('create_schema') -%}
      select 1
    {% endcall %}
  {% endif %}
{% endmacro %}

{% macro fabricspark__drop_schema(relation) -%}
  {% if adapter.is_lakehouse_schemas_enabled() or adapter.is_local_mode() %}
    {%- call statement('drop_schema') -%}
      drop database if exists {{ relation.without_identifier() }} cascade
    {%- endcall -%}
  {% else %}
    {%- call statement('drop_schema') -%}
      select 1
    {%- endcall -%}
  {% endif %}
{% endmacro %}

{#-- Helper macro to ensure the database exists before creating tables --#}
{% macro fabricspark__ensure_database_exists(schema_name) -%}
  {% if adapter.is_lakehouse_schemas_enabled() or adapter.is_local_mode() %}
    {%- call statement('ensure_database_exists') -%}
      create database if not exists {{ schema_name }}
    {%- endcall -%}
  {% else %}
    {%- call statement('ensure_database_exists') -%}
      select 1
    {%- endcall -%}
  {% endif %}
{% endmacro %}

{% macro ensure_database_exists(schema_name) %}
  {{ return(adapter.dispatch('ensure_database_exists', 'dbt')(schema_name)) }}
{% endmacro %}

{% macro fabricspark__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro fabricspark__generate_database_name(custom_database_name=none, node=none) -%}
  {#-- Return the lakehouse name as the database.
       `database` is init=False on credentials and not in `target`, so use `lakehouse`.
       In non-schema mode, include_policy.database=False excludes it from rendered SQL.
       In schema-enabled mode, include_policy.database=True renders three-part names. --#}
  {% do return(target.lakehouse) %}
{%- endmacro %}

{% macro fabricspark__generate_schema_name(custom_schema_name, node) -%}
  {#-- For non-schema lakehouses, always use the lakehouse name as the schema
       (which maps to the single Spark database).
       For schema-enabled lakehouses, use the default dbt behavior. --#}
  {% if adapter.is_lakehouse_schemas_enabled() or adapter.is_local_mode() %}
    {{ return(generate_schema_name_for_env(custom_schema_name, node)) }}
  {% else %}
    {% do return(target.lakehouse) %}
  {% endif %}
{%- endmacro %}
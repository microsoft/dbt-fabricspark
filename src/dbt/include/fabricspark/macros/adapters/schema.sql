{% macro fabricspark__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create database if not exists {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{% macro fabricspark__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop database if exists {{ relation.without_identifier() }} cascade
  {%- endcall -%}
{% endmacro %}

{#-- Helper macro to ensure the database exists before creating tables --#}
{% macro fabricspark__ensure_database_exists(schema_name) -%}
  {%- call statement('ensure_database_exists') -%}
    create database if not exists {{ schema_name }}
  {%- endcall -%}
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
  {% do return(None) %}
{%- endmacro %}
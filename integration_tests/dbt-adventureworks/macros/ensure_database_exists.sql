{#-- Override adapter's ensure_database_exists for local Spark.
     The adapter's version concatenates database.schema for three-part naming,
     which Spark's Hive catalog does not support. In local mode we simply
     create the schema as a flat Spark database. --#}
{% macro ensure_database_exists(schema_name, database=none) %}
  {%- call statement('ensure_database_exists') -%}
    create database if not exists {{ schema_name }}
  {%- endcall -%}
{% endmacro %}

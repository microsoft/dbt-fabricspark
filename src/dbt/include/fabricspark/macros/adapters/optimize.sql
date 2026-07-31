{#-- Post-build Delta file compaction.

     Every write leaves behind small Parquet files inside the Delta table, and
     small-file fragmentation is the dominant cost of downstream joins.
     ``OPTIMIZE`` compacts them and is a cheap no-op when there is nothing to
     compact, so the adapter runs it after every ``table``, ``incremental`` and
     ``snapshot`` build.

     Opt out, highest precedence first:
       1. ``DBT_FABRICSPARK_SKIP_OPTIMIZE=true`` environment variable
       2. ``{{ config(auto_optimize=false) }}`` on the model
       3. ``auto_optimize: false`` in profiles.yml

     Non-Delta relations are always skipped -- ``OPTIMIZE`` is a Delta command.
--#}

{% macro optimize(relation) %}
  {{ return(adapter.dispatch('optimize', 'dbt')(relation)) }}
{% endmacro %}

{% macro fabricspark__optimize(relation) %}
  {%- if adapter.should_auto_optimize(
           auto_optimize=config.get('auto_optimize'),
           file_format=config.get('file_format'),
           relation_is_delta=relation.is_delta) -%}
    {%- do adapter.run_optimize(get_optimize_sql(relation)) -%}
  {%- endif -%}
{% endmacro %}

{% macro get_optimize_sql(relation) %}
  {{ return(adapter.dispatch('get_optimize_sql', 'dbt')(relation)) }}
{% endmacro %}

{% macro fabricspark__get_optimize_sql(relation) %}
  {%- set sql -%}
    optimize {{ relation.render() }}
  {%- endset -%}
  {{ return(sql) }}
{% endmacro %}

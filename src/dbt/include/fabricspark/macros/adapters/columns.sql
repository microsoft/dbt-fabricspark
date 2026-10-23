{% macro get_columns_in_relation_raw(relation) -%}
  {{ return(adapter.dispatch('get_columns_in_relation_raw', 'dbt')(relation)) }}
{%- endmacro -%}

{% macro fabricspark__get_columns_in_relation_raw(relation) -%}
  {% call statement('get_columns_in_relation_raw', fetch_result=True) %}
      describe table extended {{ relation }}
  {% endcall %}
  {% do return(load_result('get_columns_in_relation_raw').table) %}
{% endmacro %}

{% macro fabricspark__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      describe table extended {{ relation.include(schema=(schema is not none)) }}
  {% endcall %}
  {% do return(load_result('get_columns_in_relation').table) %}
{% endmacro %}

{% macro fabricspark__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} alter column {{ column_name }} type {{ new_column_type }};
  {% endcall %}
{% endmacro %}

{% macro fabricspark__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if remove_columns %}
    {% if relation.is_delta is false %}
      {{ exceptions.raise_compiler_error('Dropping columns is only supported on Delta tables; ' ~ relation ~ ' is not a Delta table.') }}
    {% endif %}
    {% do fabricspark__ensure_delta_column_mapping(relation) %}
  {% endif %}

  {% if add_columns %}
    {% set sql -%}
      alter {{ relation.type }} {{ relation }} add columns
        {% for column in add_columns %}
          {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
        {% endfor %}
    {%- endset -%}
    {% do run_query(sql) %}
  {% endif %}

  {% if remove_columns %}
    {% set sql -%}
      alter {{ relation.type }} {{ relation }} drop columns (
        {% for column in remove_columns %}
          {{ adapter.quote(column.name) }}{{ ',' if not loop.last }}
        {% endfor %}
      )
    {%- endset -%}
    {% do run_query(sql) %}
  {% endif %}

{% endmacro %}

{#- Delta only drops columns when column mapping is on; enabling it upgrades the table protocol, so it is opt-in. -#}
{% macro fabricspark__ensure_delta_column_mapping(relation) %}
  {% set properties = {} %}
  {% for row in run_query('show tblproperties ' ~ relation) %}
    {% do properties.update({row[0]: row[1]}) %}
  {% endfor %}

  {% if properties.get('delta.columnMapping.mode', 'none') | lower in ['name', 'id'] %}
  {% elif config.get('enable_column_mapping_on_drop', false) %}
    {% set upgrades = ["'delta.columnMapping.mode' = 'name'"] %}
    {% if properties.get('delta.minReaderVersion', '1') | int < 2 %}
      {% do upgrades.append("'delta.minReaderVersion' = '2'") %}
    {% endif %}
    {% if properties.get('delta.minWriterVersion', '2') | int < 5 %}
      {% do upgrades.append("'delta.minWriterVersion' = '5'") %}
    {% endif %}
    {% do run_query('alter table ' ~ relation ~ ' set tblproperties (' ~ upgrades | join(', ') ~ ')') %}
  {% else %}
    {{ exceptions.raise_compiler_error(
        'Dropping columns from Delta table ' ~ relation ~ ' requires Delta column mapping. '
        ~ "Set enable_column_mapping_on_drop=true on the model to let dbt set delta.columnMapping.mode = 'name' "
        ~ '(this upgrades the table protocol to at least reader version 2 / writer version 5, so older Delta readers can no longer read it), '
        ~ "or run: ALTER TABLE " ~ relation ~ " SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')"
    ) }}
  {% endif %}
{% endmacro %}

{% macro fabricspark__alter_column_comment(relation, column_dict) %}
  {%- set loaded_relation = load_relation(relation) -%}
  {%- set check_delta = loaded_relation.is_delta if loaded_relation is not none else true -%}
  {% for column_name in column_dict %}
      {% set comment = column_dict[column_name]['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set comment_query %}
        {% if check_delta or check_delta is none %}
          alter table {{ relation }} change column
              {{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }}
              comment '{{ escaped_comment }}';
        {% else %}
          {{ exceptions.raise_compiler_error('Fabric Spark does not support formats other than delta -'~ check_delta) }}
        {% endif %}
      {% endset %}
      {% do run_query(comment_query) %}
    {% endfor %}
{% endmacro %}



{% macro get_column_comment_sql(column_name, column_dict) -%}
  {% if column_name in column_dict and column_dict[column_name]["description"] -%}
    {% set escaped_description = column_dict[column_name]["description"] | replace("'", "\\'") %}
    {% set column_comment_clause = "comment '" ~ escaped_description ~ "'" %}
  {%- endif -%}
  {{ adapter.quote(column_name) }} {{ column_comment_clause }}
{% endmacro %}

{% macro get_persist_docs_column_list(model_columns, query_columns) %}
  {% for column_name in query_columns %}
    {{ get_column_comment_sql(column_name, model_columns) }}
    {{- ", " if not loop.last else "" }}
  {% endfor %}
{% endmacro %}

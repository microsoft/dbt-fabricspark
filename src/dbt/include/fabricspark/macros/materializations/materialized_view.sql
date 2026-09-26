{% macro fabricspark_materialized_view_columns(value, label) %}
  {% do adapter.require_experimental_unstable('materialized_view') %}
  {% if value is none %}
    {{ return([]) }}
  {% endif %}
  {% set columns = [value] if value is string else value %}
  {% if columns is not sequence or columns is mapping %}
    {{ exceptions.raise_compiler_error(label ~ " must be a column name or list of names") }}
  {% endif %}
  {% set quoted = [] %}
  {% for column in columns %}
    {% if column is not string or not column | trim %}
      {{ exceptions.raise_compiler_error(label ~ " requires nonempty column names") }}
    {% endif %}
    {% do quoted.append('`' ~ column | replace('`', '``') ~ '`') %}
  {% endfor %}
  {{ return(quoted) }}
{% endmacro %}

{% materialization materialized_view, adapter='fabricspark' %}
  {% do adapter.require_experimental_unstable('materialized_view') %}
  {% if not adapter.is_session_method() %}
    {{ exceptions.raise_compiler_error("materialized_view requires method: session") }}
  {% endif %}
  {% if model['language'] != 'sql' %}
    {{ exceptions.raise_compiler_error("materialized_view supports SQL models only") }}
  {% endif %}
  {% if flags.FULL_REFRESH or should_full_refresh() %}
    {{ exceptions.raise_compiler_error(
      "materialized_view does not support --full-refresh. "
      ~ "Set on_query_change='rebuild' for an intentional SQL definition change."
    ) }}
  {% endif %}
  {% if config.get('file_format', 'delta') | lower != 'delta' %}
    {{ exceptions.raise_compiler_error("materialized_view requires file_format='delta'") }}
  {% endif %}

  {% set on_query_change = config.get('on_query_change', 'fail') %}
  {% if on_query_change not in ['fail', 'rebuild'] %}
    {{ exceptions.raise_compiler_error("on_query_change must be 'fail' or 'rebuild'") }}
  {% endif %}
  {% set partition_columns = fabricspark_materialized_view_columns(config.get('partition_by'), 'partition_by') %}
  {% set cluster_columns = fabricspark_materialized_view_columns(config.get('cluster_by'), 'cluster_by') %}
  {% if partition_columns and cluster_columns %}
    {{ exceptions.raise_compiler_error("partition_by and cluster_by are mutually exclusive") }}
  {% endif %}

  {% set query_hash_property = 'dbt.fabricspark.materialized_view.query_hash' %}
  {% set query_hash = local_md5(compiled_code) %}
  {% set properties = config.get('tblproperties') %}
  {% if properties is none %}
    {% set properties = {} %}
  {% endif %}
  {% if properties is not mapping %}
    {{ exceptions.raise_compiler_error("tblproperties must be a mapping") }}
  {% endif %}
  {% if query_hash_property in properties %}
    {{ exceptions.raise_compiler_error(query_hash_property ~ " is reserved by materialized_view") }}
  {% endif %}
  {% do properties.update({query_hash_property: query_hash}) %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_cached_relation(target_relation) %}
  {% do ensure_database_exists(target_relation.schema, database=target_relation.database, workspace=target_relation.workspace) %}

  {% set create_sql %}
    CREATE MATERIALIZED VIEW {{ target_relation }}
    USING DELTA
    TBLPROPERTIES (
      {% for key, value in properties | dictsort %}
        '{{ key | replace('\\', '\\\\') | replace("'", "''") }}' =
        '{{ value | string | replace('\\', '\\\\') | replace("'", "''") }}'{% if not loop.last %},{% endif %}
      {% endfor %}
    )
    {% if partition_columns %}
      PARTITIONED BY ({{ partition_columns | join(', ') }})
    {% elif cluster_columns %}
      CLUSTER BY ({{ cluster_columns | join(', ') }})
    {% endif %}
    AS {{ compiled_code }}
  {% endset %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% call statement('ensure_materialized_view') %}
      {{ create_sql | replace('CREATE MATERIALIZED VIEW', 'CREATE MATERIALIZED VIEW IF NOT EXISTS') }}
    {% endcall %}
  {% endif %}

  {% call statement('materialized_view_query_hash', fetch_result=True) %}
    SHOW TBLPROPERTIES {{ target_relation }} ('{{ query_hash_property }}')
  {% endcall %}
  {% set property_result = load_result('materialized_view_query_hash') %}
  {% set property_rows = property_result['data'] if property_result is not none else [] %}
  {% set existing_hash = property_rows[0][1] if property_rows | length == 1 else none %}
  {% if existing_hash != query_hash %}
    {% if on_query_change == 'fail' %}
      {{ exceptions.raise_compiler_error(
        "materialized_view SQL changed or the existing relation is not adapter-managed. "
        ~ "Set on_query_change='rebuild' to replace it explicitly."
      ) }}
    {% endif %}
    {% if existing_relation is none %}
      {% call statement('drop_changed_materialized_view') %}
        DROP MATERIALIZED VIEW {{ target_relation }}
      {% endcall %}
    {% elif existing_hash is none or existing_hash | length != 32 %}
      {% do adapter.drop_relation(existing_relation) %}
    {% else %}
      {% call statement('drop_changed_materialized_view') %}
        DROP MATERIALIZED VIEW {{ target_relation }}
      {% endcall %}
    {% endif %}
    {% call statement('main') %}
      {{ create_sql }}
    {% endcall %}
  {% else %}
    {% call statement('main') %}
      REFRESH MATERIALIZED VIEW {{ target_relation }}
    {% endcall %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

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
  {% set guard_target = none %}
  {% if on_query_change == 'fail' and env_var('DBT_FABRICSPARK_GUARDED_REFRESH', '0') == '1' %}
    {% set guard_sql_target = target_relation.render() %}
    {% set guard_target = adapter.guarded_materialized_view_target(guard_sql_target) %}
  {% endif %}
  {% set use_guarded_refresh = guard_target is not none and adapter.supports_guarded_materialized_view_refresh() %}
  {% set existing_relation = none if use_guarded_refresh else load_cached_relation(target_relation) %}
  {% call adapter.measure_materialized_view_phase('adapter.namespace_ensure') %}
    {% do ensure_database_exists(target_relation.schema, database=target_relation.database, workspace=target_relation.workspace) %}
  {% endcall %}

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

  {% call adapter.measure_materialized_view_phase('adapter.hooks') %}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% endcall %}

  {% set guarded_refreshed = false %}
  {% if use_guarded_refresh %}
    {% call adapter.run_guarded_materialized_view_refresh() %}
      {% call statement('main', fetch_result=True) %}
        REFRESH MATERIALIZED VIEW {{ guard_sql_target }} EXPECT QUERY HASH '{{ query_hash }}' IF EXISTS
      {% endcall %}
    {% endcall %}
    {% set guarded_refreshed = adapter.materialized_view_was_refreshed(
        load_result('main').table, query_hash, guard_target) %}
  {% endif %}

  {% if not guarded_refreshed %}
    {% if existing_relation is none %}
      {% call adapter.measure_materialized_view_phase('adapter.create_guard_statement') %}
        {% call statement('ensure_materialized_view') %}
          {{ create_sql | replace('CREATE MATERIALIZED VIEW', 'CREATE MATERIALIZED VIEW IF NOT EXISTS') }}
        {% endcall %}
      {% endcall %}
    {% endif %}

    {% set existing_hash = adapter.get_materialized_view_query_hash(
      target_relation,
      allow_missing_delta=(existing_relation is not none and on_query_change == 'rebuild')
    ) %}
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
  {% endif %}

  {% call adapter.measure_materialized_view_phase('adapter.hooks') %}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
  {% endcall %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

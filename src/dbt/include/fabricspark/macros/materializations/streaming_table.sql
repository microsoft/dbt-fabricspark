{% macro fabricspark_streaming_columns(value, label) %}
  {% do adapter.require_experimental_unstable('streaming_table') %}
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

{% materialization streaming_table, adapter='fabricspark' %}
  {% do adapter.require_experimental_unstable('streaming_table') %}
  {% set target_relation = this.incorporate(type='table') %}
  {% if model['language'] != 'sql' %}
    {{ exceptions.raise_compiler_error("streaming_table supports SQL models only") }}
  {% endif %}
  {% if flags.FULL_REFRESH or should_full_refresh() %}
    {{ exceptions.raise_compiler_error(
      "streaming_table does not support --full-refresh. A fresh checkpoint reads the initial snapshot; "
      ~ "normal runs resume that checkpoint. For an intentional semantic change on an owned local dev table, "
      ~ "explicitly set on_query_change='rebuild'."
    ) }}
  {% endif %}
  {% if config.get('file_format', 'delta') | lower != 'delta' %}
    {{ exceptions.raise_compiler_error("streaming_table requires file_format='delta'") }}
  {% endif %}
  {% for unsupported in [
    'schedule', 'refresh', 'warehouse_id', 'pipeline', 'pipelines', 'databricks_tags',
    'column_tags', 'liquid_clustered_by', 'clustered_by', 'buckets', 'options',
    'location_root', 'trigger', 'checkpoint_location', 'query_name', 'workspace_name'
  ] %}
    {% if config.get(unsupported) is not none %}
      {{ exceptions.raise_compiler_error(
        "streaming_table does not implement config '" ~ unsupported
        ~ "'. Reader options belong in FROM STREAM ... WITH (...); execution is always AvailableNow."
      ) }}
    {% endif %}
  {% endfor %}
  {% if target_relation.workspace %}
    {{ exceptions.raise_compiler_error("streaming_table does not support Fabric workspace targets") }}
  {% endif %}
  {% set output_mode = config.get('output_mode', 'append') %}
  {% set on_query_change = config.get('on_query_change', 'fail') %}
  {% if output_mode not in ['append', 'complete'] %}
    {{ exceptions.raise_compiler_error("output_mode must be 'append' or 'complete'") }}
  {% endif %}
  {% if on_query_change not in ['fail', 'rebuild'] %}
    {{ exceptions.raise_compiler_error("on_query_change must be 'fail' or 'rebuild'") }}
  {% endif %}
  {% set partition_columns = fabricspark_streaming_columns(config.get('partition_by'), 'partition_by') %}
  {% set cluster_columns = fabricspark_streaming_columns(config.get('cluster_by'), 'cluster_by') %}
  {% if partition_columns and cluster_columns %}
    {{ exceptions.raise_compiler_error("partition_by and cluster_by are mutually exclusive") }}
  {% endif %}
  {% set properties = config.get('tblproperties') %}
  {% if properties is none %}
    {% set properties = {} %}
  {% endif %}
  {% if properties is not mapping %}
    {{ exceptions.raise_compiler_error("tblproperties must be a mapping") }}
  {% endif %}
  {% for key, value in properties.items() %}
    {% if key is not string or not key | trim or value is none or value is mapping or (value is sequence and value is not string) %}
      {{ exceptions.raise_compiler_error("tblproperties requires nonempty string keys and scalar values") }}
    {% endif %}
  {% endfor %}

  {% do ensure_database_exists(target_relation.schema, database=target_relation.database) %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set create_sql %}
    CREATE STREAMING TABLE {{ target_relation }}
    USING DELTA
    {% if partition_columns %}
      PARTITIONED BY ({{ partition_columns | join(', ') }})
    {% elif cluster_columns %}
      CLUSTER BY ({{ cluster_columns | join(', ') }})
    {% endif %}
    {% if properties %}
      TBLPROPERTIES (
        {% for key, value in properties | dictsort %}
          '{{ key | replace('\\', '\\\\') | replace("'", "''") }}' =
          '{{ value | string | replace('\\', '\\\\') | replace("'", "''") }}'{% if not loop.last %},{% endif %}
        {% endfor %}
      )
    {% endif %}
    OPTIONS (
      'trigger' = 'availableNow',
      'outputMode' = '{{ output_mode }}',
      'onQueryChange' = '{{ on_query_change }}'
    )
    AS {{ sql }}
  {% endset %}
  {% do write(create_sql) %}
  {% set response, result_table = adapter.execute_streaming_table(target_relation, create_sql) %}
  {% do store_result('main', response, agate_table=result_table) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

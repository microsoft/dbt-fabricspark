{% materialization incremental, adapter='fabricspark', supported_languages=['sql', 'python'] -%}
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='delta') -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'append' -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {#-- Set vars --#}

  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set language = model['language'] -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = this.incorporate(path = {"identifier": this.identifier ~ '__dbt_tmp'}) -%}

  {#-- For schema-enabled lakehouses we must use a real staging table instead
       of a temp view.  Temp views that reference three-part table names trigger
       REQUIRES_SINGLE_PART_NAMESPACE when used inside INSERT INTO … SELECT.
       For non-schema lakehouses / local mode we keep the original temp view. --#}
  {%- if language == 'sql' and not adapter.is_lakehouse_schemas_enabled() -%}
    {%- set tmp_relation = tmp_relation.include(database=false, schema=false) -%}
  {%- endif -%}

  {#-- Ensure the database/schema exists before creating the table --#}
  {% do ensure_database_exists(target_relation.schema, database=target_relation.database) %}

  {#-- Set Overwrite Mode --#}
  {%- if strategy in ['insert_overwrite', 'microbatch'] and partition_by -%}
    {%- call statement() -%}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {%- endcall -%}
  {%- endif -%}

  {#-- Run pre-hooks --#}
  {{ run_hooks(pre_hooks) }}

  {#-- Incremental run logic --#}
  {%- if existing_relation is none -%}
    {#-- Relation must be created --#}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
    {% do persist_constraints(target_relation, model) %}
  {%- elif existing_relation.is_view or should_full_refresh() -%}
    {#-- Relation must be dropped & recreated --#}
    {#-- Always drop the existing relation so that CREATE TABLE succeeds even when
         the existing table is Delta but file_format is not explicitly configured.
         Skipping the drop and relying on CREATE OR REPLACE TABLE only works when
         target_relation.is_delta is set, which is not guaranteed for `this`. --#}
    {% do adapter.drop_relation(existing_relation) %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
    {% do persist_constraints(target_relation, model) %}
  {%- else -%}
    {#-- Relation must be merged --#}
    {#-- For schema-enabled lakehouses, use a persisted view (not temp view)
         to avoid REQUIRES_SINGLE_PART_NAMESPACE when Spark re-resolves temp
         view references through V2SessionCatalog during DML. --#}
    {%- if adapter.is_lakehouse_schemas_enabled() -%}
      {%- call statement('create_tmp_relation') -%}
        {{ create_view_as(tmp_relation, compiled_code) }}
      {%- endcall -%}
    {%- else -%}
      {%- call statement('create_tmp_relation', language=language) -%}
        {{ create_table_as(True, tmp_relation, compiled_code, language) }}
      {%- endcall -%}
    {%- endif -%}
    {%- do process_schema_changes(on_schema_change, tmp_relation, existing_relation) -%}
    {%- if strategy == 'microbatch' -%}
      {#-- Fabric Spark cannot run multiple statements in one query, so we
           issue DELETE and INSERT as separate calls. --#}
      {%- call statement('microbatch_delete') -%}
        {{ get_microbatch_delete_sql(tmp_relation, target_relation, partition_by) }}
      {%- endcall -%}
      {%- call statement('main') -%}
        {{ get_insert_into_sql(tmp_relation, target_relation) }}
      {%- endcall -%}
    {%- else -%}
      {%- call statement('main') -%}
        {{ dbt_spark_get_incremental_sql(strategy, tmp_relation, target_relation, existing_relation, unique_key, incremental_predicates) }}
      {%- endcall -%}
    {%- endif -%}
    {#-- Drop the staging view after the DML completes so it does not
         appear in catalog/list_relations. --#}
    {% call statement('drop_relation') -%}
      drop view if exists {{ tmp_relation }}
    {%- endcall %}
  {%- endif -%}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

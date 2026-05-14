{% materialization view, adapter='fabricspark' -%}
    {%- set identifier = model['alias'] -%}
    {%- set grant_config = config.get('grants') -%}
    {#-- Cross-workspace 4-part naming: when the model sets `workspace_name`,
         carry it through to the target relation so the rendered DDL emits the
         4-part name and Fabric Livy routes the CREATE VIEW to the correct
         workspace catalog. Mirrors the table materialization. --#}
    {%- set workspace_name = config.get('workspace_name') -%}

    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

    {%- set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database,
        type='view', workspace=workspace_name) -%}

    {#-- Ensure the database/schema exists before creating the view. For
         cross-workspace writes the ``workspace_name`` is forwarded so the
         rendered DDL is workspace-qualified
         (``CREATE DATABASE IF NOT EXISTS \`WS2\`.\`lh\`.\`schema\```). --#}
    {% do ensure_database_exists(schema, database=database, workspace=workspace_name) %}

    {{ run_hooks(pre_hooks) }}

    {#-- If there's a table with the same name, drop it via the fabricspark
         handler (also clears any auto-discovered view metadata). --#}
    {%- if old_relation is not none and old_relation.is_table -%}
      {{ fabricspark__handle_existing_table(should_full_refresh(), old_relation) }}
    {%- endif -%}

    {% call statement('main') -%}
      {{ get_create_view_as_sql(target_relation, sql) }}
    {%- endcall %}

    {% set should_revoke = should_revoke(exists_as_view, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}

{% macro fabricspark__handle_existing_table(full_refresh, old_relation) %}
    {#-- Fabric Spark: drop the table, then also drop as view to clear any
         auto-discovered metadata that lingers after the table drop. --#}
    {{ log("Dropping relation " ~ old_relation.render() ~ " because it is of type " ~ old_relation.type) }}
    {{ adapter.drop_relation(old_relation) }}
    {#-- Also drop as view in case Fabric auto-discovery re-registered it --#}
    {% call statement('drop_view_cleanup', auto_begin=False) -%}
        drop view if exists {{ old_relation.include(database=old_relation.database is not none) }}
    {%- endcall %}
{% endmacro %}

{% materialization view, adapter='fabricspark' -%}
    {#-- Ensure the database/schema exists before creating the view --#}
    {% do ensure_database_exists(model.schema) %}
    {{ return(create_or_replace_view()) }}
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

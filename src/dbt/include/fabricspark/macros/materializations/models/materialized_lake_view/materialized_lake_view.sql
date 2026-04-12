{#-- Materialized Lake View (MLV) materialization for Fabric Spark.

     Creates or replaces a Fabric Materialized Lake View using Spark SQL.
     Supports optional partitioning, constraints, comments, and TBLPROPERTIES.
     Requires either an on-demand refresh or a schedule config — one MUST be
     provided or the model will fail.

     CDF (Change Data Feed) is always enabled on upstream source tables;
     this is a hard requirement, not a user-configurable option.

     Requires:
       - Schema-enabled lakehouse (validated at runtime)
       - Fabric Runtime 1.3+ (local mode is rejected)
       - Source tables must be Delta tables (validated before creation)

     Config options:
       materialized:       'materialized_lake_view'
       database:           Target lakehouse name (for cross-lakehouse writes)
       schema:             Target schema name
       partitioned_by:     List of partition columns (optional)
       mlv_comment:        Description string (optional)
       mlv_constraints:    List of constraint dicts (optional)
                           Each: {"name": str, "expression": str, "on_mismatch": "DROP"|"FAIL"}
       tblproperties:      Dict of key-value pairs (optional)
       mlv_on_demand:      Trigger immediate refresh after creation (default: false)
       mlv_schedule:       Schedule config dict for periodic refresh (optional)

     The target lakehouse ID for MLV API calls is resolved automatically
     from the ``database`` config (lakehouse name) via the Fabric REST API.

     NOTE: At least one of ``mlv_on_demand`` or ``mlv_schedule`` MUST be set.
--#}

{% materialization materialized_lake_view, adapter='fabricspark' -%}
    {%- set identifier = model['alias'] -%}

    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(
            identifier=identifier,
            schema=schema,
            database=database,
            type='materialized_view') -%}

    {#-- Config --#}
    {%- set partitioned_by = config.get('partitioned_by', none) -%}
    {%- set mlv_comment = config.get('mlv_comment', none) -%}
    {%- set mlv_constraints = config.get('mlv_constraints', []) -%}
    {%- set tblproperties = config.get('tblproperties', none) -%}
    {%- set mlv_on_demand = config.get('mlv_on_demand', false) -%}
    {%- set mlv_schedule = config.get('mlv_schedule', none) -%}

    {#-- Resolve the target lakehouse ID from the database (lakehouse name) --#}
    {%- set target_lakehouse_name = database or target.lakehouse -%}
    {%- set mlv_lakehouse_id = adapter.mlv_resolve_lakehouse_id(target_lakehouse_name) -%}

    {#-- =====================================================================
         PRE-FLIGHT VALIDATION
         ===================================================================== --#}

    {#-- 0. Runtime prerequisites (local mode, Spark version, schema-enabled) --#}
    {% do adapter.mlv_validate_prerequisites() %}

    {#-- 1. Either on-demand or schedule MUST be configured --#}
    {% if not mlv_on_demand and mlv_schedule is none %}
        {{ exceptions.raise_compiler_error(
            "Materialized Lake View '" ~ identifier ~ "' requires either 'mlv_on_demand: true' "
            "or an 'mlv_schedule' configuration. At least one must be set for the MLV to be "
            "refreshed after creation."
        ) }}
    {% endif %}

    {#-- 2. Validate upstream tables are Delta format --#}
    {%- set upstream_relations = [] -%}
    {% for node_id in model.depends_on.nodes %}
        {% set upstream = graph.nodes.get(node_id) %}
        {% if upstream and upstream.resource_type in ('model', 'seed') %}
            {% do upstream_relations.append({
                "database": upstream.database,
                "schema": upstream.schema,
                "identifier": upstream.alias or upstream.name
            }) %}
        {% endif %}
    {% endfor %}
    {% do adapter.mlv_validate_delta_sources(upstream_relations) %}

    {#-- =====================================================================
         EXECUTION
         ===================================================================== --#}

    {#-- Ensure the database/schema exists --#}
    {% do ensure_database_exists(model.schema, database=model.database) %}

    {{ run_hooks(pre_hooks) }}

    {#-- Always enable Change Data Feed on upstream source tables --#}
    {% for node_id in model.depends_on.nodes %}
        {% set upstream = graph.nodes.get(node_id) %}
        {% if upstream and upstream.resource_type in ('model', 'seed') %}
            {% set upstream_relation = api.Relation.create(
                database=upstream.database,
                schema=upstream.schema,
                identifier=upstream.alias or upstream.name
            ) %}
            {% call statement('enable_cdf_' ~ loop.index) -%}
                ALTER TABLE {{ upstream_relation }} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
            {%- endcall %}
        {% endif %}
    {% endfor %}

    {#-- Drop existing object if it's a different type (table/view) --#}
    {% if old_relation is not none and old_relation.type.value != 'materializedview' %}
        {{ log("Dropping " ~ old_relation.type ~ " " ~ old_relation.render() ~ " to replace with materialized lake view") }}
        {{ adapter.drop_relation(old_relation) }}
    {% endif %}

    {#-- Build the CREATE OR REPLACE statement --#}
    {%- call statement('main') -%}
        create or replace materialized lake view {{ target_relation }}
        {#-- Constraints --#}
        {% if mlv_constraints %}
        (
            {% for constraint in mlv_constraints %}
                CONSTRAINT {{ constraint.name }} CHECK ({{ constraint.expression }})
                {%- if constraint.on_mismatch is defined %} ON MISMATCH {{ constraint.on_mismatch }}{% endif %}
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        )
        {% endif %}
        {#-- Partitioning --#}
        {% if partitioned_by %}
            PARTITIONED BY ({{ partitioned_by | join(', ') }})
        {% endif %}
        {#-- Comment --#}
        {% if mlv_comment %}
            COMMENT '{{ mlv_comment }}'
        {% endif %}
        {#-- Table properties --#}
        {% if tblproperties %}
            TBLPROPERTIES (
                {% for key, value in tblproperties.items() %}
                    "{{ key }}"="{{ value }}"{% if not loop.last %},{% endif %}
                {% endfor %}
            )
        {% endif %}
        AS
        {{ sql }}
    {%- endcall -%}

    {#-- Post-creation: on-demand refresh and/or schedule (failures are fatal) --#}
    {% if mlv_on_demand %}
        {{ log("Triggering on-demand MLV refresh...") }}
        {% do adapter.mlv_run_on_demand(mlv_lakehouse_id) %}
    {% endif %}

    {% if mlv_schedule %}
        {{ log("Creating/updating MLV refresh schedule...") }}
        {% do adapter.mlv_create_or_update_schedule(mlv_schedule, mlv_lakehouse_id) %}
    {% endif %}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}

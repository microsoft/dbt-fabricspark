{#-- MLV preflight check — runs once at the start of execution (on-run-start).

     Scans the project graph for any models using the
     ``materialized_lake_view`` materialization.  When at least one is found,
     validates:

       1. Not running in local mode (MLV requires Fabric Runtime).
       2. Fabric Runtime is 1.3+ (Apache Spark >= 3.5).
       3. The target lakehouse has schemas enabled.

     If no MLV models are present, this macro is a no-op.
--#}

{% macro fabricspark_mlv_preflight_check() %}
    {#-- Gather all MLV model names from the project graph --#}
    {%- set mlv_models = [] -%}
    {% if graph is defined and graph.nodes is defined %}
        {% for node_id, node in graph.nodes.items() %}
            {% if node.resource_type == 'model'
               and node.config.materialized == 'materialized_lake_view' %}
                {% do mlv_models.append(node.name) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% if mlv_models | length > 0 %}
        {{ log("MLV preflight: found " ~ mlv_models | length ~ " materialized lake view model(s): " ~ mlv_models | join(", "), info=True) }}
        {#-- Run the adapter-level prerequisite checks (local mode, Spark version, schema-enabled) --#}
        {% do adapter.mlv_validate_prerequisites() %}
        {{ log("MLV preflight: all runtime prerequisites validated successfully.", info=True) }}
    {% endif %}
{% endmacro %}

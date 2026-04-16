{% macro fabricspark__location_clause() %}
  {%- set location_root = config.get('location_root', validator=validation.any[basestring]) -%}
  {%- set identifier = model['alias'] -%}
  {%- if location_root is not none and location_root != '' and target.name == 'local-fabric' %}
    location '{{ location_root }}/{{ identifier }}'
  {%- endif %}
{%- endmacro -%}

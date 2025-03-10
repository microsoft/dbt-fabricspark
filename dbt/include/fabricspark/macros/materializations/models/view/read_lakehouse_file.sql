{% macro read_lakehouse_file(file_path, file_format) %}
    {%- set file_full_path = "abfss://{workspaceid}@onelake.dfs.fabric.microsoft.com/{lakehouseid}/{file_path}".format(workspaceid=target.workspaceid,lakehouseid=target.lakehouseid,file_path=file_path) -%}
    {{ log('Query on file: ' ~ file_full_path, info=True) }}
    {{ file_format }}.`{{ file_full_path }}`
{% endmacro %}
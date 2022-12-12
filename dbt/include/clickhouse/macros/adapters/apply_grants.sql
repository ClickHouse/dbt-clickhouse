{% macro clickhouse__get_show_grant_sql(relation) %}
    SELECT access_type as privilege_type, COALESCE(user_name, role_name) as grantee FROM system.grants WHERE table = '{{ relation.name }}'
    AND database = '{{ relation.schema }}'
{%- endmacro %}

{% macro clickhouse__call_dcl_statements(dcl_statement_list) %}
    {% for dcl_statement in dcl_statement_list %}
      {% call statement('dcl') %}
        {{ dcl_statement }};
      {% endcall %}
    {% endfor %}
{% endmacro %}


{% macro clickhouse__get_show_grant_sql(relation) %}
    SELECT toString(access_type) AS privilege_type, COALESCE(user_name, role_name) AS grantee
    FROM system.grants
    WHERE table = '{{ relation.name | replace("\\", "\\\\") | replace("'", "\\'") }}'
      AND database = '{{ relation.schema | replace("\\", "\\\\") | replace("'", "\\'") }}'
    GROUP BY privilege_type, grantee
    HAVING countIf(column IS NULL AND is_partial_revoke = 0) > 0
      AND countIf(is_partial_revoke = 1) = 0
{%- endmacro %}

{% macro on_cluster_clause_dcl(relation) %}
    {%- set cluster = adapter.get_clickhouse_cluster_name() -%}
    {%- set disable_on_cluster = config.get('disable_on_cluster') if config is defined else false -%}
    {%- if cluster is not none and (disable_on_cluster | string | lower) != 'true' %}
      ON CLUSTER {{ cluster }}
    {%- endif %}
{%- endmacro %}

{% macro clickhouse__call_dcl_statements(dcl_statement_list) %}
    {% for dcl_statement in dcl_statement_list %}
      {% call statement('dcl') %}
        {{ dcl_statement }};
      {% endcall %}
    {% endfor %}
{% endmacro %}


{%- macro clickhouse__get_grant_sql(relation, privilege, grantees) -%}
    {%- set quoted = [] -%}
    {%- for grantee in grantees -%}{%- do quoted.append(adapter.quote(grantee)) -%}{%- endfor -%}
    grant {{ on_cluster_clause_dcl(relation) }} {{ privilege }} on {{ relation }} to {{ quoted | join(', ') }}
{%- endmacro -%}

{%- macro clickhouse__get_revoke_sql(relation, privilege, grantees) -%}
    {%- set quoted = [] -%}
    {%- for grantee in grantees -%}{%- do quoted.append(adapter.quote(grantee)) -%}{%- endfor -%}
    revoke {{ on_cluster_clause_dcl(relation) }} {{ privilege }} on {{ relation }} from {{ quoted | join(', ') }}
{%- endmacro -%}

{#
    A (privilege, grantee) pair counts as granted only when system.grants has a
    table-wide grant for it (column IS NULL) and no partial revoke on this table.
    Column-level grants and database-wide grants with this table revoked are left
    out, so dbt grants the table-wide privilege again.
    Keep the text "revoke " (with a trailing space) out of this query: dbt's grants
    tests look for it in the logs to detect REVOKE statements.
#}
{% macro clickhouse__get_show_grant_sql(relation) %}
    SELECT toString(access_type) AS privilege_type, COALESCE(user_name, role_name) AS grantee
    FROM system.grants
    WHERE database = '{{ relation.schema | replace("\\", "\\\\") | replace("'", "\\'") }}'
      AND table = '{{ relation.name | replace("\\", "\\\\") | replace("'", "\\'") }}'
    GROUP BY privilege_type, grantee
    HAVING countIf(column IS NULL AND NOT is_partial_revoke) > 0
       AND countIf(is_partial_revoke) = 0
{%- endmacro %}

{% macro clickhouse__call_dcl_statements(dcl_statement_list) %}
    {% for dcl_statement in dcl_statement_list %}
      {% call statement('dcl') %}
        {{ dcl_statement }};
      {% endcall %}
    {% endfor %}
{% endmacro %}

{#
    ON CLUSTER rule for GRANT/REVOKE. Unlike on_cluster_clause, it does not skip
    Replicated databases: they replicate table DDL but not access entities, so a
    grant without ON CLUSTER would only reach the node dbt is connected to.
#}
{% macro on_cluster_clause_dcl(relation) %}
  {%- set cluster = adapter.get_clickhouse_cluster_name() -%}
  {%- if cluster is not none and (config.get('disable_on_cluster') | string | lower) != 'true' %}
    ON CLUSTER {{ cluster }}
  {%- endif %}
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

{#-
  Create or update a materialized view in ClickHouse.
  This involves creating both the materialized view itself and a
  target table that the materialized view writes to.
-#}
{%- materialization materialized_view, adapter='clickhouse' -%}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set cluster_clause = on_cluster_clause(target_relation) -%}
  {%- set refreshable_clause = refreshable_mv_clause() -%}


  {# look for an existing relation for the target table and create backup relations if necessary #}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set backup_relation = none -%}
  {%- set preexisting_backup_relation = none -%}
  {%- set preexisting_intermediate_relation = none -%}
  {% if existing_relation is not none %}
    {%- set backup_relation_type = existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
    {% if not existing_relation.can_exchange %}
      {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
      {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
    {% endif %}
  {% endif %}

  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- extract the names of the materialized views from the sql
  {% set view_names = modules.re.findall('--(?:\s)?([^:]+):begin', sql) %}

  -- extract the sql for each of the materialized view into a map
  {% set views = {} %}
  {% if view_names %}
    {% for view_name in view_names %}
      {% set view_sql = modules.re.findall('--(?:\s)?' + view_name + ':begin(.*)--(?:\s)?' + view_name + ':end', sql, flags=modules.re.DOTALL)[0] %}
      {%- set _ = views.update({view_name: view_sql}) -%}
    {% endfor %}
  {% else %}
    {%- set _ = views.update({"mv": sql}) -%}
  {% endif %}

  {% if backup_relation is none %}
    {{ log('Creating new materialized view ' + target_relation.name )}}
    {% set catchup_data = config.get("catchup", True) %}
    {{ clickhouse__get_create_materialized_view_as_sql(target_relation, sql, views, catchup_data) }}
  {% elif existing_relation.can_exchange %}
    {{ log('Replacing existing materialized view ' + target_relation.name) }}
    -- in this section, we look for mvs that has the same pattern as this model, but for some reason,
    -- are not listed in the model. This might happen when using multiple mv, and renaming one of the mv in the model.
    -- In case such mv found, we raise a warning to the user, that they might need to drop the mv manually.
    {{ log('Searching for existing materialized views with the pattern of ' + target_relation.name) }}
    {{ log('Views dictionary contents: ' + views | string) }}

        {% set tables_query %}
            select table_name
            from information_schema.tables
            where table_schema = '{{ existing_relation.schema }}'
              and table_name like '%{{ target_relation.name }}%'
              and table_type = 'VIEW'
        {% endset %}

    {% set tables_result = run_query(tables_query) %}
    {% if tables_result is not none and tables_result.columns %}
        {% set tables = tables_result.columns[0].values() %}
        {{ log('Current mvs found in ClickHouse are: ' + tables | join(', ')) }}
        {% set mv_names = [] %}
        {% for key in views.keys() %}
            {% do mv_names.append(target_relation.name ~ "_" ~ key) %}
        {% endfor %}
        {{ log('Model mvs to replace ' + mv_names | string) }}
        {% for table in tables %}
            {% if table not in mv_names %}
                {{ log('Warning - Table "' + table + '" was detected with the same pattern as model name "' + target_relation.name + '" but was not found in this run. In case it is a renamed mv that was previously part of this model, drop it manually (!!!)') }}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ log('No existing mvs found matching the pattern. continuing..', info=True) }}
    {% endif %}
    {% if should_full_refresh() %}
      {{ clickhouse__drop_mvs(target_relation, cluster_clause, views) }}

      {% call statement('main') -%}
        {{ get_create_table_as_sql(False, backup_relation, sql) }}
      {%- endcall %}
      {% do exchange_tables_atomic(backup_relation, existing_relation) %}

      {{ clickhouse__create_mvs(existing_relation, cluster_clause, refreshable_clause, views) }}
    {% else %}
      -- we need to have a 'main' statement
      {% call statement('main') -%}
        select 1
      {%- endcall %}

      -- try to alter view first to replace sql, else drop and create
      {{ clickhouse__update_mvs(target_relation, cluster_clause, refreshable_clause, views) }}

    {% endif %}
  {% else %}
    {{ log('Replacing existing materialized view ' + target_relation.name) }}
    {{ clickhouse__replace_mv(target_relation, existing_relation, intermediate_relation, backup_relation, sql, views) }}
  {% endif %}

  -- cleanup
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% set relations = [target_relation] %}
  {% for view in views %}
    {{ relations.append(target_relation.derivative('_' + view, 'materialized_view')) }}
  {% endfor %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}


{#
  There are two steps to creating a materialized view:
  1. Create a new table based on the SQL in the model
  2. Create a materialized view using the SQL in the model that inserts
  data into the table creating during step 1
#}
{% macro clickhouse__get_create_materialized_view_as_sql(relation, sql, views, catchup=True ) -%}
  {% call statement('main') %}
  {% if catchup == True %}
    {{ get_create_table_as_sql(False, relation, sql) }}
  {% else %}
  {{ log('Catchup data config was set to false, skipping mv-target-table initial insertion ')}}
   {% set has_contract = config.get('contract').enforced %}
    {{ create_table_or_empty(False, relation, sql, has_contract) }}
  {% endif %}
  {% endcall %}
  {%- set cluster_clause = on_cluster_clause(relation) -%}
  {%- set refreshable_clause = refreshable_mv_clause() -%}
  {%- set mv_relation = relation.derivative('_mv', 'materialized_view') -%}
  {{ clickhouse__create_mvs(relation, cluster_clause, refreshable_clause, views) }}
{%- endmacro %}

{% macro clickhouse__drop_mv(mv_relation, cluster_clause)  -%}
  {% call statement('drop existing mv: ' + mv_relation.name) -%}
    drop view if exists {{ mv_relation }} {{ cluster_clause }}
  {% endcall %}
{%- endmacro %}

{% macro clickhouse__create_mv(mv_relation, target_relation, cluster_clause, refreshable_clause, view_sql)  -%}
  {% call statement('create existing mv: ' + mv_relation.name) -%}
    create materialized view if not exists {{ mv_relation }} {{ cluster_clause }}
    {{ refreshable_clause }}
    to {{ target_relation }}
    as {{ view_sql }}
  {% endcall %}
{%- endmacro %}

{% macro clickhouse__modify_mv(mv_relation, cluster_clause, view_sql)  -%}
  {% call statement('modify existing mv: ' + mv_relation.name) -%}
    alter table {{ mv_relation }} {{ cluster_clause }} modify query {{ view_sql }}
  {% endcall %}
{%- endmacro %}

{% macro clickhouse__update_mv(mv_relation, target_relation, cluster_clause, refreshable_clause, view_sql)  -%}
  {% set existing_relation = adapter.get_relation(database=mv_relation.database, schema=mv_relation.schema, identifier=mv_relation.identifier) %}
  {% if existing_relation %}
    {{ clickhouse__modify_mv(mv_relation, cluster_clause, view_sql) }};
  {% else %}
    {{ clickhouse__drop_mv(mv_relation, cluster_clause) }};
    {{ clickhouse__create_mv(mv_relation, target_relation, cluster_clause, refreshable_clause, view_sql) }};
  {% endif %}

{%- endmacro %}

{% macro clickhouse__drop_mvs(target_relation, cluster_clause, views)  -%}
  {% for view in views.keys() %}
    {%- set mv_relation = target_relation.derivative('_' + view, 'materialized_view') -%}
    {{ clickhouse__drop_mv(mv_relation, cluster_clause) }};
  {% endfor %}
{%- endmacro %}

{% macro clickhouse__create_mvs(target_relation, cluster_clause, refreshable_clause, views)  -%}
  {% for view, view_sql in views.items() %}
    {%- set mv_relation = target_relation.derivative('_' + view, 'materialized_view') -%}
    {{ clickhouse__create_mv(mv_relation, target_relation, cluster_clause, refreshable_clause, view_sql) }};
  {% endfor %}
{%- endmacro %}

{% macro clickhouse__update_mvs(target_relation, cluster_clause, refreshable_clause, views)  -%}
  {% for view, view_sql in views.items() %}
    {%- set mv_relation = target_relation.derivative('_' + view, 'materialized_view') -%}
    {{ clickhouse__update_mv(mv_relation, target_relation, cluster_clause, refreshable_clause, view_sql) }};
  {% endfor %}
{%- endmacro %}

{% macro clickhouse__replace_mv(target_relation, existing_relation, intermediate_relation, backup_relation, sql, views) %}
  {# drop existing materialized view while we recreate the target table #}
  {%- set cluster_clause = on_cluster_clause(target_relation) -%}
  {%- set refreshable_clause = refreshable_mv_clause() -%}
  {{ clickhouse__drop_mvs(target_relation, cluster_clause, views) }}

  {# recreate the target table #}
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
  {%- endcall %}
  {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {# now that the target table is recreated, we can finally create our new view #}
  {{ clickhouse__create_mvs(target_relation, cluster_clause, refreshable_clause, views) }}
{% endmacro %}

{% macro refreshable_mv_clause() %}
  {%- if config.get('refreshable') is not none -%}

    {% set refreshable_config = config.get('refreshable') %}
    {% if refreshable_config is not mapping %}
      {% do exceptions.raise_compiler_error(
        "The 'refreshable' configuration must be defined as a dictionary. Please review the docs for more details."
      ) %}
    {% endif %}

    {% set refresh_interval = refreshable_config.get('interval', none) %}
    {% set refresh_randomize = refreshable_config.get('randomize', none) %}
    {% set depends_on = refreshable_config.get('depends_on', none) %}
    {% set depends_on_validation = refreshable_config.get('depends_on_validation', false) %}
    {% set append = refreshable_config.get('append', false) %}

    {% if not refresh_interval %}
      {% do exceptions.raise_compiler_error(
        "The 'refreshable' configuration is defined, but 'interval' is missing. "
        ~ "This is required to create a refreshable materialized view."
      ) %}
    {% endif %}

    {% if refresh_interval %}
      REFRESH {{ refresh_interval }}
      {# This is a comment to force a new line between REFRESH and RANDOMIZE clauses #}
      {%- if refresh_randomize -%}
        RANDOMIZE FOR {{ refresh_randomize }}
      {%- endif -%}
    {% endif %}

    {% if depends_on %}
      {% set depends_on_list = [] %}

      {% if depends_on is string %}
        {% set depends_on_list = [depends_on] %}
      {% elif depends_on is iterable %}
        {% set temp_list = depends_on_list %}
        {%- for dep in depends_on %}
          {% if dep is string %}
            {% do temp_list.append(dep) %}
          {% else %}
            {% do exceptions.raise_compiler_error(
              "The 'depends_on' configuration must be either a string or a list of strings."
            ) %}
          {% endif %}
        {% endfor %}
        {% set depends_on_list = temp_list %}
      {% else %}
        {% do exceptions.raise_compiler_error(
          "The 'depends_on' configuration must be either a string or a list of strings."
        ) %}
      {% endif %}

      {% if depends_on_validation and depends_on_list | length > 0 %}
        {%- for dep in depends_on_list %}
          {% do validate_refreshable_mv_existence(dep) %}
        {%- endfor %}
      {% endif %}

      DEPENDS ON {{ depends_on_list | join(', ') }}
    {% endif %}

    {%- if append -%}
      APPEND
    {%- endif -%}

  {%- endif -%}
{% endmacro %}


{% macro validate_refreshable_mv_existence(mv) %}
  {{ log(mv + ' was recognized as a refreshable mv dependency, checking its existence') }}
  {% set default_database = "default" %}

  {%- set parts = mv.split('.') %}
  {%- if parts | length == 2 %}
    {%- set database = parts[0] %}
    {%- set table = parts[1] %}
  {%- else %}
    {%- set database = default_database %}
    {%- set table = parts[0] %}
  {%- endif %}

  {%- set condition = "database='" + database + "' and view='" + table + "'" %}

  {% set query %}
    select count(*)
    from system.view_refreshes
    where {{ condition }}
  {% endset %}

  {% set tables_result = run_query(query) %}
    {{ log(tables_result.columns[0].values()[0]) }}
  {% if tables_result.columns[0].values()[0] > 0 %}
    {{ log('MV ' + mv + ' exists.') }}
  {% else %}
    {% do exceptions.raise_compiler_error(
      'No existing MV found matching MV: ' + mv
    ) %}
  {% endif %}
{% endmacro %}




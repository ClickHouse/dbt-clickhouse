{# -- Clickhouse has table engine "distributed". It doesn't physically exist in database, but manages queries, addressed to local tables on nodes in cluster. #}
{# -- It means that you can run selects and inserts on cluster, not only on one particular node. #}
{# -- I find it very useful to incorporate this table engine in dbt-clickhouse to run dbt on whole cluster, not just on nodes separately #}

{# -- The main difficulty about making distributed table is that you cannot create table "as" result of select query #}
{# -- Instead the flow is to make local tables (empty, with declaring variables) then to make distributed table linked to all local tables on cluster #}
{# -- But when you create table "as" you don't have to declare variables. #}
{# -- The main problem is to write macro, that would create this section in DDL corresponding to your model's sql query #}

{% materialization distributed_incremental, adapter = 'clickhouse' -%}

    {# -- First we need some helper relations: #}
    {# --  - tmp_relation: in-memory relation to make insert into distributed #}
    {# --  - local_relation: physical tables on cluster #}
    {# --  - wiew_relation: magical temporary relation, that will help us declare wariable #}
    {% set unique_key = config.get('unique_key') %}
    {% set existing_relation = load_relation(this) %}
    {% set target_relation = this.incorporate(type='table') %}
    {% set tmp_relation = make_temp_relation(target_relation) %}
    {% set local_relation = make_local_relation(target_relation) %}
    {% set view_relation = make_view_relation(target_relation) %}
    {%- set full_refresh_mode = (should_full_refresh()) -%}
    
    {# -- Main stuff is almost exactly the same, as in base adapter incremental materialization #}

    {{ do adapter.drop_relation(preexisting_intermediate_relation) }}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {% set trigger_full_refresh = (full_refresh_mode or existing_relation.is_view) %}

    {# -- So here is the flow: we make view out of sql. Database spends almost nothing to create that. #}
    {# -- But after that we have this view in system.columns and system.tables #}
    {# -- Therefore we can pull column names and types out of system tables to paste them into our local and distributed DDL's #}
    {% if existing_relation is none or trigger_full_refresh %}
        {% do adapter.drop_relation(local_relation) %}
        {% do adapter.drop_relation(target_relation) %}
        {% do adapter.drop_relation(view_relation) %}
        {% do run_query(create_view_as(view_relation, sql)) %}
        {% do run_query(create_empty_table(local_relation, view_relation)) %}

        {# -- Don't forget to drop helper view #}
        {% do adapter.drop_relation(view_relation) %}
        {% do run_query(create_distributed_table(target_relation, local_relation)) %}
        {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% else %}
        {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% endif %}

    {# -- And then we have only to run incremental upsert #}
    {# -- Even if it is first time running or full-refresh, we are using incremental upsert, because we cannot use "create table as" #}

    {% call statement("main") %}
        {{ incremental_upsert(tmp_identifier, target_relation, unique_key = unique_key) }}
    {% endcall %}

    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{# -- TODO #}
{# -- Flow to modify schema on changes #}
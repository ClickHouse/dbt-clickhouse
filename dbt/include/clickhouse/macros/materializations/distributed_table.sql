{# -- The main desctiption is in comments to distributed_incremental #}

{% materialization distributed_table, adapter = 'clickhouse' %}

    {% set unique_key = config.get('none') %}
    {% set existing_relation = load_relation(this) %}
    {% set target_relation = this.incorporate(type='table') %}
    {% set tmp_relation = clickhouse__make_temp_relation(target_relation) %}
    {% set local_relation = clickhouse__make_local_relation(target_relation) %}
    {% set view_relation = clickhouse__make_view_relation(target_relation) %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {% do adapter.drop_relation(local_relation) %}
    {% do adapter.drop_relation(target_relation) %}
    {% do adapter.drop_relation(view_relation) %}
    {% do run_query(create_view_as(view_relation, sql)) %}
    {% do run_query(clickhouse__create_empty_table(local_relation, view_relation)) %}
    {% do adapter.drop_relation(view_relation) %}
    {% do run_query(clickhouse__create_distributed_table(target_relation, local_relation)) %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}

    {% call statement("main") %}
        {{ incremental_upsert(tmp_relation, target_relation, unique_key = unique_key) }}
    {% endcall %}

    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
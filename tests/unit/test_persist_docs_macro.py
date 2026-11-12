from types import SimpleNamespace

from tests.unit.macro_harness import SandboxSafeMock


def test_persist_docs_validates_documented_columns_and_dollar_quotes_comments(macros):
    adapter = SandboxSafeMock()
    adapter.get_clickhouse_cluster_name.return_value = None
    adapter.get_columns_in_relation.return_value = [SimpleNamespace(name='id')]
    config = SandboxSafeMock()
    config.persist_relation_docs.return_value = True
    config.persist_column_docs.return_value = True
    validate_doc_columns = SandboxSafeMock()
    run_query = SandboxSafeMock()
    relation = '`my_db`.`my_table`'
    model = {
        'description': "Table description; with 'quotes'",
        'columns': {
            'id': {'description': 'id description; semicolon'},
            'column_that_does_not_exist': {'description': 'documented but absent'},
        },
    }

    macros.call(
        'clickhouse__persist_docs',
        relation,
        model,
        True,
        True,
        context={
            'adapter': adapter,
            'config': config,
            'validate_doc_columns': validate_doc_columns,
            'run_query': run_query,
        },
    )

    validate_doc_columns.assert_called_once_with(relation, model['columns'], ['id'])
    run_query.assert_called_once()
    sql = ' '.join(run_query.call_args.args[0].split())
    assert sql == (
        'alter table `my_db`.`my_table` modify comment '
        "$dbt_comment_literal_block$Table description; with 'quotes'$dbt_comment_literal_block$, "
        'comment column `id` $dbt_comment_literal_block$id description; semicolon$dbt_comment_literal_block$'
    )

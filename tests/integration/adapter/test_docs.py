import pytest
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    ref_sources__schema_yml,
)


class TestBaseDocsGenerate(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=None,
            id_type="Int32",
            text_type="String",
            time_type="DateTime",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
        )


ref_models__schema_yml = """
version: 2

models:
  - name: seed_summary
    description: "{{ doc('seed_summary') }}"
    columns: &summary_columns
      - name: first_name
        description: "{{ doc('summary_first_name') }}"
      - name: ct
        description: "{{ doc('summary_count') }}"
  - name: view_summary
    description: "{{ doc('view_summary') }}"
    columns: *summary_columns

exposures:
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('view_summary')
    owner:
      email: something@example.com
      name: Some name
    description: "{{ doc('notebook_info') }}"
    maturity: medium
    url: http://example.com/notebook/1
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']

"""


ref_models__view_summary_sql = """
{{
  config(
    materialized = "view"
  )
}}

select first_name, ct from {{ref('seed_summary')}}
order by ct asc

"""


ref_models__seed_summary_sql = """
{{
  config(
    materialized = "table"
  )
}}

select first_name, count() as ct from {{ source("my_source", "my_table") }}
group by first_name
order by first_name asc

"""


ref_models__docs_md = """
{% docs seed_summary %}
A summmary table of the seed data
{% enddocs %}

{% docs summary_first_name %}
The first name being summarized
{% enddocs %}

{% docs summary_count %}
The number of instances of the first name
{% enddocs %}

{% docs view_summary %}
A view of the summary of the ephemeral copy of the seed data
{% enddocs %}

{% docs source_info %}
My source
{% enddocs %}

{% docs table_info %}
My table
{% enddocs %}

{% docs column_info %}
An ID field
{% enddocs %}

{% docs notebook_info %}
A description of the complex exposure
{% enddocs %}

"""


def expected_references_catalog(
    project,
    role,
    id_type,
    text_type,
    time_type,
    view_type,
    table_type,
    model_stats,
    bigint_type=None,
    seed_stats=None,
    case=None,
    case_columns=False,
    view_summary_stats=None,
):
    if case is None:

        def case(x):
            return x

    col_case = case if case_columns else lambda x: x

    if seed_stats is None:
        seed_stats = model_stats

    if view_summary_stats is None:
        view_summary_stats = model_stats

    model_database = project.database
    my_schema_name = case(project.test_schema)

    summary_columns = {
        "first_name": {
            "name": "first_name",
            "index": 1,
            "type": text_type,
            "comment": None,
        },
        "ct": {
            "name": "ct",
            "index": 2,
            "type": bigint_type,
            "comment": None,
        },
    }

    seed_columns = {
        "id": {
            "name": col_case("id"),
            "index": 1,
            "type": id_type,
            "comment": None,
        },
        "first_name": {
            "name": col_case("first_name"),
            "index": 2,
            "type": text_type,
            "comment": None,
        },
        "email": {
            "name": col_case("email"),
            "index": 3,
            "type": text_type,
            "comment": None,
        },
        "ip_address": {
            "name": col_case("ip_address"),
            "index": 4,
            "type": text_type,
            "comment": None,
        },
        "updated_at": {
            "name": col_case("updated_at"),
            "index": 5,
            "type": time_type,
            "comment": None,
        },
    }
    return {
        "nodes": {
            "seed.test.seed": {
                "unique_id": "seed.test.seed",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": seed_columns,
            },
            "model.test.seed_summary": {
                "unique_id": "model.test.seed_summary",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": case("seed_summary"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": model_stats,
                "columns": summary_columns,
            },
            "model.test.view_summary": {
                "unique_id": "model.test.view_summary",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": case("view_summary"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": view_summary_stats,
                "columns": summary_columns,
            },
        },
        "sources": {
            "source.test.my_source.my_table": {
                "unique_id": "source.test.my_source.my_table",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": seed_columns,
            },
        },
    }


class TestBaseDocsGenReferences(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return expected_references_catalog(
            project,
            role=None,
            id_type="Int32",
            text_type="String",
            time_type="DateTime",
            bigint_type="UInt64",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": ref_models__schema_yml,
            "sources.yml": ref_sources__schema_yml,
            "seed_summary.sql": ref_models__seed_summary_sql,
            "view_summary.sql": ref_models__view_summary_sql,
            "docs.md": ref_models__docs_md,
        }

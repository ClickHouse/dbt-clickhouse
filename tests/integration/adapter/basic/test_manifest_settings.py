import json
import os
import shutil

import pytest
from dbt.adapters.clickhouse.dbclient import DEDUP_WINDOW_SETTING
from dbt.tests.util import run_dbt

MODEL_SQL = """
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='id',
    settings={'allow_nullable_key': 1}
) }}
select 1 as id
"""


class TestAdapterSettingsInManifest:
    """
    Verify that adapter-injected settings are present in the manifest after `dbt parse`,
    so that parse-manifest and run-manifest agree and `state:modified` does not fire falsely
    on deferred runs when nothing has changed.
    """

    def _read_manifest_json(self, project):
        path = os.path.join(project.project_root, "target", "manifest.json")
        with open(path) as f:
            return json.load(f)

    def _get_model_settings_from_manifest(self, manifest, model_name):
        node = next(n for n in manifest["nodes"].values() if n["name"] == model_name)
        return node["config"].get("settings") or {}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": MODEL_SQL}

    def test_parse_and_run_manifests_agree_on_settings(self, project):
        """manifest.json from dbt parse and dbt run must contain identical model settings.

        We read manifest.json (not partial_parse.msgpack) because that is the enriched
        artifact written after set_macro_resolver fires, and the one dbt uses for --state
        comparisons.
        """
        run_dbt(["parse"])
        parse_manifest = self._read_manifest_json(project)

        run_dbt(["run"])
        run_manifest = self._read_manifest_json(project)

        parse_settings = self._get_model_settings_from_manifest(parse_manifest, "my_model")
        run_settings = self._get_model_settings_from_manifest(run_manifest, "my_model")

        assert DEDUP_WINDOW_SETTING in parse_settings, (
            f"Expected '{DEDUP_WINDOW_SETTING}' to be injected into the parse manifest, "
            f"but got settings={parse_settings!r}."
        )
        assert parse_settings == run_settings, (
            f"Parse manifest settings {parse_settings!r} differ from "
            f"run manifest settings {run_settings!r}. "
            "Adapter-injected settings must be present at parse time to avoid "
            "false state:modified hits on deferred runs."
        )

    def test_no_models_selected_on_second_deferred_run(self, project):
        """A deferred run after an unchanged first run must select zero models."""
        run_dbt(["run"])

        state_dir = os.path.join(project.project_root, "state")
        os.makedirs(state_dir, exist_ok=True)
        shutil.copy(
            os.path.join(project.project_root, "target", "manifest.json"),
            os.path.join(state_dir, "manifest.json"),
        )

        results = run_dbt(["run", "--select", "state:modified+", "--defer", "--state", "state/"])

        assert len(results) == 0, (
            f"Expected 0 models to run (nothing changed), but got {len(results)}: "
            f"{[r.node.name for r in results]}."
        )

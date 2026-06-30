import os
import shutil

import pytest
from dbt.tests.util import get_manifest, run_dbt

MODEL_SQL = """
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='id',
    settings={'allow_nullable_key': 1}
) }}
select 1 as id
"""


class TestAdapterSettingsNotPersistedInManifest:
    """
    Regression test for adapter-injected settings leaking into manifest.json.

    `get_model_settings` is called during `dbt run` to build the DDL SETTINGS clause.
    Before the fix, it passed a direct reference to `model['config']['settings']` into
    `update_model_settings`, which wrote `replicated_deduplication_window='0'` back into
    the live config dict. Because dbt serialises manifest.json after the run, the injected
    key was persisted.

    Commands like `dbt parse` and `dbt ls` never call `get_model_settings`, so they produce
    a clean manifest without the injected key. A subsequent `dbt run --select state:modified+
    --defer --state <prev>` then saw `config_changed` on every affected model even when
    nothing had actually changed.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": MODEL_SQL}

    def test_no_models_selected_on_second_deferred_run(self, project):
        """Verify that a second deferred run selects no models when nothing has changed."""
        # First run: produces target/manifest.json
        run_dbt(["run"])

        # Save the manifest as the state artifact (simulates CI uploading it)
        state_dir = os.path.join(project.project_root, "state")
        os.makedirs(state_dir, exist_ok=True)
        shutil.copy(
            os.path.join(project.project_root, "target", "manifest.json"),
            os.path.join(state_dir, "manifest.json"),
        )

        # Second run with no code changes: nothing should be selected
        results = run_dbt(["run", "--select", "state:modified+", "--defer", "--state", "state/"])

        assert len(results) == 0, (
            f"Expected 0 models to run (nothing changed), but got {len(results)}: "
            f"{[r.node.name for r in results]}. "
            "This indicates adapter-injected settings leaked into the first manifest, "
            "causing a false config_changed on the second run."
        )

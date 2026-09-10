import os
import time
from typing import Any, Callable, Optional, TypedDict

import pytest
import yaml


def below_version(major: int, minor: int = 0, _ch_test_version_value: Optional[str] = None) -> bool:
    """
    >>> below_version(25, _ch_test_version_value='24.8')
    True
    >>> below_version(25, _ch_test_version_value='25.7')
    False
    >>> below_version(25, 7, _ch_test_version_value='25.6')
    True
    >>> below_version(25, 7, _ch_test_version_value='25.7')
    False
    """
    current_version = (
        _ch_test_version_value
        or os.environ.get('DBT_CH_TEST_CH_VERSION', '0.0')
        or '0.0'  # Extra 0.0 to make Mypy happy
    )
    actual_major, actual_minor = current_version.split('.')
    return int(actual_major) < major or (int(actual_major) == major and int(actual_minor) < minor)


retry_config = TypedDict('retry_config', {'max_retries': int, 'delay': float})
DEFAULT_RETRY_CONFIG: retry_config = {
    "max_retries": 20,
    "delay": 0.5,
}


def retry_until_assertion_passes(
    func: Callable[[], Any],
    max_retries: int = DEFAULT_RETRY_CONFIG["max_retries"],
    delay: float = DEFAULT_RETRY_CONFIG["delay"],
) -> Any:
    last_error: Optional[AssertionError] = None
    for attempt in range(max_retries + 1):  # +1 to include the initial attempt
        try:
            return func()
        except AssertionError as e:
            last_error = e
            if attempt < max_retries:  # Don't sleep after the last attempt
                time.sleep(delay)
            continue
    # If we get here, all retries failed
    if last_error:
        raise last_error
    return None


# ---------------------------------------------------------------------------
# Migration of deprecated generic-test yml forms.
#
# Many test fixtures inherited from the dbt-tests-adapter package still use the
# deprecated schema.yml forms: custom test arguments directly under the test
# name, and config keys like `severity` at the test's top level. Python dbt
# only warns about these; dbt core v2 fails the parse (dbt1159). Upstream has
# not migrated its fixtures yet (checked 2026-08-12, dbt-tests-adapter 1.20.0
# and dbt-adapters main), so we rewrite them ourselves. Both engines accept the
# modern `arguments:` / `config:` forms.
# ---------------------------------------------------------------------------

TEST_CONFIG_KEYS = {
    'severity',
    'tags',
    'where',
    'error_if',
    'warn_if',
    'fail_calc',
    'store_failures',
    'limit',
    'enabled',
    'alias',
    'database',
    'schema',
    'meta',
    'group',
}
TEST_RESERVED_KEYS = {'name', 'description', 'config', 'arguments', 'column_name'}


def _migrate_test_entry(entry):
    if not isinstance(entry, dict):
        return entry, False
    out, changed = {}, False
    for test_name, body in entry.items():
        if not isinstance(body, dict):
            out[test_name] = body
            continue
        new_body, args, cfg = {}, {}, {}
        for key, value in body.items():
            if key in TEST_RESERVED_KEYS:
                new_body[key] = value
            elif key in TEST_CONFIG_KEYS:
                cfg[key] = value
            else:
                args[key] = value
        if args:
            new_body.setdefault('arguments', {}).update(args)
            changed = True
        if cfg:
            new_body.setdefault('config', {}).update(cfg)
            changed = True
        out[test_name] = new_body
    return out, changed


# dbt 1.10 deprecations (hard errors in dbt core v2): node-level `docs`,
# `meta` and `tags` belong under `config:`.
_CONFIG_KEYS = ('docs', 'meta', 'tags')


def _migrate_config_keys(node):
    changed = False
    for key in _CONFIG_KEYS:
        if key in node:
            node.setdefault('config', {})[key] = node.pop(key)
            changed = True
    return changed


def _migrate_node(node):
    changed = _migrate_config_keys(node)
    for key in ('tests', 'data_tests'):
        if isinstance(node.get(key), list):
            new_list = []
            for entry in node[key]:
                new_entry, entry_changed = _migrate_test_entry(entry)
                new_list.append(new_entry)
                changed = changed or entry_changed
            node[key] = new_list
    for col in node.get('columns') or []:
        changed = _migrate_node(col) or changed
    for table in node.get('tables') or []:
        changed = _migrate_node(table) or changed
    return changed


def migrate_yml(yml_str):
    """Rewrite deprecated yml (top-level generic-test args, node-level
    docs/meta/tags) into the modern `arguments:` / `config:` form. No-op if
    nothing to migrate."""
    doc = yaml.safe_load(yml_str)
    if not isinstance(doc, dict):
        return yml_str
    changed = False
    for section in ('models', 'seeds', 'snapshots', 'sources'):
        for node in doc.get(section) or []:
            if isinstance(node, dict):
                changed = _migrate_node(node) or changed
    for node in doc.get('exposures') or []:
        if isinstance(node, dict):
            changed = _migrate_config_keys(node) or changed
    return yaml.safe_dump(doc) if changed else yml_str


class MigratedTestArgs:
    """Mixin for test classes inheriting yml fixtures from dbt-tests-adapter:
    intercepts the parent's `models` fixture and migrates deprecated generic-test
    yml forms. Put it first in the base list:

        class TestDateAdd(MigratedTestArgs, BaseDateAdd): ...

    A class that defines its own `models` fixture shadows this one — wrap the
    yml strings with `migrate_yml(...)` there instead."""

    def _migrated_files(self, fixture_name):
        for klass in type(self).__mro__[1:]:
            if klass is MigratedTestArgs:
                continue  # skip our own fixture or we recurse
            fixture_fn = klass.__dict__.get(fixture_name)
            if fixture_fn is not None:
                raw = getattr(fixture_fn, '__wrapped__', fixture_fn)
                base = raw(self)
                return {
                    name: migrate_yml(content)
                    if name.endswith(('.yml', '.yaml')) and isinstance(content, str)
                    else content
                    for name, content in base.items()
                }
        return {}

    @pytest.fixture(scope="class")
    def models(self):
        return self._migrated_files('models')

schema_change_fail_error = """
The source and target schemas on this incremental model are out of sync.
They can be reconciled in several ways:
  - set the `on_schema_change` config to `append_new_columns` or `sync_all_columns`.
  - Re-run the incremental model with `full_refresh: True` to update the target schema.
  - update the schema manually and re-run the process.

Additional troubleshooting context:
   Source columns not in target: {0}
   Target columns not in source: {1}
   New column types: {2}
"""

schema_change_datatype_error = """
The source and target schemas on this incremental model contain different data types.  This is not supported.

   Changed column types: {0}
"""

schema_change_missing_source_error = """
The target schema in on this incremental model contains a column not in the source schema.  This is not supported.

   Source columns not in target: {0}
"""

lw_deletes_not_enabled_error = """
Attempting to apply the configuration `use_lw_deletes` to enable the delete+insert incremental strategy, but
`light weight deletes` are either not available or not enabled on this ClickHouse server.
"""

lw_deletes_not_enabled_warning = """
`light weight deletes` are either not available or not enabled on this ClickHouse server.  This prevents the use
of the delete+insert incremental strategy, which may negatively affect performance for incremental models.
"""

nd_mutations_not_enabled_error = """
Attempting to apply the configuration `use_lw_deletes` to enable the delete+insert incremental strategy, but
the required `allow_nondeterministic_mutations` is not enabled and is `read_only` for this user
"""

nd_mutations_not_enabled_warning = """
The setting `allow_nondeterministic_mutations` is not enabled and is `read_only` for this user` This prevents the use
of `light weight deletes` and therefore the delete+insert incremental strategy.  This may negatively affect performance
for incremental models
"""

schema_change_fail_error = """
The source and target schemas on this {0} model are out of sync.
They can be reconciled in several ways:
  - set the `{1}` config to `append_new_columns` or `sync_all_columns`.
  - Re-run the {0} model with `full_refresh: True` to update the target schema.
  - update the schema manually and re-run the process.

Additional troubleshooting context:
   Source columns not in target: {2}
   Target columns not in source: {3}
   New column types: {4}
"""

schema_change_datatype_error = """
The source and target schemas on this incremental model contain different data types.  This is not supported.

   Changed column types: {0}
"""

schema_change_missing_source_error = """
The target schema in on this incremental model contains a column not in the source schema.  This is not supported.

   Source columns not in target: {0}
"""

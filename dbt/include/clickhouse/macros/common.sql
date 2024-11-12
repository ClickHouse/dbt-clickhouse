{% macro py_write(code, relation) -%}
{{- code -}}
# Generated by dbt-python
def main(read_df, write_df, fal_context=None):
  dbt_context = dbtObj(read_df)
  df = model(dbt_context, fal_context)
  return write_df(
      '{{ relation.quote(False, False, False) }}',
      df
  )
{%- endmacro %}
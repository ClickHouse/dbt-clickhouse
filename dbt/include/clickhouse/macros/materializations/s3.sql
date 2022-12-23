{% macro clickhouse_s3source(config_name='', bucket='', path='', fmt='', structure='',
    aws_access_key_id='', aws_secret_access_key='', compression='') %}
  {% if config_name and not config_name.lower().endswith('s3') %}
    {{ exceptions.raise_compiler_error("S3 configuration should end with 's3'") }}
  {% endif %}
  {% set s3config = config.get(config_name, {}) %}
  {{ adapter.s3source_clause(
    config_name=config_name,
    s3_model_config=s3config,
    bucket=bucket,
    path=path,
    fmt=fmt,
    structure=structure,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    compression=compression) }}
{% endmacro %}

{% macro clickhouse_osssource(config_name='', bucket='', path='', fmt='', structure='',
    oss_access_key_id='', oss_secret_access_key='', compression='') %}
  {% if config_name and not config_name.lower().endswith('oss') %}
    {{ exceptions.raise_compiler_error("OSS configuration should end with 'oss'") }}
  {% endif %}
  {% set ossconfig = config.get(config_name, {}) %}
  {{ adapter.osssource_clause(
    config_name=config_name,
    oss_model_config=ossconfig,
    bucket=bucket,
    path=path,
    fmt=fmt,
    structure=structure,
    oss_access_key_id=oss_access_key_id,
    oss_secret_access_key=oss_secret_access_key,
    compression=compression) }}
{% endmacro %}

{%- macro external_read_format(config, rendered_options) -%}
  {#- The format an external model's files are read back in: the configured
      format, else the one external_write_options picked for the write -#}
  {%- set allowed_formats = ['csv', 'parquet', 'json'] -%}
  {%- set format = config.get('format') -%}
  {%- if format -%}
    {%- if format not in allowed_formats -%}
      {{ exceptions.raise_compiler_error("Invalid format: " ~ format ~ ". Allowed formats are: " ~ allowed_formats | join(', ')) }}
    {%- endif -%}
  {%- else -%}
    {%- set format = rendered_options['format'] | lower -%}
    {%- set format = format if format in allowed_formats else 'parquet' -%}
  {%- endif -%}
  {{ return(format) }}
{%- endmacro -%}

{%- macro external_read_function(read_location, format, config, rendered_options, column_names=none) -%}
  {#- The read_csv/read_json/read_parquet call that reads an external model's
      files back with the model's read options. DuckDB detects compression from
      a lower-case extension only, so the write compression is passed to the
      CSV and JSON readers. A CSV written without a header is read without one,
      under the model's column names when they are known, since the sniffer would
      otherwise name the columns column0, column1, ... or take the first row as
      the header. -#}
  {%- if format == 'json' -%}
    {%- set read_options = config.get('json_read_options', {'auto_detect': True}) -%}
  {%- elif format == 'csv' -%}
    {%- set read_options = config.get('csv_read_options', {'auto_detect': True}) -%}
  {%- else -%}
    {%- set read_options = config.get('parquet_read_options', {'union_by_name': False}) -%}
  {%- endif -%}
  {%- if format != 'parquet' and rendered_options.get('compression') and 'compression' not in read_options -%}
    {%- set read_options = dict(read_options, compression=rendered_options['compression']) -%}
  {%- endif -%}
  {%- set read_names = none -%}
  {%- if format == 'csv' -%}
    {%- set read_keys = read_options.keys() | map('lower') | list -%}
    {%- set headerless_names = adapter.external_csv_read_names(rendered_options, column_names or []) -%}
    {%- if headerless_names is not none and 'header' not in read_keys -%}
      {%- set read_options = dict(read_options, header=false) -%}
      {%- if not ('names' in read_keys or 'column_names' in read_keys or 'columns' in read_keys) -%}
        {%- set read_names = headerless_names -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
  read_{{ format }}('{{ adapter.escape_sql_string(read_location) }}'
  {%- for key, value in read_options.items() -%}
    , {{ key }}=
    {%- if value is string -%}
      '{{ value }}'
    {%- else -%}
      {{ value }}
    {%- endif -%}
  {%- endfor -%}
  {%- if read_names -%}
    , names=[
    {%- for name in read_names -%}
      '{{ adapter.escape_sql_string(name) }}'{{ ', ' if not loop.last }}
    {%- endfor -%}
    ]
  {%- endif -%}
  )
{%- endmacro -%}

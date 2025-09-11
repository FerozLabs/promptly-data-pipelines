{{ 
  config(
    materialized = 'table',
    format = "PARQUET",
    partitioned_by = ["ingestion_cdc_date"],
    location = "s3://iceberg/raw/provider_postgres/",
    schema = "raw",
    tags = ["cdc", "raw", "postgres"],
    incremental_strategy='merge',
    unique_key='provider_id'
  ) 
}}

select
    json_query(_message, 'lax $.payload.after.provider_id') as provider_id,
    json_query(_message, 'lax $.payload.after') as nested_data,
    _timestamp as ingestion_cdc_time,
    date_format(_timestamp, '%Y-%m-%d') as ingestion_cdc_date,
    current_timestamp as ingestion_timestamp
from kafka.default."cdc.public.provider"
where json_query(_message, 'lax $.payload.after') is not null

limit 10

{{ 
  config(
    materialized = "table",
    format = "PARQUET",
    location = "s3://iceberg/raw/care_site_postgres/",
    schema = "raw",
    tags = ["postgres", "raw"]
  ) 
}}

select
    care_site_id,
    care_site_name,
    care_site_source_value
from postgresql.public.care_site

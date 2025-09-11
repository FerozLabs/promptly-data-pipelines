{{ 
  config(
    materialized = "table",
    format = "PARQUET",
    location = "s3://iceberg/curated/provider/",
    schema = "curated",
    tags = ["curated"]
  ) 
}}


with parsed_nested_data as (
    select
            cast(json_query(nested_data, 'lax $.provider_id') as integer) as provider_id,

            regexp_replace(
                cast(json_query(nested_data, 'lax $.provider_name') as varchar(255)),
                '^"|"$',
                ''
            ) as provider_name,

            regexp_replace(
                cast(json_query(nested_data, 'lax $.npi') as varchar(10)),
                '^"|"$',
                ''
            ) as npi,

            regexp_replace(
                cast(json_query(nested_data, 'lax $.specialty') as varchar(10)),
                '^"|"$',
                ''
            ) as specialty_concept_id,

            regexp_replace(
                cast(json_query(nested_data, 'lax $.care_site') as varchar(255)),
                '^"|"$',
                ''
            ) as care_site_name,

            regexp_replace(
                cast(json_query(nested_data, 'lax $.provider_source_value') as varchar(255)),
                '^"|"$',
                ''
            ) as provider_source_value,

            cast(json_query(nested_data, 'lax $.provider_id_source_value') as varchar(255)) as provider_id_source_value
    from {{ ref('raw_provider_postgres') }}
    where
        1=1
        and provider_id is not null
)

select
    a.provider_id,
    a.provider_name,
    a.npi,
    a.specialty_concept_id,
    b.care_site_id,
    a.care_site_name,
    b.care_site_source_value,
    a.provider_source_value,
    a.provider_id_source_value
from parsed_nested_data a
left join {{ ref('raw_care_site_postgres') }} b
on a.care_site_name = b.care_site_name

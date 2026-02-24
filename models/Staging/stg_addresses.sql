{{ config(materialized='view') }}

select
    address_lid,
    address,
    precision_code,
    is_primary_address,
    primary_address_lid,
    geometry,
    created_ts
from {{ source('bronze_lightbox','address') }}

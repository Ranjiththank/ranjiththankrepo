{{ config(materialized='view') }}

select
    address_lid,
    assessment_lid
from {{ source('bronze_lightbox','address_assessment_relation') }}

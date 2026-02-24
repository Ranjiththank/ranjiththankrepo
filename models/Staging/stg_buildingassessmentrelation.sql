{{ config(materialized='view') }}

select
    building_lid,
    assessment_lid
from {{ source('bronze_lightbox','building_assessment_relation') }}

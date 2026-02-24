{{ config(materialized='table') }}

with building as (

    select *
    from {{ ref('stg_buildings') }}
),

building_assessment as (

    select
        b.building_lid,
        b.primary_parcel_lid,
        b.area_sqft,
        b.height_avg_ft,
        b.ground_elevation_avg_ft,
        b.primary_address_lid,
        b.created_ts as building_created_ts,

        a.assessment_lid,
        a.fips_code,
        a.county,
        a.taxarn,
        a.owner_name,
        a.asmt_year

    from building b

    inner join {{ ref('stg_buildingassessmentrelation') }} bar
        on b.building_lid = bar.building_lid

    inner join {{ ref('stg_assessments') }} a
        on bar.assessment_lid = a.assessment_lid

),

assessment_address as (

    select
        ba.*,
        ad.address_lid,
        ad.address,
        ad.precision_code,
        ad.geometry as address_geometry

    from building_assessment ba

    inner join {{ ref('stg_addressassessmentrelation') }} aar
        on ba.assessment_lid = aar.assessment_lid

    inner join {{ ref('stg_addresses') }} ad
        on aar.address_lid = ad.address_lid
)

select *
from assessment_address
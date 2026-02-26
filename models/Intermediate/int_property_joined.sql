

with building as (

    select *
    from {{ ref('stg_buildings') }}
    where building_lid is not null
      and primary_parcel_lid is not null

),

building_assessment as (

    select
        b.building_lid,
        b.primary_parcel_lid,
        b.area_sqft,
        b.primary_address_lid,
        b.created_ts as building_created_ts,
        b.bf_stories_number,
        --year(b.created_ts) as yr_blt,

        a.assessment_lid,
        a.asmt_year,
        a.parcel_apn,
        b.area_sqft as building_sqft,
        null as land_sqft,
        a.units_number,
        year(a.created_ts) as yr_blt,
        a.owner_occupied,
        a.building_quality,
        a.bldg_class,
        a.use_code_std_ctgr_desc_lps,
        a.use_code_std_desc_lps,
        a.use_code_muni_desc,
        a.condo_project_building_name,
        a.business_park_name,
        

    from building b

    inner join {{ ref('stg_buildingassessmentrelation') }} bar
        on b.building_lid = bar.building_lid

    inner join {{ ref('int_assessment_filtered') }} a
        on bar.assessment_lid = a.assessment_lid

),

assessment_address as (

    select
        ba.*,
        ad.address_lid,
        ad.address

    from building_assessment ba

    inner join {{ ref('stg_addressassessmentrelation') }} aar
        on ba.assessment_lid = aar.assessment_lid

    inner join {{ ref('stg_addresses') }} ad
        on aar.address_lid = ad.address_lid
        and ad.is_primary_address = true
)

select *
from assessment_address
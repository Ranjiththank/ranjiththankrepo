{{ config(
    materialized='table',
    alias='PROPERTY_CORE_STAGING',

    post_hook="

    ------------------------------------------------------------------
    -- STEP 1: EXPIRE EXISTING ACTIVE RECORDS WHERE HASH CHANGED
    ------------------------------------------------------------------

    update DEV_GDP_SILVER_DB.PROPERTY.PROPERTY_CORE t
    set 
        GDP_IS_ACTIVE = false,
        GDP_UPDATED_TS = current_timestamp(),
        GDP_UPDATED_BY = current_user()
    from {{ this }} s
    where t.SOURCE_UNIQUE_ID = s.SOURCE_UNIQUE_ID
      and t.GDP_IS_ACTIVE = true
      and t.PROPERTY_CORE_HKEY <> s.PROPERTY_CORE_HKEY
    ;

    ------------------------------------------------------------------
    -- STEP 2: INSERT NEW OR CHANGED RECORDS
    ------------------------------------------------------------------

    insert into DEV_GDP_SILVER_DB.PROPERTY.PROPERTY_CORE (
        PROPERTY_CORE_HKEY,
        SOURCE_PROPERTY_ID,
        SOURCE_UNIQUE_ID,
        REF_PROPERTY_KIND_SKEY,
        REF_PROPERTY_STATUS_SKEY,
        REF_GDP_SOURCE_SYSTEM_SKEY,
        PROPERTY_NAME,
        BUSINESS_PARK_NAME,
        PARCEL_NUMBER,
        GROSS_AREA,
        GROSS_AREA_UOM,
        NET_RENTABLE_AREA,
        NET_RENTABLE_AREA_UOM,
        NUMBER_OF_UNITS,
        TOTAL_FLOORS,
        YEAR_BUILT,
        YEAR_RENOVATED,
        OWNER_OCCUPIED,
        TENANCY_TYPE,
        PROPERTY_CLASS,
        IS_STATISTICAL
    )
    select
        s.PROPERTY_CORE_HKEY,
        s.SOURCE_PROPERTY_ID,
        s.SOURCE_UNIQUE_ID,
        s.REF_PROPERTY_KIND_SKEY,
        s.REF_PROPERTY_STATUS_SKEY,
        s.REF_GDP_SOURCE_SYSTEM_SKEY,
        s.PROPERTY_NAME,
        s.BUSINESS_PARK_NAME,
        s.PARCEL_NUMBER,
        s.GROSS_AREA,
        s.GROSS_AREA_UOM,
        s.NET_RENTABLE_AREA,
        s.NET_RENTABLE_AREA_UOM,
        s.NUMBER_OF_UNITS,
        s.TOTAL_FLOORS,
        s.YEAR_BUILT,
        s.YEAR_RENOVATED,
        s.OWNER_OCCUPIED,
        s.TENANCY_TYPE,
        s.PROPERTY_CLASS,
        s.IS_STATISTICAL
    from {{ this }} s
    left join DEV_GDP_SILVER_DB.PROPERTY.PROPERTY_CORE t
        on s.SOURCE_UNIQUE_ID = t.SOURCE_UNIQUE_ID
       and t.GDP_IS_ACTIVE = true
    where t.SOURCE_UNIQUE_ID is null
       or t.PROPERTY_CORE_HKEY <> s.PROPERTY_CORE_HKEY
    ;

    "
) }}

with base as (

    select *
    from {{ ref('int_property_dedup') }}

),

derived as (

    select
        b.*,
        case
            when use_code_std_ctgr_desc_lps = 'Residential'
                 and use_code_std_desc_lps in (
                     'DUPLEX (2 UNITS, ANY COMBINATION)',
                     'TRIPLEX (3 UNITS, ANY COMBINATION)'
                 )
                then 'BUILDING'
            when use_code_std_ctgr_desc_lps = 'Commercial'
                 and use_code_std_desc_lps = 'OFFICE BUILDING'
                then 'BUILDING'
            else 'LAND'
        end as property_kind_name
    from base b

),

derived_status as (

    select
        d.*,
        case
            when property_kind_name = 'BUILDING'
                then 'EXISTING'
            else null
        end as property_status_name
    from derived d

),

final_select as (

    select

        ------------------------------------------------------------------
        -- HASH KEY (SCD2 change detection)
        ------------------------------------------------------------------
        {{ dbt_utils.generate_surrogate_key([
            'building_lid',
            'yr_blt',
            'units_number',
            'bf_stories_number',
            'owner_occupied',
            'building_quality',
            'bldg_class'
        ]) }} as PROPERTY_CORE_HKEY,

        ------------------------------------------------------------------
        -- SOURCE KEYS
        ------------------------------------------------------------------
        ds.building_lid as SOURCE_PROPERTY_ID,
        concat('LIGHTBOX','-',ds.building_lid) as SOURCE_UNIQUE_ID,

        ------------------------------------------------------------------
        -- FOREIGN KEYS
        ------------------------------------------------------------------
        rpk.property_kind_skey as REF_PROPERTY_KIND_SKEY,
        rps.property_status_skey as REF_PROPERTY_STATUS_SKEY,
        rss.gdp_source_system_skey as REF_GDP_SOURCE_SYSTEM_SKEY,

        ------------------------------------------------------------------
        -- CORE ATTRIBUTES
        ------------------------------------------------------------------
        nullif(trim(condo_project_building_name),'') as PROPERTY_NAME,
        nullif(trim(business_park_name),'') as BUSINESS_PARK_NAME,
        trim(parcel_apn) as PARCEL_NUMBER,

        ------------------------------------------------------------------
        -- PHYSICAL ATTRIBUTES
        ------------------------------------------------------------------
        round(
            case
                when ds.property_kind_name = 'BUILDING' then building_sqft
                when ds.property_kind_name = 'LAND' then land_sqft / 43560
            end
        ,2) as GROSS_AREA,

        case
            when ds.property_kind_name = 'BUILDING' then 'SQFT'
            when ds.property_kind_name = 'LAND' then 'ACRES'
        end as GROSS_AREA_UOM,

        null as NET_RENTABLE_AREA,
        null as NET_RENTABLE_AREA_UOM,

        case
            when units_number >= 0 then units_number
        end as NUMBER_OF_UNITS,

        case
            when ds.property_kind_name = 'BUILDING'
                 and bf_stories_number > 0
                then bf_stories_number
        end as TOTAL_FLOORS,

        yr_blt as YEAR_BUILT,
        null as YEAR_RENOVATED,

        ------------------------------------------------------------------
        -- CLASSIFICATION & FLAGS
        ------------------------------------------------------------------
        case
            when ds.property_kind_name = 'BUILDING'
                then case when owner_occupied = 'Y' then 'Y' else 'N' end
        end as OWNER_OCCUPIED,

        case
            when ds.property_kind_name = 'BUILDING'
                then case
                        when units_number > 1 then 'MULTI_TENANT'
                        else 'SINGLE_TENANT'
                     end
        end as TENANCY_TYPE,

        case
            when building_quality in ('A','B','C') then building_quality
            when bldg_class in ('A','B','C') then bldg_class
        end as PROPERTY_CLASS,

        false as IS_STATISTICAL

    from derived_status ds
    left join {{ ref('stg_ref_property_kind') }} rpk
        on ds.property_kind_name = rpk.property_kind_name
    left join {{ ref('stg_ref_property_status') }} rps
        on ds.property_status_name = rps.property_status_name
    left join {{ ref('stg_ref_gdp_source_system') }} rss
        on rss.gdp_source_system_name = 'LIGHTBOX'

)

select *
from final_select
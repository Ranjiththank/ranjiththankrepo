{{ config(
    materialized='incremental',
    unique_key='source_unique_id',
    alias='Property_Core'
) }}

with base as (

    select *
    from {{ ref('int_property_dedup') }}

),

final as (

    select

       
        DEV_GDP_SILVER_DB.PROPERTY.PROPERTY_CORE_SKEY_SEQ.nextval
            as property_core_skey,

        
        building_lid as source_property_id,

       
        null as ref_property_kind_skey,
        null as ref_property_status_skey,

        
        address as property_name,

        null as business_park_name,

        
        primary_parcel_lid as parcel_number,

        
        area_sqft as gross_area,
        'SF' as gross_area_uom,

        
        null as net_rentable_area,
        null as net_rentable_area_uom,

        null as number_of_units,
        null as number_of_floors,

        
        asmt_year as year_built,

        null as year_renovated,
        null as owner_occupied,
        null as tenancy_type,

        
        md5(
            upper(coalesce(building_lid,'')) ||
            upper(coalesce(primary_parcel_lid,'')) ||
            upper(coalesce(address,'')) ||
            cast(coalesce(area_sqft,0) as varchar) ||
            cast(coalesce(asmt_year,0) as varchar)
        ) as property_core_hkey,

        
        'LIGHTBOX' || building_lid as source_unique_id,

       
        null as ref_gdp_source_system_skey,

        true as gdp_is_active,

        current_timestamp() as gdp_inserted_ts,
        current_user() as gdp_inserted_by,
        current_timestamp() as gdp_updated_ts,
        current_user() as gdp_updated_by

    from base

)

select *
from final

{% if is_incremental() %}
where source_unique_id not in (
    select source_unique_id
    from {{ this }}
    where gdp_is_active = true
)
{% endif %}
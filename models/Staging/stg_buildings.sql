
select
    building_lid,
    area_sqft,
    height_avg_ft,
    ground_elevation_avg_ft,
    primary_address_lid,
    primary_parcel_lid,
    geometry,
    created_ts
from {{ source('bronze_lightbox','building') }}


select
    building_lid,
    area_sqft,
    height_avg_ft,
    ground_elevation_avg_ft,
    primary_address_lid,
    primary_parcel_lid,
    geometry,
    created_ts,
    case
    when area_sqft < 1500 then 1
    when area_sqft between 1500 and 2500 then 2
    else 3
end as bf_stories_number,
from {{ source('bronze_lightbox','building') }}

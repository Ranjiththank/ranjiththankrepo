
select
    assessment_lid,
    fips_code,
    county,
    taxarn,
    owner_name,
    created_ts,
    year(created_ts) as asmt_year
from {{ source('bronze_lightbox','assessment') }}

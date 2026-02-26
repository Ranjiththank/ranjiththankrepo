

with ranked as (

    select
        *,
        row_number() over (
            partition by primary_parcel_lid
            order by
                asmt_year desc,
                building_sqft desc
        ) as rn

    from {{ ref('int_property_joined') }}

)

select
    *

from ranked
where rn = 1
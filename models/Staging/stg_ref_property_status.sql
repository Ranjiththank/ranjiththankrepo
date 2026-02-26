
select
*
from {{ source('bronze_lightbox','REF_PROPERTY_STATUS') }}


select
    *
from {{ source('bronze_lightbox','REF_GDP_SOURCE_SYSTEM') }}

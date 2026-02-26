

with base as (

    select *
    from {{ ref('stg_assessments') }}

)

select *
from base

where
    use_code_std_ctgr_desc_lps != 'Residential'

    OR (

        use_code_std_ctgr_desc_lps = 'Residential'

        AND use_code_std_desc_lps in (

            'CLUSTER HOME (RESIDENTIAL)',
            'CONDOMINIUM UNIT (RESIDENTIAL)',
            'COOPERATIVE UNIT (RESIDENTIAL)',
            'PLANNED UNIT DEVELOPMENT (PUD) (RESIDENTIAL)',
            'RESIDENTIAL COMMON AREA (CONDO/PUD/ETC.)',
            'TIMESHARE (RESIDENTIAL)',
            'ZERO LOT LINE (RESIDENTIAL)',
            'MISC RESIDENTIAL IMPROVEMENT',
            'MODULAR/PRE-FABRICATED HOMES',
            'RESIDENTIAL INCOME (GENERAL) (MULTI-FAMILY)',
            'DUPLEX (2 UNITS, ANY COMBINATION)',
            'TRIPLEX (3 UNITS, ANY COMBINATION)',
            'QUADRUPLEX (4 UNITS, ANY COMBINATION)',
            'APARTMENT HOUSE (5+ UNITS)',
            'APARTMENT HOUSE (100+ UNITS)',
            'GARDEN APT, COURT APT (5+ UNITS)',
            'HIGHRISE APARTMENTS',
            'BOARDING HOUSE, ROOMING HOUSE, APT HOTEL, TRANSIEN',
            'MULTI-FAMILY DWELLINGS',
            'APARTMENTS (GENERIC)',
            'DORMITORY, GROUP QUARTERS (RESIDENTIAL)'

        )

    )

select
    assessment_lid,
    fips_code,
    county,
    taxarn,
    owner_name,
    created_ts,
    year(created_ts) as asmt_year,
    case
    when county in ('Alameda','Contra Costa')
        then 'Residential'
    else 'Commercial'
end as use_code_std_ctgr_desc_lps,

case
    when county = 'Alameda'
        then 'DUPLEX (2 UNITS, ANY COMBINATION)'
    when county = 'Contra Costa'
        then 'TRIPLEX (3 UNITS, ANY COMBINATION)'
    else 'OFFICE BUILDING'
end as use_code_std_desc_lps,

case
    when county = 'Alameda'
        then 'ALAMEDA MUNICIPAL RESIDENTIAL'
    when county = 'Contra Costa'
        then 'CONTRA COSTA MUNICIPAL RESIDENTIAL'
    else 'GENERIC MUNICIPAL'
end as use_code_muni_desc,

case
        when county = 'Alameda'
            then 'Alameda Heights'
        when county = 'Contra Costa'
            then 'Contra Costa Towers'
        when county = 'San Francisco'
            then 'SF Business Center'
        when county = 'Santa Clara'
            then 'Silicon Valley Plaza'
        when county = 'San Mateo'
            then 'Bayfront Corporate Park'
        else 'Generic Property'
    end as condo_project_building_name,

    case
    when county = 'Alameda'
        then 'Alameda Residential Park'
    when county = 'Contra Costa'
        then 'Contra Costa Residential Park'
    when county = 'San Francisco'
        then 'Downtown SF Business Park'
    when county = 'Santa Clara'
        then 'Silicon Valley Tech Park'
    when county = 'San Mateo'
        then 'Bayfront Office Campus'
    else 'Generic Business Campus'
end as business_park_name,


upper(trim(
    case
        when county = 'Alameda'
            then 'ALM-' || assessment_lid
        when county = 'Contra Costa'
            then 'CC-' || assessment_lid
        else 'GEN-' || assessment_lid
    end
)) as parcel_apn,

case
    when county = 'Alameda' then 2
    when county = 'Contra Costa' then 3
    else 1
end as units_number,
case
    when county in ('Alameda','Contra Costa')
        then 'Y'
    else 'N'
end as owner_occupied,
case
    when county = 'Alameda' then 'A'
    when county = 'Contra Costa' then 'B'
    else 'C'
end as building_quality,

case
    when county = 'Alameda' then 'A'
    when county = 'Contra Costa' then 'B'
    else null
end as bldg_class

from {{ source('bronze_lightbox','assessment') }}

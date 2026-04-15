with stg_address as (
    select distinct
        addressid,
        city,
        stateprovinceid
    from {{ ref('address') }}
),

stg_referenced_addresses as (
    select distinct shiptoaddressid as addressid
    from {{ ref('salesorderheader') }}
    where shiptoaddressid is not null
    union
    select distinct billtoaddressid as addressid
    from {{ ref('salesorderheader') }}
    where billtoaddressid is not null
),

stg_all_addresses as (
    select
        r.addressid,
        a.city,
        a.stateprovinceid
    from stg_referenced_addresses r
    left join stg_address a on r.addressid = a.addressid
    union
    select
        addressid,
        city,
        stateprovinceid
    from stg_address
),

stg_stateprovince as (
    select distinct
        stateprovinceid,
        name,
        countryregioncode
    from {{ ref('stateprovince') }}
),

stg_countryregion as (
    select distinct
        countryregioncode,
        name
    from {{ ref('countryregion') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['stg_all_addresses.addressid']) }} as address_key,
    stg_all_addresses.addressid,
    stg_all_addresses.city as city_name,
    stg_stateprovince.name as state_name,
    stg_countryregion.name as country_name
from stg_all_addresses
left join stg_stateprovince on stg_all_addresses.stateprovinceid = stg_stateprovince.stateprovinceid
left join stg_countryregion on stg_stateprovince.countryregioncode = stg_countryregion.countryregioncode

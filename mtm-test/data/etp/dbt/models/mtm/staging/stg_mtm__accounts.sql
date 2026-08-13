-- Staging view over the raw accounts extract. The source table is replaced in full
-- on every run, so no incremental logic is needed here.

with source as (

    select * from {{ source('mtm_raw', 'accounts') }}

),

renamed as (

    select
        account_id,
        trim(account_name)                   as account_name,
        upper(trim(account_status))          as account_status,
        cast(opened_at as timestamp)         as opened_at

    from source

)

select * from renamed

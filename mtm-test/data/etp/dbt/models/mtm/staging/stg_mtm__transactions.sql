-- Staging view over the raw transactions extract: renames to snake_case,
-- casts the cursor field, and drops the ingestion-time technical columns.

with source as (

    select * from {{ source('mtm_raw', 'transactions') }}

),

renamed as (

    select
        transaction_id,
        account_id,
        cast(amount as numeric)              as amount,
        upper(trim(currency))                as currency_code,
        cast(transaction_date as date)       as transaction_date,
        cast(updated_at as timestamp)        as updated_at

    from source

)

select * from renamed

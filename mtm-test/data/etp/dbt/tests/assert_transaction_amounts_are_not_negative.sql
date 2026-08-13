-- Singular test: credits are modelled as separate reversal rows, so a raw transaction
-- amount below zero means the extract mis-parsed the API payload rather than a genuine
-- refund. Returns offending rows; the test fails if any are found.

select
    transaction_id,
    account_id,
    amount,
    transaction_date

from {{ ref('stg_mtm__transactions') }}

where amount < 0

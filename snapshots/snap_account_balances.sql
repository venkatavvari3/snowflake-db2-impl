-- Snapshot for tracking account balance changes
{{ config(
    target_schema='snapshots',
    unique_key='account_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

select 
    account_id,
    customer_id,
    account_type,
    balance,
    is_active,
    opened_date,
    updated_at
from {{ source('raw_banking', 'accounts') }}

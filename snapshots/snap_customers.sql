-- Snapshot for tracking customer changes over time
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

select 
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    address_line_1,
    address_line_2,
    city,
    postal_code,
    country,
    customer_status,
    kyc_status,
    risk_rating,
    updated_at
from {{ source('raw_banking', 'customers') }}

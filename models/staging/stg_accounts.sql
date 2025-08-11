-- Staging model for account data
-- Standardizes account information from core banking system

{{ config(
    materialized='view',
    tags=['staging', 'accounts']
) }}

with source_data as (
    select
        account_id,
        customer_id,
        account_number,
        account_type,
        account_status,
        balance,
        currency,
        opened_date,
        closed_date,
        interest_rate,
        overdraft_limit,
        created_at,
        updated_at
    from {{ source('raw_banking', 'accounts') }}
),

cleaned_data as (
    select
        account_id,
        customer_id,
        
        -- Account type standardization
        upper(trim(account_type)) as account_type,
        
        -- Account identifiers
        account_number,
        
        -- Financial details
        upper(trim(currency)) as currency,
        balance,
        coalesce(overdraft_limit, 0) as overdraft_limit,
        coalesce(interest_rate, 0) as interest_rate,
        
        -- Status and dates
        upper(trim(account_status)) as account_status,
        opened_date,
        closed_date,
        
        -- Calculated fields
        case 
            when balance < 0 then 'OVERDRAWN'
            when balance = 0 then 'ZERO_BALANCE'
            when balance > 0 and balance <= 1000 then 'LOW_BALANCE'
            when balance > 1000 and balance <= 10000 then 'MEDIUM_BALANCE'
            when balance > 10000 then 'HIGH_BALANCE'
        end as balance_tier,
        
        case 
            when overdraft_limit > 0 then true
            else false
        end as has_overdraft,
        
        case 
            when closed_date is not null then datediff('day', opened_date, closed_date)
            else datediff('day', opened_date, current_date())
        end as account_age_days,
        
        case 
            when account_status = 'ACTIVE' and closed_date is null then true
            else false
        end as is_active,
        
        -- Metadata
        created_at,
        updated_at
        
    from source_data
    where customer_id is not null
)

select * from cleaned_data

-- Staging model for transaction data
-- Cleans and categorizes banking transactions

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='append_new_columns',
    tags=['staging', 'transactions', 'incremental']
) }}

with source_data as (
    select
        transaction_id,
        account_id,
        transaction_type,
        amount,
        currency,
        transaction_date,
        description,
        merchant_name,
        merchant_category,
        reference_number,
        balance_after_transaction,
        created_at
        card_last_four,
        authorization_code,
        settlement_date,
        status,
        created_at,
        updated_at,
        _loaded_at
    from {{ source('raw_banking', 'transactions') }}
    
    {% if is_incremental() %}
        -- Only process new transactions
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

cleaned_data as (
    select
        transaction_id,
        account_id,
        
        -- Date and time handling
        transaction_date,
        transaction_time,
        timestamp_from_parts(
            transaction_date, 
            coalesce(transaction_time, time('00:00:00'))
        ) as transaction_timestamp,
        
        -- Transaction details
        upper(trim(transaction_type)) as transaction_type,
        upper(trim(transaction_code)) as transaction_code,
        amount,
        upper(trim(currency)) as currency,
        balance_after,
        trim(description) as description,
        
        -- Merchant information
        trim(merchant_name) as merchant_name,
        upper(trim(merchant_category)) as merchant_category,
        merchant_id,
        
        -- Reference and channel
        reference_number,
        upper(trim(channel)) as channel,
        
        -- Location
        upper(trim(location_country)) as location_country,
        trim(location_city) as location_city,
        
        -- Card details
        upper(trim(card_type)) as card_type,
        card_last_four,
        authorization_code,
        
        -- Settlement and status
        settlement_date,
        upper(trim(status)) as status,
        
        -- Derived fields
        case 
            when amount > 0 then 'CREDIT'
            when amount < 0 then 'DEBIT'
            else 'ZERO'
        end as transaction_direction,
        
        abs(amount) as absolute_amount,
        
        case 
            when abs(amount) >= {{ var('high_value_transaction_threshold') }} then 'HIGH_VALUE'
            when abs(amount) >= 1000 then 'MEDIUM_VALUE'
            when abs(amount) >= 100 then 'LOW_VALUE'
            else 'MICRO'
        end as transaction_value_tier,
        
        -- Transaction categorization
        case 
            when merchant_category in ('GROCERY', 'SUPERMARKET') then 'GROCERIES'
            when merchant_category in ('RESTAURANT', 'CAFE', 'FAST_FOOD') then 'DINING'
            when merchant_category in ('GAS_STATION', 'FUEL') then 'FUEL'
            when merchant_category in ('ATM') then 'ATM'
            when merchant_category in ('BANK_TRANSFER', 'WIRE_TRANSFER') then 'TRANSFER'
            when merchant_category in ('SALARY', 'PAYROLL') then 'INCOME'
            when merchant_category in ('MORTGAGE', 'LOAN', 'CREDIT_CARD') then 'DEBT_PAYMENT'
            when merchant_category in ('INSURANCE') then 'INSURANCE'
            when merchant_category in ('UTILITIES', 'PHONE', 'INTERNET') then 'UTILITIES'
            when merchant_category in ('RETAIL', 'SHOPPING') then 'RETAIL'
            when merchant_category in ('HEALTHCARE', 'MEDICAL') then 'HEALTHCARE'
            when merchant_category in ('TRAVEL', 'HOTEL', 'AIRLINE') then 'TRAVEL'
            when merchant_category in ('ENTERTAINMENT', 'MOVIES', 'SPORTS') then 'ENTERTAINMENT'
            else 'OTHER'
        end as spending_category,
        
        -- Risk flags
        case 
            when location_country != 'GB' then true
            else false
        end as is_international,
        
        case 
            when channel = 'ATM' and time(transaction_time) between time('22:00:00') and time('06:00:00') then true
            else false
        end as is_unusual_hour,
        
        case 
            when amount < 0 and abs(amount) > 5000 then true
            else false
        end as is_large_withdrawal,
        
        -- Metadata
        created_at,
        updated_at,
        _loaded_at
        
    from source_data
    where account_id is not null
      and transaction_date is not null
      and amount is not null
)

select * from cleaned_data

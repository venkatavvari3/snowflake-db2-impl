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
    from {{ source('raw_banking', 'transactions') }}
    
    {% if is_incremental() %}
        -- Only process new transactions based on created_at
        where created_at > (select max(created_at) from {{ this }})
    {% endif %}
),

cleaned_data as (
    select
        transaction_id,
        account_id,
        
        -- Date and time handling
        transaction_date as transaction_timestamp,
        
        -- Transaction details
        upper(trim(transaction_type)) as transaction_type,
        amount,
        upper(trim(currency)) as currency,
        balance_after_transaction,
        trim(description) as description,
        
        -- Merchant information
        trim(merchant_name) as merchant_name,
        upper(trim(merchant_category)) as merchant_category,
        
        -- Reference
        reference_number,
        
        -- Simple derived fields
        case 
            when amount > 0 then 'CREDIT'
            when amount < 0 then 'DEBIT'
            else 'ZERO'
        end as transaction_direction,
        
        abs(amount) as absolute_amount,
        
        -- Metadata
        created_at
        
    from source_data
    where account_id is not null
      and transaction_date is not null
      and amount is not null
)

select * from cleaned_data

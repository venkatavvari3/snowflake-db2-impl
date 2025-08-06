-- Transaction fact table for analytics
-- Provides detailed transaction data optimized for reporting and analysis

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='append_new_columns',
    tags=['marts', 'core', 'transactions', 'fact']
) }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

accounts as (
    select 
        account_id,
        customer_id,
        account_type,
        account_subtype
    from {{ ref('stg_accounts') }}
),

customers as (
    select 
        customer_id,
        customer_segment,
        generation,
        risk_tier
    from {{ ref('dim_customers') }}
)

select
    -- Transaction identifiers
    t.transaction_id,
    t.account_id,
    a.customer_id,
    
    -- Date and time dimensions
    t.transaction_date,
    t.transaction_time,
    t.transaction_timestamp,
    extract(year from t.transaction_date) as transaction_year,
    extract(month from t.transaction_date) as transaction_month,
    extract(quarter from t.transaction_date) as transaction_quarter,
    extract(dayofweek from t.transaction_date) as transaction_day_of_week,
    extract(hour from t.transaction_time) as transaction_hour,
    
    -- Transaction details
    t.transaction_type,
    t.transaction_code,
    t.amount,
    t.absolute_amount,
    t.currency,
    t.balance_after,
    t.description,
    t.transaction_direction,
    t.transaction_value_tier,
    t.spending_category,
    
    -- Merchant information
    t.merchant_name,
    t.merchant_category,
    t.merchant_id,
    
    -- Channel and location
    t.channel,
    t.location_country,
    t.location_city,
    t.is_international,
    
    -- Card information
    t.card_type,
    t.card_last_four,
    
    -- Account context
    a.account_type,
    a.account_subtype,
    
    -- Customer context
    c.customer_segment,
    c.generation,
    c.risk_tier,
    
    -- Risk and fraud indicators
    t.is_unusual_hour,
    t.is_large_withdrawal,
    
    -- Status
    t.status,
    
    -- Additional derived fields for analytics
    case 
        when extract(dayofweek from t.transaction_date) in (1, 7) then 'WEEKEND'
        else 'WEEKDAY'
    end as day_type,
    
    case 
        when extract(hour from t.transaction_time) between 6 and 11 then 'MORNING'
        when extract(hour from t.transaction_time) between 12 and 17 then 'AFTERNOON'  
        when extract(hour from t.transaction_time) between 18 and 21 then 'EVENING'
        else 'NIGHT'
    end as time_of_day,
    
    case 
        when t.transaction_date >= date_trunc('month', current_date()) then 'CURRENT_MONTH'
        when t.transaction_date >= dateadd('month', -1, date_trunc('month', current_date())) then 'LAST_MONTH'
        when t.transaction_date >= dateadd('month', -3, date_trunc('month', current_date())) then 'LAST_3_MONTHS'
        when t.transaction_date >= dateadd('year', -1, current_date()) then 'LAST_YEAR'
        else 'OLDER'
    end as recency_bucket,
    
    -- Metadata
    t.created_at,
    t.updated_at,
    t._loaded_at,
    current_timestamp() as dbt_updated_at

from transactions t
join accounts a on t.account_id = a.account_id
left join customers c on a.customer_id = c.customer_id

-- Core customer dimension table
-- Provides a comprehensive view of customer information for analytics

{{ config(
    materialized='table',
    tags=['marts', 'core', 'customers']
) }}

with customer_base as (
    select * from {{ ref('stg_customers') }}
),

customer_accounts as (
    select
        customer_id,
        count(*) as total_accounts,
        count(case when is_active then 1 end) as active_accounts,
        count(case when account_type = 'CURRENT' then 1 end) as current_accounts,
        count(case when account_type = 'SAVINGS' then 1 end) as savings_accounts,
        count(case when account_type = 'CREDIT' then 1 end) as credit_accounts,
        sum(case when is_active then balance else 0 end) as total_balance,
        max(opened_date) as most_recent_account_date,
        min(opened_date) as first_account_date
    from {{ ref('stg_accounts') }}
    group by customer_id
),

customer_credit as (
    select
        customer_id,
        count(*) as total_credit_products,
        count(case when loan_status = 'ACTIVE' then 1 end) as performing_loans,
        count(case when loan_status != 'ACTIVE' then 1 end) as non_performing_loans,
        sum(outstanding_balance) as total_debt,
        sum(loan_amount) as total_credit_limit,
        avg(case when loan_amount > 0 then outstanding_balance / loan_amount else 0 end) as avg_credit_utilization,
        0 as max_days_past_due  -- Column not available in current data
    from {{ ref('stg_credit_loans') }}
    where loan_status != 'CLOSED'
    group by customer_id
),

customer_transactions as (
    select
        t.account_id,
        a.customer_id,
        count(*) as total_transactions_3m,
        sum(case when t.transaction_direction = 'DEBIT' then abs(t.amount) else 0 end) as total_spending_3m,
        sum(case when t.transaction_direction = 'CREDIT' then t.amount else 0 end) as total_income_3m,
        avg(case when t.transaction_direction = 'DEBIT' then abs(t.amount) end) as avg_transaction_amount,
        0 as international_transactions  -- Column not available in current data
    from {{ ref('stg_transactions') }} t
    join {{ ref('stg_accounts') }} a on t.account_id = a.account_id
    where t.transaction_timestamp >= dateadd('month', -3, current_date())
    group by t.account_id, a.customer_id
),

customer_transaction_summary as (
    select
        customer_id,
        sum(total_transactions_3m) as total_transactions_3m,
        sum(total_spending_3m) as total_spending_3m,
        sum(total_income_3m) as total_income_3m,
        avg(avg_transaction_amount) as avg_transaction_amount,
        sum(international_transactions) as international_transactions_3m
    from customer_transactions
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone_number,
    c.date_of_birth,
    c.age,
    c.address_line_1,
    c.address_line_2,
    c.city,
    c.postal_code,
    c.country,
    c.customer_since,
    c.tenure_years,
    c.customer_status,
    c.risk_rating,
    
    -- Account information
    coalesce(acc.total_accounts, 0) as total_accounts,
    coalesce(acc.active_accounts, 0) as active_accounts,
    coalesce(acc.current_accounts, 0) as current_accounts,
    coalesce(acc.savings_accounts, 0) as savings_accounts,
    coalesce(acc.credit_accounts, 0) as credit_accounts,
    coalesce(acc.total_balance, 0) as total_balance,
    acc.most_recent_account_date,
    acc.first_account_date,
    
    -- Credit information
    coalesce(cr.total_credit_products, 0) as total_credit_products,
    coalesce(cr.performing_loans, 0) as performing_loans,
    coalesce(cr.non_performing_loans, 0) as non_performing_loans,
    coalesce(cr.total_debt, 0) as total_debt,
    coalesce(cr.total_credit_limit, 0) as total_credit_limit,
    coalesce(cr.avg_credit_utilization, 0) as avg_credit_utilization,
    coalesce(cr.max_days_past_due, 0) as max_days_past_due,
    
    -- Transaction behavior (last 3 months)
    coalesce(ts.total_transactions_3m, 0) as total_transactions_3m,
    coalesce(ts.total_spending_3m, 0) as total_spending_3m,
    coalesce(ts.total_income_3m, 0) as total_income_3m,
    coalesce(ts.avg_transaction_amount, 0) as avg_transaction_amount,
    coalesce(ts.international_transactions_3m, 0) as international_transactions_3m,
    
    -- Customer segmentation
    case 
        when acc.total_balance >= 100000 then 'PREMIUM'
        when acc.total_balance >= 50000 then 'AFFLUENT'
        when acc.total_balance >= 10000 then 'MASS_AFFLUENT'
        when acc.total_balance >= 1000 then 'MASS_MARKET'
        else 'BASIC'
    end as customer_segment,
    
    case 
        when c.age < 25 then 'GEN_Z'
        when c.age < 40 then 'MILLENNIAL'
        when c.age < 55 then 'GEN_X'
        when c.age < 70 then 'BABY_BOOMER'
        else 'SILENT_GENERATION'
    end as generation,
    
    case 
        when ts.total_transactions_3m >= 50 then 'HIGH_ACTIVITY'
        when ts.total_transactions_3m >= 20 then 'MEDIUM_ACTIVITY'
        when ts.total_transactions_3m >= 5 then 'LOW_ACTIVITY'
        else 'INACTIVE'
    end as activity_level,
    
    -- Risk indicators
    case 
        when cr.non_performing_loans > 0 or cr.max_days_past_due > 30 then 'HIGH_RISK'
        when cr.avg_credit_utilization > 0.8 or ts.international_transactions_3m > 10 then 'MEDIUM_RISK'
        else 'LOW_RISK'
    end as risk_tier,
    
    -- Metadata
    c.created_at,
    c.updated_at,
    current_timestamp() as dbt_updated_at

from customer_base c
left join customer_accounts acc on c.customer_id = acc.customer_id
left join customer_credit cr on c.customer_id = cr.customer_id  
left join customer_transaction_summary ts on c.customer_id = ts.customer_id

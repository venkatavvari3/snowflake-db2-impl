-- Financial performance metrics and KPIs
-- Provides key financial metrics for management reporting

{{ config(
    materialized='table',
    tags=['marts', 'finance', 'kpis']
) }}

with account_balances as (
    select
        date_trunc('month', current_date()) as report_month,
        sum(case when account_type = 'CURRENT' and is_active then balance else 0 end) as current_account_balances,
        sum(case when account_type = 'SAVINGS' and is_active then balance else 0 end) as savings_account_balances,
        sum(case when is_active then balance else 0 end) as total_deposits,
        count(case when is_active then 1 end) as total_active_accounts
    from {{ ref('stg_accounts') }}
),

loan_portfolio as (
    select
        date_trunc('month', current_date()) as report_month,
        sum(outstanding_balance) as total_loan_portfolio,
        sum(case when loan_status = 'ACTIVE' then outstanding_balance else 0 end) as performing_loans,
        sum(case when loan_status != 'ACTIVE' then outstanding_balance else 0 end) as non_performing_loans,
        0 as severely_delinquent_loans,  -- Column not available in current data
        count(*) as total_loan_accounts,
        avg(interest_rate) as avg_interest_rate
    from {{ ref('stg_credit_loans') }}
    where loan_status != 'CLOSED'
),

transaction_volumes as (
    select
        date_trunc('month', transaction_timestamp) as transaction_month,
        count(*) as transaction_count,
        sum(case when transaction_direction = 'DEBIT' then absolute_amount else 0 end) as total_outflow,
        sum(case when transaction_direction = 'CREDIT' then absolute_amount else 0 end) as total_inflow,
        avg(absolute_amount) as avg_transaction_amount,
        count(distinct account_id) as active_accounts
    from {{ ref('fct_transactions') }}
    where transaction_timestamp >= dateadd('month', -12, current_date())
    group by date_trunc('month', transaction_timestamp)
),

current_month_transactions as (
    select 
        date_trunc('month', current_date()) as report_month,
        count(*) as current_month_transactions,
        sum(case when transaction_direction = 'DEBIT' then absolute_amount else 0 end) as current_month_outflow,
        sum(case when transaction_direction = 'CREDIT' then absolute_amount else 0 end) as current_month_inflow
    from {{ ref('fct_transactions') }}
    where transaction_timestamp >= date_trunc('month', current_date())
),

customer_metrics as (
    select
        date_trunc('month', current_date()) as report_month,
        count(*) as total_customers,
        count(case when customer_status = 'ACTIVE' then 1 end) as active_customers,
        count(case when customer_segment = 'PREMIUM' then 1 end) as premium_customers,
        count(case when customer_segment = 'AFFLUENT' then 1 end) as affluent_customers,
        count(case when risk_tier = 'HIGH_RISK' then 1 end) as high_risk_customers,
        avg(total_balance) as avg_customer_balance,
        avg(tenure_years) as avg_customer_tenure
    from {{ ref('dim_customers') }}
    where customer_status = 'ACTIVE'
)

select
    ab.report_month,
    
    -- Balance sheet metrics
    ab.current_account_balances,
    ab.savings_account_balances,
    ab.total_deposits,
    lp.total_loan_portfolio,
    ab.total_deposits - lp.total_loan_portfolio as net_deposits,
    
    -- Asset quality
    lp.performing_loans,
    lp.non_performing_loans,
    lp.severely_delinquent_loans,
    case 
        when lp.total_loan_portfolio > 0 
        then lp.non_performing_loans / lp.total_loan_portfolio 
        else 0 
    end as npl_ratio,
    case 
        when lp.total_loan_portfolio > 0 
        then lp.severely_delinquent_loans / lp.total_loan_portfolio 
        else 0 
    end as charge_off_ratio,
    
    -- Profitability metrics
    lp.avg_interest_rate,
    case 
        when ab.total_deposits > 0 
        then lp.total_loan_portfolio / ab.total_deposits 
        else 0 
    end as loan_to_deposit_ratio,
    
    -- Activity metrics
    cmt.current_month_transactions,
    cmt.current_month_inflow,
    cmt.current_month_outflow,
    cmt.current_month_inflow - cmt.current_month_outflow as net_flow,
    
    -- Customer metrics
    cm.total_customers,
    cm.active_customers,
    cm.premium_customers,
    cm.affluent_customers,
    cm.high_risk_customers,
    cm.avg_customer_balance,
    cm.avg_customer_tenure,
    
    -- Account metrics
    ab.total_active_accounts,
    lp.total_loan_accounts,
    case 
        when cm.active_customers > 0 
        then ab.total_active_accounts::float / cm.active_customers 
        else 0 
    end as accounts_per_customer,
    
    -- Risk metrics
    case 
        when cm.total_customers > 0 
        then cm.high_risk_customers::float / cm.total_customers 
        else 0 
    end as high_risk_customer_ratio,
    
    current_timestamp() as dbt_updated_at

from account_balances ab
cross join loan_portfolio lp
cross join current_month_transactions cmt
cross join customer_metrics cm

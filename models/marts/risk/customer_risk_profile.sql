-- Risk analytics and monitoring
-- Provides comprehensive risk metrics for credit risk management

{{ config(
    materialized='table',
    tags=['marts', 'risk', 'credit_risk']
) }}

with customer_risk_profile as (
    select
        c.customer_id,
        c.customer_segment,
        c.age,
        c.tenure_years,
        c.total_balance,
        c.total_debt,
        c.total_credit_limit,
        c.avg_credit_utilization,
        c.max_days_past_due,
        c.risk_tier,
        
        -- Calculate debt-to-asset ratio
        case 
            when c.total_balance > 0 
            then c.total_debt / c.total_balance 
            else null 
        end as debt_to_asset_ratio,
        
        -- Recent transaction behavior
        c.total_transactions_3m,
        c.total_spending_3m,
        c.international_transactions_3m
    from {{ ref('dim_customers') }} c
    where c.customer_status = 'ACTIVE'
),

loan_performance as (
    select
        customer_id,
        count(*) as total_credit_products,
        sum(outstanding_balance) as total_exposure,
        sum(case when loan_status != 'ACTIVE' then outstanding_balance else 0 end) as npl_exposure,
        0 as severe_delinquency_exposure,  -- Column not available
        0 as max_dpd,  -- Column not available
        avg(case when loan_amount > 0 then outstanding_balance / loan_amount else 0 end) as avg_utilization,
        sum(case when loan_type = 'MORTGAGE' then outstanding_balance else 0 end) as mortgage_exposure,
        sum(case when loan_type = 'CREDIT' then outstanding_balance else 0 end) as credit_card_exposure,
        sum(case when loan_type = 'PERSONAL' then outstanding_balance else 0 end) as personal_loan_exposure
    from {{ ref('stg_credit_loans') }}
    where loan_status != 'CLOSED'
    group by customer_id
),

transaction_risk_indicators as (
    select
        a.customer_id,
        0 as international_txn_3m,  -- Column not available in current data
        0 as unusual_hour_txn_3m,  -- Column not available in current data
        count(case when t.is_large_withdrawal then 1 end) as large_withdrawal_3m,
        count(case when t.transaction_value_tier = 'HIGH_VALUE' then 1 end) as high_value_txn_3m,
        sum(case when t.transaction_direction = 'DEBIT' and t.spending_category = 'ATM' then t.absolute_amount else 0 end) as atm_withdrawals_3m,
        count(distinct t.location_country) as countries_transacted_3m
    from {{ ref('fct_transactions') }} t
    join {{ ref('stg_accounts') }} a on t.account_id = a.account_id
    where t.transaction_date >= dateadd('month', -3, current_date())
    group by a.customer_id
),

risk_scoring as (
    select
        crp.customer_id,
        crp.customer_segment,
        crp.age,
        crp.tenure_years,
        crp.risk_tier,
        
        -- Financial metrics
        crp.total_balance,
        coalesce(lp.total_exposure, 0) as total_credit_exposure,
        coalesce(lp.npl_exposure, 0) as npl_exposure,
        crp.avg_credit_utilization,
        crp.debt_to_asset_ratio,
        coalesce(lp.max_dpd, 0) as max_days_past_due,
        
        -- Product exposure
        coalesce(lp.mortgage_exposure, 0) as mortgage_exposure,
        coalesce(lp.credit_card_exposure, 0) as credit_card_exposure,
        coalesce(lp.personal_loan_exposure, 0) as personal_loan_exposure,
        
        -- Behavioral risk indicators
        coalesce(tri.international_txn_3m, 0) as international_txn_3m,
        coalesce(tri.unusual_hour_txn_3m, 0) as unusual_hour_txn_3m,
        coalesce(tri.large_withdrawal_3m, 0) as large_withdrawal_3m,
        coalesce(tri.high_value_txn_3m, 0) as high_value_txn_3m,
        coalesce(tri.atm_withdrawals_3m, 0) as atm_withdrawals_3m,
        coalesce(tri.countries_transacted_3m, 1) as countries_transacted_3m,
        
        -- Risk score calculation (0-100 scale)
        (
            -- Credit utilization risk (0-25)
            case 
                when crp.avg_credit_utilization > 0.9 then 25
                when crp.avg_credit_utilization > 0.7 then 20
                when crp.avg_credit_utilization > 0.5 then 15
                when crp.avg_credit_utilization > 0.3 then 10
                else 5
            end +
            
            -- Delinquency risk (0-30)
            case 
                when coalesce(lp.max_dpd, 0) > 90 then 30
                when coalesce(lp.max_dpd, 0) > 60 then 25
                when coalesce(lp.max_dpd, 0) > 30 then 20
                when coalesce(lp.max_dpd, 0) > 0 then 15
                else 0
            end +
            
            -- Debt-to-asset risk (0-20)
            case 
                when crp.debt_to_asset_ratio > 0.8 then 20
                when crp.debt_to_asset_ratio > 0.6 then 15
                when crp.debt_to_asset_ratio > 0.4 then 10
                when crp.debt_to_asset_ratio > 0.2 then 5
                else 0
            end +
            
            -- Behavioral risk (0-15)
            case 
                when coalesce(tri.international_txn_3m, 0) > 10 then 5 else 0
            end +
            case 
                when coalesce(tri.unusual_hour_txn_3m, 0) > 5 then 3 else 0
            end +
            case 
                when coalesce(tri.large_withdrawal_3m, 0) > 2 then 4 else 0
            end +
            case 
                when coalesce(tri.countries_transacted_3m, 1) > 3 then 3 else 0
            end +
            
            -- Age and tenure risk (0-10)
            case 
                when crp.age < 25 and crp.tenure_years < 1 then 10
                when crp.age < 30 and crp.tenure_years < 2 then 7
                when crp.tenure_years < 1 then 5
                else 0
            end
        ) as calculated_risk_score
        
    from customer_risk_profile crp
    left join loan_performance lp on crp.customer_id = lp.customer_id
    left join transaction_risk_indicators tri on crp.customer_id = tri.customer_id
)

select
    customer_id,
    customer_segment,
    age,
    tenure_years,
    risk_tier,
    
    -- Financial position
    total_balance,
    total_credit_exposure,
    npl_exposure,
    avg_credit_utilization,
    debt_to_asset_ratio,
    max_days_past_due,
    
    -- Product breakdown
    mortgage_exposure,
    credit_card_exposure,
    personal_loan_exposure,
    
    -- Behavioral indicators
    international_txn_3m,
    unusual_hour_txn_3m,
    large_withdrawal_3m,
    high_value_txn_3m,
    atm_withdrawals_3m,
    countries_transacted_3m,
    
    -- Risk score and classification
    calculated_risk_score,
    
    case 
        when calculated_risk_score >= 70 then 'VERY_HIGH'
        when calculated_risk_score >= 50 then 'HIGH'
        when calculated_risk_score >= 30 then 'MEDIUM'
        when calculated_risk_score >= 15 then 'LOW'
        else 'VERY_LOW'
    end as calculated_risk_tier,
    
    case 
        when npl_exposure > 0 then 'EXISTING_NPL'
        when calculated_risk_score >= 70 then 'IMMEDIATE_ATTENTION'
        when calculated_risk_score >= 50 then 'ENHANCED_MONITORING'
        when calculated_risk_score >= 30 then 'STANDARD_MONITORING'
        else 'LOW_RISK'
    end as monitoring_category,
    
    -- Risk flags
    case when avg_credit_utilization > 0.8 then true else false end as high_utilization_flag,
    case when max_days_past_due > 30 then true else false end as delinquent_flag,
    case when international_txn_3m > 10 then true else false end as high_international_activity_flag,
    case when debt_to_asset_ratio > 0.6 then true else false end as high_leverage_flag,
    
    current_timestamp() as dbt_updated_at

from risk_scoring

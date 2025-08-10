-- Staging model for credit and loan data
-- Standardizes credit products, loans, and payment information

{{ config(
    materialized='view',
    tags=['staging', 'credit', 'loans']
) }}

with source_data as (
    select
        loan_id,
        customer_id,
        loan_type,
        loan_amount,
        outstanding_balance,
        interest_rate,
        loan_term_months,
        monthly_payment,
        start_date,
        maturity_date,
        loan_status,
        collateral_type,
        collateral_value,
        created_at,
        updated_at
        payment_due_date,
        days_past_due,
        default_flag,
        charge_off_flag,
        loan_status,
        risk_grade,
        ltv_ratio,
        debt_to_income,
        created_at,
        updated_at,
        _loaded_at
    from {{ source('raw_banking', 'credit_loans') }}
),

cleaned_data as (
    select
        loan_id,
        customer_id,
        account_id,
        
        -- Product information
        upper(trim(product_type)) as product_type,
        upper(trim(product_subtype)) as product_subtype,
        
        -- Financial details
        loan_amount,
        outstanding_balance,
        upper(trim(currency)) as currency,
        interest_rate,
        term_months,
        monthly_payment,
        
        -- Payment information
        next_payment_date,
        upper(trim(payment_status)) as payment_status,
        minimum_payment,
        payment_due_date,
        coalesce(days_past_due, 0) as days_past_due,
        
        -- Dates
        origination_date,
        maturity_date,
        
        -- Collateral
        upper(trim(collateral_type)) as collateral_type,
        collateral_value,
        
        -- Credit information
        credit_limit,
        available_credit,
        
        -- Risk indicators
        coalesce(default_flag, false) as default_flag,
        coalesce(charge_off_flag, false) as charge_off_flag,
        upper(trim(loan_status)) as loan_status,
        upper(trim(risk_grade)) as risk_grade,
        ltv_ratio,
        debt_to_income,
        
        -- Calculated fields
        case 
            when outstanding_balance > 0 and loan_amount > 0 
            then outstanding_balance / loan_amount
            else 0
        end as remaining_balance_ratio,
        
        case 
            when credit_limit > 0 and available_credit >= 0
            then (credit_limit - available_credit) / credit_limit
            else 0
        end as credit_utilization,
        
        case 
            when maturity_date > current_date()
            then datediff('month', current_date(), maturity_date)
            else 0
        end as months_to_maturity,
        
        case 
            when origination_date <= current_date()
            then datediff('month', origination_date, current_date())
            else 0
        end as loan_age_months,
        
        -- Risk categorization
        case 
            when days_past_due = 0 then 'CURRENT'
            when days_past_due between 1 and 30 then '1-30_DPD'
            when days_past_due between 31 and 60 then '31-60_DPD'
            when days_past_due between 61 and 90 then '61-90_DPD'
            when days_past_due > 90 then '90+_DPD'
        end as delinquency_bucket,
        
        case 
            when product_type = 'CREDIT_CARD' and credit_utilization > 0.8 then 'HIGH_UTILIZATION'
            when product_type = 'CREDIT_CARD' and credit_utilization > 0.5 then 'MEDIUM_UTILIZATION'
            when product_type = 'CREDIT_CARD' and credit_utilization > 0.3 then 'LOW_UTILIZATION'
            when product_type = 'CREDIT_CARD' then 'MINIMAL_UTILIZATION'
            else 'N/A'
        end as utilization_tier,
        
        case 
            when loan_status in ('CLOSED', 'PAID_OFF') then 'CLOSED'
            when loan_status in ('CURRENT', 'ACTIVE') and days_past_due = 0 then 'PERFORMING'
            when loan_status = 'ACTIVE' and days_past_due > 0 then 'NON_PERFORMING'
            when loan_status in ('DEFAULT', 'CHARGE_OFF') then 'DEFAULTED'
            else 'OTHER'
        end as performance_status,
        
        -- Metadata
        created_at,
        updated_at,
        _loaded_at
        
    from source_data
    where customer_id is not null
)

select * from cleaned_data

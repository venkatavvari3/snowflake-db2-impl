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
    from {{ source('raw_banking', 'credit_loans') }}
),

cleaned_data as (
    select
        loan_id,
        customer_id,
        
        -- Loan details
        upper(trim(loan_type)) as loan_type,
        loan_amount,
        outstanding_balance,
        interest_rate,
        loan_term_months,
        monthly_payment,
        
        -- Dates
        start_date as origination_date,
        maturity_date,
        
        -- Status
        upper(trim(loan_status)) as loan_status,
        
        -- Collateral
        upper(trim(collateral_type)) as collateral_type,
        collateral_value,
        
        -- Simple calculated fields
        case 
            when maturity_date > current_date()
            then datediff('month', current_date(), maturity_date)
            else 0
        end as months_to_maturity,
        
        case 
            when start_date <= current_date()
            then datediff('month', start_date, current_date())
            else 0
        end as loan_age_months,
        
        -- Metadata
        created_at,
        updated_at
        
    from source_data
    where customer_id is not null
)

select * from cleaned_data

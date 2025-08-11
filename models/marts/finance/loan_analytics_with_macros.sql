-- Example of using macros in a real model
-- This shows how to use your business_days_between macro

{{ config(
    materialized='view',
    tags=['marts', 'finance', 'macros_example']
) }}

select
    loan_id,
    customer_id,
    loan_type,
    loan_amount,
    outstanding_balance,
    
    -- Using the business_days_between macro with available columns
    {{ business_days_between('created_at::date', 'current_date()') }} as loan_age_business_days,
    
    -- Using the mask_pii macro for demonstration
    {{ mask_pii('loan_id', 'LOAN-XXXXX') }} as masked_loan_id,
    
    -- Regular calculations for comparison
    datediff('day', created_at::date, current_date()) as loan_age_calendar_days,
    
    -- Business logic using the macro results
    case 
        when {{ business_days_between('created_at::date', 'current_date()') }} > 730 then 'Long Term'   -- 2 years in business days  
        when {{ business_days_between('created_at::date', 'current_date()') }} > 365 then 'Medium Term' -- 1 year in business days
        else 'Short Term'
    end as loan_tenure_category,
    
    created_at,
    updated_at
    
from {{ ref('stg_credit_loans') }}
where loan_status = 'ACTIVE'

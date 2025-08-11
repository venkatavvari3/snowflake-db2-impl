-- Staging model for KYC and AML data
-- Standardizes compliance and regulatory data

{{ config(
    materialized='view',
    tags=['staging', 'kyc', 'aml', 'compliance']
) }}

with source_data as (
    select
        kyc_id,
        customer_id,
        kyc_status,
        kyc_completion_date,
        aml_risk_score,
        aml_risk_level,
        last_review_date,
        next_review_date,
        document_verification_status,
        identity_verification_status,
        address_verification_status,
        source_of_funds_verified,
        politically_exposed_person,
        sanctions_screening_status,
        enhanced_due_diligence_required,
        created_at,
        updated_at
    from {{ source('raw_banking', 'kyc_aml') }}
),

cleaned_data as (
    select
        kyc_id,
        customer_id,
        
        -- KYC Status
        upper(trim(kyc_status)) as kyc_status,
        kyc_completion_date,
        
        -- AML Risk Assessment
        aml_risk_score,
        upper(trim(aml_risk_level)) as aml_risk_level,
        
        -- Review dates
        last_review_date,
        next_review_date,
        
        -- Verification status
        upper(trim(document_verification_status)) as document_verification_status,
        upper(trim(identity_verification_status)) as identity_verification_status,
        upper(trim(address_verification_status)) as address_verification_status,
        
        -- Source of funds
        coalesce(source_of_funds_verified, false) as source_of_funds_verified,
        
        -- Risk flags
        coalesce(politically_exposed_person, false) as pep_flag,
        upper(trim(sanctions_screening_status)) as sanctions_screening_status,
        coalesce(enhanced_due_diligence_required, false) as enhanced_due_diligence_required,
        
        -- Simple derived fields
        case 
            when next_review_date is not null and next_review_date <= current_date() then true
            else false
        end as review_due,
        

        
        case 
            when next_review_date is not null 
            then datediff('day', current_date(), next_review_date)
            else null
        end as days_to_review,
        
        -- Simple risk assessment
        case 
            when politically_exposed_person then 'HIGH'
            when aml_risk_level = 'HIGH' then 'HIGH'
            when aml_risk_level = 'MEDIUM' then 'MEDIUM'
            else 'LOW'
        end as overall_risk_tier,
        
        -- Metadata
        created_at,
        updated_at
        
    from source_data
    where customer_id is not null
)

select * from cleaned_data

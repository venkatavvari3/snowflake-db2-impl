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
        document_issuer,
        document_expiry,
        verification_method,
        verification_date,
        last_review_date,
        next_review_date,
        aml_score,
        compliance_officer_id,
        notes,
        created_at,
        updated_at,
        _loaded_at
    from {{ source('raw_banking', 'kyc_aml') }}
),

cleaned_data as (
    select
        kyc_id,
        customer_id,
        
        -- KYC details
        upper(trim(kyc_type)) as kyc_type,
        upper(trim(kyc_status)) as kyc_status,
        kyc_date,
        expiry_date,
        upper(trim(risk_rating)) as risk_rating,
        
        -- Risk flags
        coalesce(pep_flag, false) as pep_flag,
        coalesce(sanctions_flag, false) as sanctions_flag,
        coalesce(adverse_media_flag, false) as adverse_media_flag,
        
        -- Financial profile
        trim(source_of_wealth) as source_of_wealth,
        trim(source_of_funds) as source_of_funds,
        upper(trim(occupation)) as occupation,
        upper(trim(industry)) as industry,
        annual_income,
        net_worth,
        
        -- Geographic information
        upper(trim(country_of_residence)) as country_of_residence,
        upper(trim(nationality)) as nationality,
        
        -- Documentation
        upper(trim(document_type)) as document_type,
        document_number,
        upper(trim(document_issuer)) as document_issuer,
        document_expiry,
        upper(trim(verification_method)) as verification_method,
        verification_date,
        
        -- Review cycle
        last_review_date,
        next_review_date,
        aml_score,
        compliance_officer_id,
        trim(notes) as notes,
        
        -- Derived fields
        case 
            when expiry_date is not null and expiry_date <= current_date() then true
            else false
        end as is_expired,
        
        case 
            when next_review_date is not null and next_review_date <= current_date() then true
            else false
        end as review_due,
        
        case 
            when expiry_date is not null 
            then datediff('day', current_date(), expiry_date)
            else null
        end as days_to_expiry,
        
        case 
            when next_review_date is not null 
            then datediff('day', current_date(), next_review_date)
            else null
        end as days_to_review,
        
        -- Risk assessment
        case 
            when pep_flag or sanctions_flag then 'VERY_HIGH'
            when adverse_media_flag then 'HIGH'
            when aml_score >= 70 then 'HIGH'
            when aml_score >= 50 then 'MEDIUM'
            when aml_score >= 30 then 'LOW'
            else 'VERY_LOW'
        end as overall_risk_tier,
        
        -- Compliance status
        case 
            when kyc_status = 'APPROVED' and not is_expired and not review_due then 'COMPLIANT'
            when kyc_status = 'APPROVED' and (is_expired or review_due) then 'REVIEW_REQUIRED'
            when kyc_status = 'PENDING' then 'PENDING_APPROVAL'
            when kyc_status = 'REJECTED' then 'NON_COMPLIANT'
            else 'UNKNOWN'
        end as compliance_status,
        
        -- High risk indicators
        (pep_flag::int + sanctions_flag::int + adverse_media_flag::int + 
         case when aml_score >= 70 then 1 else 0 end) as risk_indicator_count,
        
        -- Metadata
        created_at,
        updated_at,
        _loaded_at
        
    from source_data
    where customer_id is not null
)

select * from cleaned_data

-- Staging model for fraud alerts and investigations
-- Cleans and standardizes fraud detection data

{{ config(
    materialized='view',
    tags=['staging', 'fraud', 'security']
) }}

with source_data as (
    select
        alert_id,
        transaction_id,
        customer_id,
        account_id,
        alert_type,
        alert_severity,
        alert_date,
        alert_description,
        alert_status,
        investigation_notes,
        resolved_date,
        false_positive,
        created_at,
        updated_at
    from {{ source('raw_banking', 'fraud_alerts') }}
),

cleaned_data as (
    select
        alert_id,
        customer_id,
        transaction_id,
        account_id,
        
        -- Alert classification
        upper(trim(alert_type)) as alert_type,
        upper(trim(alert_severity)) as alert_severity,
        upper(trim(alert_status)) as alert_status,
        
        -- Timing
        alert_date,
        
        -- Investigation details
        trim(alert_description) as alert_description,
        trim(investigation_notes) as investigation_notes,
        resolved_date,
        coalesce(false_positive, false) as false_positive_flag,
        
        -- Simple derived fields
        case 
            when resolved_date is not null 
            then datediff('hour', alert_date, resolved_date)
            else null
        end as resolution_time_hours,
        
        case 
            when false_positive then 'FALSE_POSITIVE'
            when alert_status = 'RESOLVED' then 'INVESTIGATED_CLEAN'
            when alert_status = 'ACTIVE' then 'UNDER_INVESTIGATION'
            else 'PENDING_REVIEW'
        end as investigation_outcome,
        
        -- Metadata
        created_at,
        updated_at
        
    from source_data
    where customer_id is not null
)

select * from cleaned_data

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
        created_at,
        updated_at,
        _loaded_at
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
        upper(trim(alert_subtype)) as alert_subtype,
        risk_score,
        upper(trim(alert_status)) as alert_status,
        
        -- Timing
        alert_date,
        alert_time,
        timestamp_from_parts(
            alert_date, 
            coalesce(alert_time, time('00:00:00'))
        ) as alert_timestamp,
        
        -- Investigation details
        upper(trim(investigation_status)) as investigation_status,
        coalesce(false_positive_flag, false) as false_positive_flag,
        coalesce(confirmed_fraud_flag, false) as confirmed_fraud_flag,
        
        -- Financial impact
        amount_involved,
        upper(trim(currency)) as currency,
        
        -- Source and reason
        upper(trim(alert_source)) as alert_source,
        trim(alert_reason) as alert_reason,
        investigator_id,
        resolution_date,
        trim(resolution_notes) as resolution_notes,
        
        -- Derived fields
        case 
            when resolution_date is not null 
            then datediff('hour', alert_timestamp, timestamp_from_parts(resolution_date, time('00:00:00')))
            else null
        end as resolution_time_hours,
        
        case 
            when risk_score >= 80 then 'CRITICAL'
            when risk_score >= 60 then 'HIGH'
            when risk_score >= 40 then 'MEDIUM'
            when risk_score >= 20 then 'LOW'
            else 'MINIMAL'
        end as risk_tier,
        
        case 
            when confirmed_fraud_flag then 'CONFIRMED_FRAUD'
            when false_positive_flag then 'FALSE_POSITIVE'
            when investigation_status = 'CLOSED' then 'INVESTIGATED_CLEAN'
            when investigation_status = 'OPEN' then 'UNDER_INVESTIGATION'
            else 'PENDING_REVIEW'
        end as investigation_outcome,
        
        -- Metadata
        created_at,
        updated_at,
        _loaded_at
        
    from source_data
    where customer_id is not null
)

select * from cleaned_data

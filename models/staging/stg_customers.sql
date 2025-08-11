-- Staging model for customer data from core banking system
-- This model cleans and standardizes customer information

{{ config(
    materialized='view',
    tags=['staging', 'customers', 'pii']
) }}

with source_data as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        phone_number,
        date_of_birth,
        address_line_1,
        address_line_2,
        city,
        postal_code,
        country,
        customer_since,
        customer_status,
        risk_rating,
        created_at,
        updated_at
    from {{ source('raw_banking', 'customers') }}
),

cleaned_data as (
    select
        customer_id,
        
        -- Clean and standardize names
        trim(upper(first_name)) as first_name,
        trim(upper(last_name)) as last_name,
        
        -- Email validation and cleaning
        case 
            when regexp_like(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
            then lower(trim(email))
            else null
        end as email,
        
        -- Phone number standardization (UK format)
        case 
            when phone_number is not null
            then regexp_replace(phone_number, '[^0-9+]', '')
            else null
        end as phone_number,
        
        -- Date validation
        case 
            when date_of_birth between '1920-01-01' and current_date() - interval '18 years'
            then date_of_birth
            else null
        end as date_of_birth,
        
        -- Address standardization
        trim(upper(address_line_1)) as address_line_1,
        trim(upper(address_line_2)) as address_line_2,
        trim(upper(city)) as city,
        upper(trim(postal_code)) as postal_code,
        coalesce(upper(trim(country)), 'GB') as country,
        
        -- Dates
        customer_since,
        upper(trim(customer_status)) as customer_status,
        upper(trim(risk_rating)) as risk_rating,
        
        -- Calculated fields
        floor(datediff('year', date_of_birth, current_date())) as age,
        datediff('year', customer_since, current_date()) as tenure_years,
        
        -- PII masking for non-production environments
        {% if var('mask_pii') %}
            sha2(concat(first_name, last_name, email), 256) as customer_hash,
            'MASKED' as masked_first_name,
            'MASKED' as masked_last_name,
            'masked@email.com' as masked_email
        {% else %}
            null as customer_hash,
            first_name as masked_first_name,
            last_name as masked_last_name,
            email as masked_email
        {% endif %},
        
        -- Metadata
        created_at,
        updated_at
        
    from source_data
)

select * from cleaned_data

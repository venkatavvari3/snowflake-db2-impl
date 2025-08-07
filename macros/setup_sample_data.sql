{% macro setup_sample_data() %}
  {{ return(run_query('
    CREATE SCHEMA IF NOT EXISTS raw_data;
    
    CREATE OR REPLACE TABLE raw_data.customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(255),
        phone_number VARCHAR(50),
        date_of_birth DATE,
        address_line_1 VARCHAR(255),
        address_line_2 VARCHAR(255),
        city VARCHAR(100),
        postal_code VARCHAR(20),
        country VARCHAR(100),
        customer_since DATE,
        customer_status VARCHAR(50),
        risk_rating VARCHAR(20),
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
  ')) }}
{% endmacro %}

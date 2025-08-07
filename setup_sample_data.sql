-- Setup script to create sample banking data for testing the dbt pipeline
-- Run this in Snowflake to create the source tables that the dbt models expect

-- Create the raw_data schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Create sample customers table
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

-- Insert sample customer data
INSERT INTO raw_data.customers VALUES
('CUST001', 'John', 'Smith', 'john.smith@email.com', '+44 20 7946 0958', '1985-03-15', '123 High Street', 'Apartment 4B', 'London', 'SW1A 1AA', 'United Kingdom', '2020-01-15', 'ACTIVE', 'LOW', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('CUST002', 'Emma', 'Johnson', 'emma.johnson@email.com', '+44 161 496 0197', '1990-07-22', '456 King Road', NULL, 'Manchester', 'M1 4ET', 'United Kingdom', '2021-03-10', 'ACTIVE', 'MEDIUM', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('CUST003', 'Michael', 'Brown', 'michael.brown@email.com', '+44 131 496 0345', '1978-11-03', '789 Queen Avenue', 'Suite 12', 'Edinburgh', 'EH1 2NG', 'United Kingdom', '2019-08-20', 'ACTIVE', 'LOW', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('CUST004', 'Sarah', 'Davis', 'sarah.davis@email.com', '+44 29 2048 0234', '1992-12-01', '321 Prince Street', NULL, 'Cardiff', 'CF10 3AT', 'United Kingdom', '2022-01-05', 'ACTIVE', 'HIGH', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('CUST005', 'David', 'Wilson', 'david.wilson@email.com', '+44 28 9024 0187', '1983-05-18', '654 Duke Lane', NULL, 'Belfast', 'BT1 1AA', 'United Kingdom', '2020-11-30', 'INACTIVE', 'MEDIUM', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Create sample accounts table
CREATE OR REPLACE TABLE raw_data.accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    account_number VARCHAR(50),
    account_type VARCHAR(50),
    account_status VARCHAR(50),
    balance DECIMAL(15,2),
    currency VARCHAR(3),
    opened_date DATE,
    closed_date DATE,
    interest_rate DECIMAL(5,4),
    overdraft_limit DECIMAL(15,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample account data
INSERT INTO raw_data.accounts VALUES
('ACC001', 'CUST001', '12345678', 'CURRENT', 'ACTIVE', 2500.50, 'GBP', '2020-01-15', NULL, 0.0100, 1000.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ACC002', 'CUST001', '12345679', 'SAVINGS', 'ACTIVE', 15000.00, 'GBP', '2020-02-01', NULL, 0.0250, 0.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ACC003', 'CUST002', '23456789', 'CURRENT', 'ACTIVE', 1200.75, 'GBP', '2021-03-10', NULL, 0.0100, 500.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ACC004', 'CUST003', '34567890', 'CURRENT', 'ACTIVE', 3400.25, 'GBP', '2019-08-20', NULL, 0.0100, 2000.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ACC005', 'CUST003', '34567891', 'SAVINGS', 'ACTIVE', 25000.00, 'GBP', '2019-09-15', NULL, 0.0300, 0.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ACC006', 'CUST004', '45678901', 'CURRENT', 'ACTIVE', 800.00, 'GBP', '2022-01-05', NULL, 0.0100, 1500.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ACC007', 'CUST005', '56789012', 'CURRENT', 'CLOSED', 0.00, 'GBP', '2020-11-30', '2023-06-15', 0.0100, 0.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Create sample transactions table
CREATE OR REPLACE TABLE raw_data.transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50),
    transaction_type VARCHAR(50),
    amount DECIMAL(15,2),
    currency VARCHAR(3),
    transaction_date TIMESTAMP_NTZ,
    description VARCHAR(255),
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(100),
    reference_number VARCHAR(100),
    balance_after_transaction DECIMAL(15,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample transaction data
INSERT INTO raw_data.transactions VALUES
('TXN001', 'ACC001', 'DEBIT', -25.50, 'GBP', '2024-01-15 10:30:00', 'Coffee Shop Purchase', 'Starbucks London', 'Food & Drink', 'REF001', 2475.00, CURRENT_TIMESTAMP()),
('TXN002', 'ACC001', 'CREDIT', 2000.00, 'GBP', '2024-01-01 09:00:00', 'Salary Deposit', 'ABC Company Ltd', 'Salary', 'SAL001', 2500.50, CURRENT_TIMESTAMP()),
('TXN003', 'ACC001', 'DEBIT', -150.00, 'GBP', '2024-01-10 14:20:00', 'Grocery Shopping', 'Tesco Metro', 'Groceries', 'GRO001', 2350.50, CURRENT_TIMESTAMP()),
('TXN004', 'ACC002', 'CREDIT', 1000.00, 'GBP', '2024-01-05 12:00:00', 'Transfer from Current', 'Internal Transfer', 'Transfer', 'TRF001', 16000.00, CURRENT_TIMESTAMP()),
('TXN005', 'ACC003', 'DEBIT', -45.75, 'GBP', '2024-01-12 16:45:00', 'Online Purchase', 'Amazon UK', 'Online Retail', 'AMZ001', 1154.00, CURRENT_TIMESTAMP()),
('TXN006', 'ACC004', 'CREDIT', 1800.00, 'GBP', '2024-01-01 08:30:00', 'Freelance Payment', 'XYZ Consulting', 'Freelance', 'FRL001', 5200.25, CURRENT_TIMESTAMP()),
('TXN007', 'ACC004', 'DEBIT', -1800.00, 'GBP', '2024-01-20 11:15:00', 'Rent Payment', 'Property Management Co', 'Housing', 'RENT001', 3400.25, CURRENT_TIMESTAMP());

-- Create sample credit_loans table
CREATE OR REPLACE TABLE raw_data.credit_loans (
    loan_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    loan_type VARCHAR(50),
    principal_amount DECIMAL(15,2),
    interest_rate DECIMAL(5,4),
    loan_term_months INTEGER,
    monthly_payment DECIMAL(15,2),
    outstanding_balance DECIMAL(15,2),
    loan_status VARCHAR(50),
    disbursed_date DATE,
    maturity_date DATE,
    last_payment_date DATE,
    next_payment_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample loan data
INSERT INTO raw_data.credit_loans VALUES
('LOAN001', 'CUST001', 'MORTGAGE', 250000.00, 0.0350, 300, 1250.00, 240000.00, 'ACTIVE', '2020-01-15', '2045-01-15', '2024-01-01', '2024-02-01', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('LOAN002', 'CUST002', 'PERSONAL', 15000.00, 0.0650, 60, 290.50, 12500.00, 'ACTIVE', '2021-06-01', '2026-06-01', '2024-01-01', '2024-02-01', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('LOAN003', 'CUST003', 'AUTO', 25000.00, 0.0450, 72, 392.80, 18000.00, 'ACTIVE', '2022-03-15', '2028-03-15', '2024-01-01', '2024-02-01', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('LOAN004', 'CUST004', 'PERSONAL', 8000.00, 0.0750, 36, 250.75, 6500.00, 'ACTIVE', '2023-01-10', '2026-01-10', '2024-01-01', '2024-02-01', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Create sample fraud_alerts table
CREATE OR REPLACE TABLE raw_data.fraud_alerts (
    alert_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    account_id VARCHAR(50),
    transaction_id VARCHAR(50),
    alert_type VARCHAR(50),
    alert_severity VARCHAR(20),
    alert_status VARCHAR(50),
    alert_description TEXT,
    risk_score DECIMAL(5,2),
    triggered_date TIMESTAMP_NTZ,
    resolved_date TIMESTAMP_NTZ,
    resolved_by VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample fraud alert data
INSERT INTO raw_data.fraud_alerts VALUES
('FRD001', 'CUST004', 'ACC006', 'TXN008', 'UNUSUAL_SPENDING', 'HIGH', 'RESOLVED', 'Large transaction outside normal pattern', 85.50, '2024-01-15 14:30:00', '2024-01-15 16:00:00', 'fraud_analyst_1', CURRENT_TIMESTAMP()),
('FRD002', 'CUST002', 'ACC003', 'TXN009', 'VELOCITY_CHECK', 'MEDIUM', 'OPEN', 'Multiple transactions in short timeframe', 65.25, '2024-01-18 10:15:00', NULL, NULL, CURRENT_TIMESTAMP()),
('FRD003', 'CUST001', 'ACC001', 'TXN010', 'LOCATION_ANOMALY', 'LOW', 'FALSE_POSITIVE', 'Transaction from unusual location', 35.75, '2024-01-20 09:45:00', '2024-01-20 11:30:00', 'fraud_analyst_2', CURRENT_TIMESTAMP());

-- Create sample kyc_aml table
CREATE OR REPLACE TABLE raw_data.kyc_aml (
    record_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    check_type VARCHAR(50),
    check_status VARCHAR(50),
    check_date DATE,
    expiry_date DATE,
    risk_rating VARCHAR(20),
    pep_status BOOLEAN,
    sanctions_match BOOLEAN,
    source_of_funds VARCHAR(255),
    occupation VARCHAR(100),
    annual_income DECIMAL(15,2),
    documentation_status VARCHAR(50),
    last_review_date DATE,
    next_review_date DATE,
    reviewed_by VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample KYC/AML data
INSERT INTO raw_data.kyc_aml VALUES
('KYC001', 'CUST001', 'FULL_KYC', 'APPROVED', '2020-01-10', '2025-01-10', 'LOW', FALSE, FALSE, 'Employment', 'Software Engineer', 75000.00, 'COMPLETE', '2024-01-10', '2025-01-10', 'kyc_officer_1', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('KYC002', 'CUST002', 'FULL_KYC', 'APPROVED', '2021-03-05', '2026-03-05', 'MEDIUM', FALSE, FALSE, 'Employment', 'Marketing Manager', 55000.00, 'COMPLETE', '2024-03-05', '2025-03-05', 'kyc_officer_2', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('KYC003', 'CUST003', 'FULL_KYC', 'APPROVED', '2019-08-15', '2024-08-15', 'LOW', FALSE, FALSE, 'Business', 'Consultant', 95000.00, 'COMPLETE', '2023-08-15', '2024-08-15', 'kyc_officer_1', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('KYC004', 'CUST004', 'ENHANCED_DD', 'UNDER_REVIEW', '2022-01-01', '2027-01-01', 'HIGH', FALSE, TRUE, 'Investment', 'Investment Banker', 150000.00, 'PENDING', '2024-01-01', '2025-01-01', 'kyc_officer_3', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('KYC005', 'CUST005', 'SIMPLIFIED_DD', 'EXPIRED', '2020-11-25', '2023-11-25', 'MEDIUM', FALSE, FALSE, 'Employment', 'Teacher', 35000.00, 'EXPIRED', '2023-11-25', '2024-11-25', 'kyc_officer_2', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Show completion message
SELECT 'Sample banking data setup completed successfully!' as status;

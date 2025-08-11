{% docs dim_customers_description %}
# Customer Dimension Table

This table provides a comprehensive 360-degree view of customers combining:

- **Core Demographics**: Personal information, contact details, location
- **Account Summary**: Total accounts, balances, product holdings
- **Credit Portfolio**: Loan details, credit utilization, risk metrics  
- **Transaction Behavior**: Spending patterns, transaction volumes, activity levels
- **Risk Assessment**: Credit scores, risk tiers, compliance status
- **Customer Segmentation**: Value-based segments for targeted marketing

## Key Business Uses:
- Customer analytics and reporting
- Risk assessment and monitoring
- Marketing campaign targeting
- Product recommendation engines
- Regulatory reporting (KYC/AML)

## Data Refresh:
Updated daily via dbt pipeline at 2 AM UTC

## Key Metrics:
- **Customer Segments**: PREMIUM, AFFLUENT, MASS_AFFLUENT, MASS_MARKET, BASIC
- **Risk Tiers**: LOW_RISK, MEDIUM_RISK, HIGH_RISK  
- **Activity Levels**: HIGH_ACTIVITY, MEDIUM_ACTIVITY, LOW_ACTIVITY, INACTIVE
{% enddocs %}

{% docs fct_transactions_description %}
# Transaction Fact Table

Core fact table for all banking transactions providing detailed transaction data for analytics:

## Transaction Types:
- **CREDIT**: Money coming into accounts (deposits, transfers in, salary)
- **DEBIT**: Money leaving accounts (withdrawals, purchases, transfers out)

## Key Dimensions:
- Time dimensions (year, month, quarter, day of week, hour)
- Customer and account context
- Merchant and category information
- Geographic and channel data

## Business Applications:
- Transaction volume and value reporting
- Customer spending pattern analysis
- Merchant category analysis
- Fraud detection and monitoring
- Revenue and fee calculation

## Data Quality:
- All transactions validated for amount > 0
- Customer and account relationships verified
- Timestamps validated for reasonable ranges
{% enddocs %}

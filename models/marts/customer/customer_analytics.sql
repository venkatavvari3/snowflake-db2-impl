-- Customer analytics and insights
-- Provides comprehensive customer behavior analysis and segmentation

{{ config(
    materialized='table',
    tags=['marts', 'customer', 'analytics']
) }}

with customer_transaction_behavior as (
    select
        a.customer_id,
        
        -- Transaction volume metrics
        count(*) as total_transactions_12m,
        count(case when t.transaction_timestamp >= dateadd('month', -3, current_date()) then 1 end) as transactions_3m,
        count(case when t.transaction_timestamp >= dateadd('month', -1, current_date()) then 1 end) as transactions_1m,
        
        -- Spending patterns
        sum(case when t.transaction_direction = 'DEBIT' then t.absolute_amount else 0 end) as total_spending_12m,
        sum(case when t.transaction_direction = 'DEBIT' and t.transaction_timestamp >= dateadd('month', -3, current_date()) then t.absolute_amount else 0 end) as spending_3m,
        avg(case when t.transaction_direction = 'DEBIT' then t.absolute_amount end) as avg_spending_amount,
        
        -- Income patterns
        sum(case when t.transaction_direction = 'CREDIT' then t.absolute_amount else 0 end) as total_income_12m,
        sum(case when t.transaction_direction = 'CREDIT' and t.transaction_timestamp >= dateadd('month', -3, current_date()) then t.absolute_amount else 0 end) as income_3m,
        
        -- Channel preferences (not available in current data)
        0 as online_transactions,
        0 as mobile_transactions,
        0 as atm_transactions,
        0 as branch_transactions,
        0 as pos_transactions,
        
        -- Spending categories (using merchant_category)
        sum(case when t.merchant_category = 'GROCERIES' and t.transaction_direction = 'DEBIT' then t.absolute_amount else 0 end) as groceries_spending,
        sum(case when t.merchant_category = 'FOOD & DRINK' and t.transaction_direction = 'DEBIT' then t.absolute_amount else 0 end) as dining_spending,
        sum(case when t.merchant_category = 'RETAIL' and t.transaction_direction = 'DEBIT' then t.absolute_amount else 0 end) as retail_spending,
        sum(case when t.merchant_category = 'UTILITIES' and t.transaction_direction = 'DEBIT' then t.absolute_amount else 0 end) as utilities_spending,
        sum(case when t.merchant_category = 'FUEL' and t.transaction_direction = 'DEBIT' then t.absolute_amount else 0 end) as fuel_spending,
        sum(case when t.merchant_category = 'TRAVEL' and t.transaction_direction = 'DEBIT' then t.absolute_amount else 0 end) as travel_spending,
        
        -- Timing patterns
        count(case when t.day_type = 'WEEKEND' then 1 end) as weekend_transactions,
        count(case when t.time_of_day = 'MORNING' then 1 end) as morning_transactions,
        count(case when t.time_of_day = 'EVENING' then 1 end) as evening_transactions,
        
        -- Geographic patterns
        0 as international_transactions,  -- Column not available in current data
        1 as countries_used,  -- Defaulting to 1 as location data not available
        
        -- Last transaction date
        max(t.transaction_timestamp) as last_transaction_date
        
    from {{ ref('fct_transactions') }} t
    join {{ ref('stg_accounts') }} a on t.account_id = a.account_id
    where t.transaction_timestamp >= dateadd('month', -12, current_date())
    group by a.customer_id
),

customer_product_usage as (
    select
        customer_id,
        count(case when account_type = 'CURRENT' and is_active then 1 end) as active_current_accounts,
        count(case when account_type = 'SAVINGS' and is_active then 1 end) as active_savings_accounts,
        count(case when account_type = 'CREDIT' and is_active then 1 end) as active_credit_accounts,
        sum(case when account_type = 'CURRENT' and is_active then balance else 0 end) as current_account_balance,
        sum(case when account_type = 'SAVINGS' and is_active then balance else 0 end) as savings_account_balance,
        max(opened_date) as newest_account_date,
        min(opened_date) as oldest_account_date
    from {{ ref('stg_accounts') }}
    group by customer_id
),

customer_engagement as (
    select
        ctb.customer_id,
        
        -- Activity metrics
        ctb.total_transactions_12m,
        ctb.transactions_3m,
        ctb.transactions_1m,
        ctb.last_transaction_date,
        datediff('day', ctb.last_transaction_date, current_date()) as days_since_last_transaction,
        
        -- Financial behavior
        ctb.total_spending_12m,
        ctb.spending_3m,
        ctb.total_income_12m,
        ctb.income_3m,
        ctb.avg_spending_amount,
        
        -- Savings behavior
        case 
            when ctb.income_3m > 0 and ctb.spending_3m > 0 
            then (ctb.income_3m - ctb.spending_3m) / ctb.income_3m 
            else null 
        end as savings_rate_3m,
        
        -- Channel preferences
        case 
            when ctb.total_transactions_12m > 0 then ctb.mobile_transactions::float / ctb.total_transactions_12m 
            else 0 
        end as mobile_channel_preference,
        
        case 
            when ctb.total_transactions_12m > 0 then ctb.online_transactions::float / ctb.total_transactions_12m 
            else 0 
        end as online_channel_preference,
        
        case 
            when ctb.total_transactions_12m > 0 then ctb.branch_transactions::float / ctb.total_transactions_12m 
            else 0 
        end as branch_channel_preference,
        
        -- Spending patterns
        ctb.groceries_spending,
        ctb.dining_spending,
        ctb.retail_spending,
        ctb.utilities_spending,
        ctb.fuel_spending,
        ctb.travel_spending,
        
        -- Product usage
        cpu.active_current_accounts,
        cpu.active_savings_accounts,
        cpu.active_credit_accounts,
        cpu.current_account_balance,
        cpu.savings_account_balance,
        
        -- Engagement scoring (0-100)
        least(100, greatest(0,
            -- Transaction frequency (0-30)
            case 
                when ctb.transactions_3m >= 30 then 30
                when ctb.transactions_3m >= 20 then 25
                when ctb.transactions_3m >= 10 then 20
                when ctb.transactions_3m >= 5 then 15
                when ctb.transactions_3m >= 1 then 10
                else 0
            end +
            
            -- Channel diversity (0-20)
            case 
                when (ctb.mobile_transactions > 0)::int + (ctb.online_transactions > 0)::int + 
                     (ctb.atm_transactions > 0)::int + (ctb.branch_transactions > 0)::int >= 3 then 20
                when (ctb.mobile_transactions > 0)::int + (ctb.online_transactions > 0)::int + 
                     (ctb.atm_transactions > 0)::int + (ctb.branch_transactions > 0)::int >= 2 then 15
                else 10
            end +
            
            -- Product usage (0-25)
            (cpu.active_current_accounts * 8) + 
            (cpu.active_savings_accounts * 10) + 
            (cpu.active_credit_accounts * 7) +
            
            -- Recency (0-25)
            case 
                when datediff('day', ctb.last_transaction_date, current_date()) <= 7 then 25
                when datediff('day', ctb.last_transaction_date, current_date()) <= 30 then 20
                when datediff('day', ctb.last_transaction_date, current_date()) <= 90 then 15
                when datediff('day', ctb.last_transaction_date, current_date()) <= 180 then 10
                else 0
            end
        )) as engagement_score
        
    from customer_transaction_behavior ctb
    left join customer_product_usage cpu on ctb.customer_id = cpu.customer_id
)

select
    c.customer_id,
    c.customer_segment,
    c.generation,
    c.age,
    c.tenure_years,
    c.customer_status,
    c.total_balance,
    
    -- Engagement metrics
    ce.engagement_score,
    case 
        when ce.engagement_score >= 80 then 'HIGHLY_ENGAGED'
        when ce.engagement_score >= 60 then 'ENGAGED'
        when ce.engagement_score >= 40 then 'MODERATELY_ENGAGED'
        when ce.engagement_score >= 20 then 'LOW_ENGAGEMENT'
        else 'DISENGAGED'
    end as engagement_tier,
    
    -- Activity metrics
    ce.total_transactions_12m,
    ce.transactions_3m,
    ce.transactions_1m,
    ce.days_since_last_transaction,
    
    -- Financial behavior
    ce.total_spending_12m,
    ce.spending_3m,
    ce.total_income_12m,
    ce.income_3m,
    ce.avg_spending_amount,
    ce.savings_rate_3m,
    
    -- Channel preferences
    case 
        when ce.mobile_channel_preference >= 0.5 then 'MOBILE_FIRST'
        when ce.online_channel_preference >= 0.5 then 'DIGITAL_FIRST'
        when ce.branch_channel_preference >= 0.3 then 'BRANCH_PREFERRED'
        else 'MIXED_CHANNEL'
    end as preferred_channel,
    
    ce.mobile_channel_preference,
    ce.online_channel_preference,
    ce.branch_channel_preference,
    
    -- Spending patterns
    ce.groceries_spending,
    ce.dining_spending,
    ce.retail_spending,
    ce.utilities_spending,
    ce.fuel_spending,
    ce.travel_spending,
    
    -- Product usage
    ce.active_current_accounts,
    ce.active_savings_accounts,
    ce.active_credit_accounts,
    ce.current_account_balance,
    ce.savings_account_balance,
    
    -- Customer lifecycle stage
    case 
        when c.tenure_years < 1 then 'NEW_CUSTOMER'
        when c.tenure_years < 3 and ce.engagement_score >= 60 then 'GROWING_CUSTOMER'
        when c.tenure_years >= 3 and ce.engagement_score >= 70 then 'LOYAL_CUSTOMER'
        when c.tenure_years >= 5 and c.total_balance >= 50000 then 'VIP_CUSTOMER'
        when ce.days_since_last_transaction > 90 then 'AT_RISK'
        when ce.engagement_score < 30 then 'DORMANT'
        else 'STABLE_CUSTOMER'
    end as lifecycle_stage,
    
    -- Cross-sell opportunities
    case 
        when ce.active_savings_accounts = 0 and ce.savings_rate_3m > 0.1 then 'SAVINGS_OPPORTUNITY'
        when ce.active_credit_accounts = 0 and ce.total_spending_12m > 20000 then 'CREDIT_CARD_OPPORTUNITY'
        when ce.travel_spending > 5000 and ce.active_credit_accounts = 0 then 'TRAVEL_CARD_OPPORTUNITY'
        else 'NO_IMMEDIATE_OPPORTUNITY'
    end as cross_sell_opportunity,
    
    current_timestamp() as dbt_updated_at

from {{ ref('dim_customers') }} c
left join customer_engagement ce on c.customer_id = ce.customer_id
where c.customer_status = 'ACTIVE'

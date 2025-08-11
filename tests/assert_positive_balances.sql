-- Test to ensure no negative account balances in our data
-- This is a business rule test for banking data integrity

select
    account_id,
    balance,
    customer_id
from {{ ref('stg_accounts') }}
where balance < 0
  and is_active = true

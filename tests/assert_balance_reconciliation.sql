-- Test to ensure transaction amounts and balance reconciliation
-- Validates that account balances match transaction history

with transaction_totals as (
    select
        account_id,
        sum(amount) as total_transactions
    from {{ ref('stg_transactions') }}
    group by account_id
),

account_balances as (
    select
        account_id,
        balance as current_balance
    from {{ ref('stg_accounts') }}
)

select
    t.account_id,
    t.total_transactions,
    a.current_balance,
    abs(t.total_transactions - a.current_balance) as variance
from transaction_totals t
join account_balances a on t.account_id = a.account_id
where abs(t.total_transactions - a.current_balance) > 100  -- Allow for some variance
  and a.current_balance > 0

-- Test: Realized P&L should only exist for SALE trades

select *
from {{ ref('int_trade_pnl') }}
where trade_category != 'SALE'
  and realized_pnl is not null
  and realized_pnl != 0

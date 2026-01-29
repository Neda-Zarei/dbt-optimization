# Portfolio Analytics - dbt Sample Project

A dbt project targeting **Snowflake** that simulates institutional portfolio analytics across 3 pipelines of increasing complexity. Built as a realistic test input for optimization tools.

## Pipelines

The project has 3 linearly dependent pipelines (A &rarr; B &rarr; C):

| Pipeline | Focus | Models | Complexity |
|----------|-------|--------|------------|
| **A** | Cashflow analytics | 4 | Simple |
| **B** | Trade & position analytics | 9 | Medium |
| **C** | Portfolio performance & risk | 19 | Complex |

**Total: 32 models** (12 staging views, 9 intermediate views, 11 mart tables)

## Project Structure

```
models/
  pipeline_a/          -- Cashflow pipeline
    staging/           -- stg_portfolios, stg_cashflows
    marts/             -- fact_cashflow_summary, report_monthly_cashflows
  pipeline_b/          -- Trade analytics (depends on A)
    staging/           -- stg_brokers, stg_market_prices, stg_securities, stg_trades
    intermediate/      -- int_trades_enriched, int_trade_pnl
    marts/             -- fact_trade_summary, fact_portfolio_positions, report_trading_performance
  pipeline_c/          -- Portfolio analytics (depends on A + B)
    staging/           -- stg_benchmarks, stg_benchmark_returns, stg_fund_hierarchy,
                          stg_portfolio_benchmarks, stg_positions_daily, stg_valuations
    intermediate/      -- int_position_attribution, int_sector_attribution,
                          int_benchmark_aligned, int_portfolio_returns_daily,
                          int_portfolio_vs_benchmark, int_risk_metrics, int_fund_rollup
    marts/             -- fact_position_snapshot, fact_sector_performance,
                          fact_portfolio_performance, fact_fund_summary,
                          report_ic_dashboard, report_lp_quarterly
setup/
  create_raw_tables.sql  -- Generates source data in Snowflake (20 portfolios, 50 securities, 5+ years)
seeds/                   -- Reference data CSVs
macros/                  -- Reusable SQL macros (date_utils, aggregations, financial_calculations)
```

## Setup

### Prerequisites
- Snowflake account
- dbt-core with dbt-snowflake adapter

### 1. Configure connection

Set environment variables:
```bash
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"
```

### 2. Create source data

Run `setup/create_raw_tables.sql` in Snowflake (via Snowsight or SnowSQL). This creates 12 raw tables with realistic financial data.

### 3. Install dependencies and run

```bash
dbt deps
dbt run --full-refresh
```

### Running individual pipelines

```bash
dbt run --select models/pipeline_a
dbt run --select models/pipeline_b
dbt run --select models/pipeline_c
```

## SQL Anti-Patterns (Intentional)

The models contain deliberate SQL inefficiencies for Artemis to detect and optimize:

- **Self-joins instead of LAG/LEAD** - Prior period lookups via joins rather than window functions
- **Repeated window functions** - Same `PARTITION BY` clause duplicated across multiple CTEs
- **ROW_NUMBER + WHERE instead of QUALIFY** - Subquery filtering pattern
- **Unnecessary CTE separation** - Computations split across CTEs that could be merged
- **Redundant joins** - Re-joining data already available from upstream models
- **Repeated CASE expressions** - Same classification logic duplicated
- **Unnecessary GROUP BY aggregations** - Pre-aggregating in separate CTEs when window functions would suffice

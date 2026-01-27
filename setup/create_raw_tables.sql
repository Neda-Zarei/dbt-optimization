-- Setup script to create mock raw source tables for dbt project
-- Run this in Snowflake before executing dbt run

USE DATABASE DBT_DEMO;
USE SCHEMA DEV;

-- ============================================================================
-- Pipeline A: Cashflow Sources
-- ============================================================================

-- Portfolios table
CREATE OR REPLACE TABLE portfolios (
    portfolio_id VARCHAR(50),
    portfolio_name VARCHAR(200),
    portfolio_type VARCHAR(50),
    fund_id VARCHAR(50),
    status VARCHAR(20),
    inception_date DATE,
    currency VARCHAR(3),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

INSERT INTO portfolios VALUES
('PF001', 'Bain Growth Fund I', 'EQUITY', 'FD001', 'ACTIVE', '2020-01-15', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF002', 'Bain Value Fund II', 'EQUITY', 'FD001', 'ACTIVE', '2019-06-01', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF003', 'Credit Opportunities Fund', 'FIXED_INCOME', 'FD002', 'ACTIVE', '2018-03-20', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF004', 'High Yield Strategy', 'FIXED_INCOME', 'FD002', 'ACTIVE', '2021-09-10', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF005', 'Multi-Asset Balanced', 'MULTI_ASSET', 'FD003', 'ACTIVE', '2017-11-05', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF006', 'Emerging Markets Fund', 'EQUITY', 'FD003', 'ACTIVE', '2022-02-14', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF007', 'Real Estate Income Fund', 'ALTERNATIVE', 'FD004', 'ACTIVE', '2019-07-22', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF008', 'Infrastructure Fund', 'ALTERNATIVE', 'FD004', 'ACTIVE', '2020-10-30', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Cashflows table
CREATE OR REPLACE TABLE cashflows (
    cashflow_id VARCHAR(50),
    portfolio_id VARCHAR(50),
    cashflow_type VARCHAR(30),
    cashflow_date DATE,
    amount DECIMAL(18,2),
    currency VARCHAR(3),
    description VARCHAR(500),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- Generate realistic cashflow data (contributions, distributions, dividends, fees)
INSERT INTO cashflows
WITH date_spine AS (
    SELECT DATEADD(month, SEQ4(), '2023-01-01')::DATE AS month_date
    FROM TABLE(GENERATOR(ROWCOUNT => 24))  -- 24 months
),
portfolio_months AS (
    SELECT p.portfolio_id, d.month_date
    FROM portfolios p
    CROSS JOIN date_spine d
)
SELECT
    CONCAT('CF', ROW_NUMBER() OVER (ORDER BY pm.portfolio_id, pm.month_date, cf_type.type)) AS cashflow_id,
    pm.portfolio_id,
    cf_type.type AS cashflow_type,
    pm.month_date AS cashflow_date,
    CASE cf_type.type
        WHEN 'CONTRIBUTION' THEN UNIFORM(100000, 5000000, RANDOM())
        WHEN 'DISTRIBUTION' THEN -UNIFORM(50000, 2000000, RANDOM())
        WHEN 'DIVIDEND' THEN UNIFORM(10000, 500000, RANDOM())
        WHEN 'FEE' THEN -UNIFORM(5000, 100000, RANDOM())
    END AS amount,
    'USD' AS currency,
    CONCAT(cf_type.type, ' for ', pm.month_date) AS description,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM portfolio_months pm
CROSS JOIN (
    SELECT 'CONTRIBUTION' AS type UNION ALL
    SELECT 'DISTRIBUTION' UNION ALL
    SELECT 'DIVIDEND' UNION ALL
    SELECT 'FEE'
) cf_type
WHERE UNIFORM(0, 1, RANDOM()) > 0.5;  -- Randomly skip some cashflows

-- ============================================================================
-- Pipeline B: Trade Sources
-- ============================================================================

-- Securities table
CREATE OR REPLACE TABLE securities (
    security_id VARCHAR(50),
    ticker VARCHAR(20),
    security_name VARCHAR(200),
    security_type VARCHAR(50),
    asset_class VARCHAR(50),
    sector VARCHAR(100),
    industry VARCHAR(100),
    currency VARCHAR(3),
    exchange VARCHAR(50),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

INSERT INTO securities VALUES
('SEC001', 'AAPL', 'Apple Inc.', 'STOCK', 'EQUITY', 'Technology', 'Consumer Electronics', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC002', 'MSFT', 'Microsoft Corporation', 'STOCK', 'EQUITY', 'Technology', 'Software', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC003', 'GOOGL', 'Alphabet Inc.', 'STOCK', 'EQUITY', 'Technology', 'Internet Services', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC004', 'JPM', 'JPMorgan Chase & Co.', 'STOCK', 'EQUITY', 'Financials', 'Banking', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC005', 'BAC', 'Bank of America Corp.', 'STOCK', 'EQUITY', 'Financials', 'Banking', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC006', 'JNJ', 'Johnson & Johnson', 'STOCK', 'EQUITY', 'Healthcare', 'Pharmaceuticals', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC007', 'XOM', 'Exxon Mobil Corporation', 'STOCK', 'EQUITY', 'Energy', 'Oil & Gas', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC008', 'SPY', 'SPDR S&P 500 ETF', 'ETF', 'EQUITY', 'N/A', 'Index Fund', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC009', 'AGG', 'iShares Core US Aggregate Bond ETF', 'ETF', 'FIXED_INCOME', 'N/A', 'Bond Fund', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC010', 'CORP_BOND_001', 'Apple 3.5% 2025', 'BOND', 'FIXED_INCOME', 'Corporate', 'Technology', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC011', 'CORP_BOND_002', 'Microsoft 2.9% 2026', 'BOND', 'FIXED_INCOME', 'Corporate', 'Technology', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC012', 'CORP_BOND_003', 'JPM 4.1% 2027', 'BOND', 'FIXED_INCOME', 'Corporate', 'Banking', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC013', 'TSLA', 'Tesla Inc.', 'STOCK', 'EQUITY', 'Consumer Discretionary', 'Automobiles', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC014', 'NVDA', 'NVIDIA Corporation', 'STOCK', 'EQUITY', 'Technology', 'Semiconductors', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC015', 'VZ', 'Verizon Communications', 'STOCK', 'EQUITY', 'Telecommunications', 'Telecom Services', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Brokers table
CREATE OR REPLACE TABLE brokers (
    broker_id VARCHAR(50),
    broker_name VARCHAR(200),
    broker_type VARCHAR(50),
    region VARCHAR(50),
    is_active BOOLEAN,
    commission_rate DECIMAL(6,4),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

INSERT INTO brokers VALUES
('BRK001', 'Goldman Sachs', 'FULL_SERVICE', 'NORTH_AMERICA', TRUE, 0.0025, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BRK002', 'Morgan Stanley', 'FULL_SERVICE', 'NORTH_AMERICA', TRUE, 0.0023, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BRK003', 'Interactive Brokers', 'DISCOUNT', 'GLOBAL', TRUE, 0.0005, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BRK004', 'Charles Schwab', 'DISCOUNT', 'NORTH_AMERICA', TRUE, 0.0000, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BRK005', 'Barclays', 'FULL_SERVICE', 'EUROPE', TRUE, 0.0020, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Trades table
CREATE OR REPLACE TABLE trades (
    trade_id VARCHAR(50),
    portfolio_id VARCHAR(50),
    security_id VARCHAR(50),
    broker_id VARCHAR(50),
    trade_date DATE,
    settlement_date DATE,
    trade_type VARCHAR(20),
    quantity DECIMAL(18,4),
    price DECIMAL(18,4),
    gross_amount DECIMAL(18,2),
    commission DECIMAL(18,2),
    fees DECIMAL(18,2),
    net_amount DECIMAL(18,2),
    currency VARCHAR(3),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- Generate realistic trade data
INSERT INTO trades
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2023-01-01')::DATE AS trade_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730))  -- 2 years
    WHERE DAYOFWEEK(trade_date) NOT IN (0, 6)  -- Exclude weekends
),
trade_gen AS (
    SELECT
        CONCAT('TR', ROW_NUMBER() OVER (ORDER BY p.portfolio_id, d.trade_date)) AS trade_id,
        p.portfolio_id,
        s.security_id,
        b.broker_id,
        d.trade_date,
        DATEADD(day, 2, d.trade_date) AS settlement_date,
        CASE WHEN UNIFORM(0, 1, RANDOM()) > 0.5 THEN 'BUY' ELSE 'SELL' END AS trade_type,
        UNIFORM(100, 10000, RANDOM()) AS quantity,
        CASE s.security_id
            WHEN 'SEC001' THEN UNIFORM(150, 180, RANDOM())
            WHEN 'SEC002' THEN UNIFORM(300, 380, RANDOM())
            WHEN 'SEC003' THEN UNIFORM(120, 145, RANDOM())
            WHEN 'SEC004' THEN UNIFORM(140, 160, RANDOM())
            WHEN 'SEC005' THEN UNIFORM(30, 40, RANDOM())
            WHEN 'SEC006' THEN UNIFORM(160, 175, RANDOM())
            WHEN 'SEC007' THEN UNIFORM(95, 115, RANDOM())
            WHEN 'SEC008' THEN UNIFORM(400, 450, RANDOM())
            WHEN 'SEC009' THEN UNIFORM(100, 110, RANDOM())
            ELSE UNIFORM(50, 200, RANDOM())
        END AS price,
        b.commission_rate
    FROM portfolios p
    CROSS JOIN date_spine d
    CROSS JOIN securities s
    CROSS JOIN brokers b
    WHERE UNIFORM(0, 1, RANDOM()) > 0.995  -- Sparse trades
)
SELECT
    trade_id,
    portfolio_id,
    security_id,
    broker_id,
    trade_date,
    settlement_date,
    trade_type,
    quantity,
    price,
    ROUND(quantity * price, 2) AS gross_amount,
    ROUND(quantity * price * commission_rate, 2) AS commission,
    ROUND(UNIFORM(1, 10, RANDOM()), 2) AS fees,
    ROUND(
        CASE WHEN trade_type = 'BUY'
        THEN quantity * price + (quantity * price * commission_rate) + UNIFORM(1, 10, RANDOM())
        ELSE -(quantity * price - (quantity * price * commission_rate) - UNIFORM(1, 10, RANDOM()))
        END, 2
    ) AS net_amount,
    'USD' AS currency,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM trade_gen
LIMIT 5000;  -- Limit total trades

-- Market prices table
CREATE OR REPLACE TABLE market_prices (
    security_id VARCHAR(50),
    price_date DATE,
    open_price DECIMAL(18,4),
    high_price DECIMAL(18,4),
    low_price DECIMAL(18,4),
    close_price DECIMAL(18,4),
    volume BIGINT,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- Generate daily price data
INSERT INTO market_prices
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2023-01-01')::DATE AS price_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730))
    WHERE DAYOFWEEK(price_date) NOT IN (0, 6)
)
SELECT
    s.security_id,
    d.price_date,
    CASE s.security_id
        WHEN 'SEC001' THEN UNIFORM(150, 180, RANDOM())
        WHEN 'SEC002' THEN UNIFORM(300, 380, RANDOM())
        WHEN 'SEC003' THEN UNIFORM(120, 145, RANDOM())
        ELSE UNIFORM(50, 200, RANDOM())
    END AS open_price,
    CASE s.security_id
        WHEN 'SEC001' THEN UNIFORM(150, 180, RANDOM())
        WHEN 'SEC002' THEN UNIFORM(300, 380, RANDOM())
        ELSE UNIFORM(50, 200, RANDOM())
    END AS high_price,
    CASE s.security_id
        WHEN 'SEC001' THEN UNIFORM(150, 180, RANDOM())
        WHEN 'SEC002' THEN UNIFORM(300, 380, RANDOM())
        ELSE UNIFORM(50, 200, RANDOM())
    END AS low_price,
    CASE s.security_id
        WHEN 'SEC001' THEN UNIFORM(150, 180, RANDOM())
        WHEN 'SEC002' THEN UNIFORM(300, 380, RANDOM())
        WHEN 'SEC003' THEN UNIFORM(120, 145, RANDOM())
        ELSE UNIFORM(50, 200, RANDOM())
    END AS close_price,
    UNIFORM(1000000, 50000000, RANDOM()) AS volume,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM securities s
CROSS JOIN date_spine d;

-- ============================================================================
-- Pipeline C: Portfolio Analytics Sources
-- ============================================================================

-- Valuations table
CREATE OR REPLACE TABLE valuations (
    valuation_id VARCHAR(50),
    portfolio_id VARCHAR(50),
    valuation_date DATE,
    nav DECIMAL(18,2),
    nav_per_share DECIMAL(18,6),
    shares_outstanding DECIMAL(18,6),
    gross_assets DECIMAL(18,2),
    total_liabilities DECIMAL(18,2),
    net_assets DECIMAL(18,2),
    currency VARCHAR(3),
    fx_rate_to_usd DECIMAL(18,8),
    nav_usd DECIMAL(18,2),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- Generate daily NAV data
INSERT INTO valuations
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2023-01-01')::DATE AS val_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730))
    WHERE DAYOFWEEK(val_date) NOT IN (0, 6)
)
SELECT
    CONCAT('VAL', ROW_NUMBER() OVER (ORDER BY p.portfolio_id, d.val_date)) AS valuation_id,
    p.portfolio_id,
    d.val_date AS valuation_date,
    UNIFORM(50000000, 500000000, RANDOM()) AS nav,
    UNIFORM(95, 115, RANDOM()) AS nav_per_share,
    UNIFORM(500000, 5000000, RANDOM()) AS shares_outstanding,
    UNIFORM(55000000, 520000000, RANDOM()) AS gross_assets,
    UNIFORM(1000000, 20000000, RANDOM()) AS total_liabilities,
    UNIFORM(50000000, 500000000, RANDOM()) AS net_assets,
    'USD' AS currency,
    1.0 AS fx_rate_to_usd,
    UNIFORM(50000000, 500000000, RANDOM()) AS nav_usd,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM portfolios p
CROSS JOIN date_spine d;

-- Positions daily table
CREATE OR REPLACE TABLE positions_daily (
    position_id VARCHAR(50),
    portfolio_id VARCHAR(50),
    security_id VARCHAR(50),
    position_date DATE,
    quantity DECIMAL(18,4),
    cost_basis_price DECIMAL(18,4),
    cost_basis_value DECIMAL(18,2),
    market_price DECIMAL(18,4),
    market_value DECIMAL(18,2),
    market_value_usd DECIMAL(18,2),
    unrealized_pnl DECIMAL(18,2),
    unrealized_pnl_pct DECIMAL(8,4),
    weight_pct DECIMAL(8,4),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- Generate position snapshots
INSERT INTO positions_daily
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2023-01-01')::DATE AS pos_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730))
    WHERE DAYOFWEEK(pos_date) NOT IN (0, 6)
),
positions AS (
    SELECT
        CONCAT('POS', ROW_NUMBER() OVER (ORDER BY p.portfolio_id, s.security_id, d.pos_date)) AS position_id,
        p.portfolio_id,
        s.security_id,
        d.pos_date AS position_date,
        UNIFORM(1000, 50000, RANDOM()) AS quantity,
        UNIFORM(50, 200, RANDOM()) AS cost_basis_price,
        UNIFORM(50, 200, RANDOM()) AS market_price
    FROM portfolios p
    CROSS JOIN securities s
    CROSS JOIN date_spine d
    WHERE UNIFORM(0, 1, RANDOM()) > 0.7  -- Not all portfolios hold all securities
)
SELECT
    position_id,
    portfolio_id,
    security_id,
    position_date,
    quantity,
    cost_basis_price,
    ROUND(quantity * cost_basis_price, 2) AS cost_basis_value,
    market_price,
    ROUND(quantity * market_price, 2) AS market_value,
    ROUND(quantity * market_price, 2) AS market_value_usd,
    ROUND((market_price - cost_basis_price) * quantity, 2) AS unrealized_pnl,
    ROUND((market_price - cost_basis_price) / NULLIF(cost_basis_price, 0) * 100, 4) AS unrealized_pnl_pct,
    UNIFORM(0.5, 15.0, RANDOM()) AS weight_pct,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM positions
LIMIT 50000;

-- Benchmarks table
CREATE OR REPLACE TABLE benchmarks (
    benchmark_id VARCHAR(50),
    benchmark_name VARCHAR(200),
    benchmark_ticker VARCHAR(20),
    asset_class VARCHAR(50),
    region VARCHAR(50),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

INSERT INTO benchmarks VALUES
('BM001', 'S&P 500 Index', 'SPX', 'EQUITY', 'NORTH_AMERICA', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM002', 'Russell 2000 Index', 'RUT', 'EQUITY', 'NORTH_AMERICA', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM003', 'Bloomberg US Aggregate Bond Index', 'AGG', 'FIXED_INCOME', 'NORTH_AMERICA', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM004', 'MSCI World Index', 'MXWO', 'EQUITY', 'GLOBAL', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM005', 'MSCI Emerging Markets Index', 'MXEF', 'EQUITY', 'EMERGING_MARKETS', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM006', 'Bloomberg High Yield Corporate Bond Index', 'HY', 'FIXED_INCOME', 'NORTH_AMERICA', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Benchmark returns table
CREATE OR REPLACE TABLE benchmark_returns (
    benchmark_id VARCHAR(50),
    return_date DATE,
    daily_return DECIMAL(10,6),
    index_level DECIMAL(18,4),
    total_return_index DECIMAL(18,4),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- Generate benchmark return data
INSERT INTO benchmark_returns
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2023-01-01')::DATE AS ret_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730))
    WHERE DAYOFWEEK(ret_date) NOT IN (0, 6)
)
SELECT
    b.benchmark_id,
    d.ret_date AS return_date,
    UNIFORM(-0.03, 0.03, RANDOM()) AS daily_return,
    CASE b.benchmark_id
        WHEN 'BM001' THEN UNIFORM(4000, 4500, RANDOM())
        WHEN 'BM002' THEN UNIFORM(1800, 2100, RANDOM())
        ELSE UNIFORM(1000, 3000, RANDOM())
    END AS index_level,
    CASE b.benchmark_id
        WHEN 'BM001' THEN UNIFORM(8000, 9000, RANDOM())
        ELSE UNIFORM(5000, 10000, RANDOM())
    END AS total_return_index,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM benchmarks b
CROSS JOIN date_spine d;

-- Portfolio benchmarks mapping
CREATE OR REPLACE TABLE portfolio_benchmarks (
    portfolio_id VARCHAR(50),
    benchmark_id VARCHAR(50),
    is_primary BOOLEAN,
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

INSERT INTO portfolio_benchmarks VALUES
('PF001', 'BM001', TRUE, '2020-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF002', 'BM001', TRUE, '2019-06-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF003', 'BM003', TRUE, '2018-03-20', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF004', 'BM006', TRUE, '2021-09-10', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF005', 'BM004', TRUE, '2017-11-05', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF006', 'BM005', TRUE, '2022-02-14', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF007', 'BM001', TRUE, '2019-07-22', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF008', 'BM001', TRUE, '2020-10-30', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Fund hierarchy table
CREATE OR REPLACE TABLE fund_hierarchy (
    entity_id VARCHAR(50),
    entity_name VARCHAR(200),
    entity_type VARCHAR(50),
    parent_entity_id VARCHAR(50),
    hierarchy_level INT,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

INSERT INTO fund_hierarchy VALUES
('FD001', 'Bain Capital Public Equity', 'FUND', NULL, 1, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('FD002', 'Bain Capital Credit', 'FUND', NULL, 1, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('FD003', 'Bain Capital Multi-Strategy', 'FUND', NULL, 1, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('FD004', 'Bain Capital Alternatives', 'FUND', NULL, 1, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST001', 'Large Cap Strategy', 'STRATEGY', 'FD001', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST002', 'Growth Strategy', 'STRATEGY', 'FD001', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST003', 'Investment Grade Strategy', 'STRATEGY', 'FD002', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST004', 'High Yield Strategy', 'STRATEGY', 'FD002', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST005', 'Balanced Strategy', 'STRATEGY', 'FD003', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST006', 'EM Strategy', 'STRATEGY', 'FD003', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF001', 'Bain Growth Fund I', 'PORTFOLIO', 'ST002', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF002', 'Bain Value Fund II', 'PORTFOLIO', 'ST001', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF003', 'Credit Opportunities Fund', 'PORTFOLIO', 'ST003', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF004', 'High Yield Strategy', 'PORTFOLIO', 'ST004', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF005', 'Multi-Asset Balanced', 'PORTFOLIO', 'ST005', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF006', 'Emerging Markets Fund', 'PORTFOLIO', 'ST006', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF007', 'Real Estate Income Fund', 'PORTFOLIO', 'FD004', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF008', 'Infrastructure Fund', 'PORTFOLIO', 'FD004', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- ============================================================================
-- Verification queries
-- ============================================================================

SELECT 'portfolios' AS table_name, COUNT(*) AS row_count FROM portfolios
UNION ALL
SELECT 'cashflows', COUNT(*) FROM cashflows
UNION ALL
SELECT 'securities', COUNT(*) FROM securities
UNION ALL
SELECT 'brokers', COUNT(*) FROM brokers
UNION ALL
SELECT 'trades', COUNT(*) FROM trades
UNION ALL
SELECT 'market_prices', COUNT(*) FROM market_prices
UNION ALL
SELECT 'valuations', COUNT(*) FROM valuations
UNION ALL
SELECT 'positions_daily', COUNT(*) FROM positions_daily
UNION ALL
SELECT 'benchmarks', COUNT(*) FROM benchmarks
UNION ALL
SELECT 'benchmark_returns', COUNT(*) FROM benchmark_returns
UNION ALL
SELECT 'portfolio_benchmarks', COUNT(*) FROM portfolio_benchmarks
UNION ALL
SELECT 'fund_hierarchy', COUNT(*) FROM fund_hierarchy
ORDER BY table_name;

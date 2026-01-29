-- Setup script to create mock raw source tables for dbt project
-- Run this in Snowflake before executing dbt run
-- This version generates HEAVY data for realistic runtime testing

USE DATABASE DBT_DEMO;
USE SCHEMA DEV;

-- ============================================================================
-- Configuration: Adjust these to control data volume
-- ============================================================================
-- For HEAVY runtime testing: keep these values high
-- For quick testing: reduce ROWCOUNT values

-- ============================================================================
-- Pipeline A: Cashflow Sources (Foundation - no dependencies)
-- ============================================================================

-- Portfolios table (20 portfolios across 4 funds)
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
-- Fund 1: Public Equity (5 portfolios)
('PF001', 'Bain Growth Fund I', 'EQUITY', 'FD001', 'ACTIVE', '2018-01-15', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF002', 'Bain Value Fund II', 'EQUITY', 'FD001', 'ACTIVE', '2018-06-01', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF003', 'Bain Small Cap Fund', 'EQUITY', 'FD001', 'ACTIVE', '2019-03-20', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF004', 'Bain Large Cap Fund', 'EQUITY', 'FD001', 'ACTIVE', '2019-09-10', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF005', 'Bain Tech Focus Fund', 'EQUITY', 'FD001', 'ACTIVE', '2020-01-05', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Fund 2: Credit (5 portfolios)
('PF006', 'Credit Opportunities Fund', 'FIXED_INCOME', 'FD002', 'ACTIVE', '2018-03-20', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF007', 'High Yield Strategy', 'FIXED_INCOME', 'FD002', 'ACTIVE', '2018-09-10', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF008', 'Investment Grade Fund', 'FIXED_INCOME', 'FD002', 'ACTIVE', '2019-02-14', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF009', 'Distressed Debt Fund', 'FIXED_INCOME', 'FD002', 'ACTIVE', '2019-07-22', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF010', 'Convertible Bond Fund', 'FIXED_INCOME', 'FD002', 'ACTIVE', '2020-03-15', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Fund 3: Multi-Strategy (5 portfolios)
('PF011', 'Multi-Asset Balanced', 'MULTI_ASSET', 'FD003', 'ACTIVE', '2017-11-05', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF012', 'Emerging Markets Fund', 'MULTI_ASSET', 'FD003', 'ACTIVE', '2018-02-14', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF013', 'Global Macro Fund', 'MULTI_ASSET', 'FD003', 'ACTIVE', '2018-08-20', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF014', 'Tactical Allocation Fund', 'MULTI_ASSET', 'FD003', 'ACTIVE', '2019-04-10', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF015', 'Risk Parity Fund', 'MULTI_ASSET', 'FD003', 'ACTIVE', '2019-10-30', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Fund 4: Alternatives (5 portfolios)
('PF016', 'Real Estate Income Fund', 'ALTERNATIVE', 'FD004', 'ACTIVE', '2018-07-22', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF017', 'Infrastructure Fund', 'ALTERNATIVE', 'FD004', 'ACTIVE', '2018-10-30', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF018', 'Private Equity Co-Invest', 'ALTERNATIVE', 'FD004', 'ACTIVE', '2019-01-15', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF019', 'Commodities Fund', 'ALTERNATIVE', 'FD004', 'ACTIVE', '2019-06-01', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF020', 'Hedge Fund Solutions', 'ALTERNATIVE', 'FD004', 'ACTIVE', '2020-02-20', 'USD', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Cashflows table (HEAVY: 5 years of monthly data × 20 portfolios × 4 types)
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

-- Generate 5 years of cashflow data (2019-2024)
INSERT INTO cashflows
WITH date_spine AS (
    SELECT DATEADD(month, SEQ4(), '2019-01-01')::DATE AS month_date
    FROM TABLE(GENERATOR(ROWCOUNT => 72))  -- 6 years = 72 months
),
portfolio_months AS (
    SELECT p.portfolio_id, d.month_date
    FROM portfolios p
    CROSS JOIN date_spine d
    WHERE d.month_date >= p.inception_date
)
SELECT
    CONCAT('CF', LPAD(ROW_NUMBER() OVER (ORDER BY pm.portfolio_id, pm.month_date, cf_type.type), 8, '0')) AS cashflow_id,
    pm.portfolio_id,
    cf_type.type AS cashflow_type,
    pm.month_date AS cashflow_date,
    CASE cf_type.type
        WHEN 'CONTRIBUTION' THEN UNIFORM(500000, 10000000, RANDOM())
        WHEN 'DISTRIBUTION' THEN -UNIFORM(100000, 5000000, RANDOM())
        WHEN 'DIVIDEND' THEN UNIFORM(50000, 1000000, RANDOM())
        WHEN 'FEE' THEN -UNIFORM(10000, 200000, RANDOM())
    END AS amount,
    'USD' AS currency,
    CONCAT(cf_type.type, ' - ', pm.portfolio_id, ' - ', pm.month_date) AS description,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM portfolio_months pm
CROSS JOIN (
    SELECT 'CONTRIBUTION' AS type UNION ALL
    SELECT 'DISTRIBUTION' UNION ALL
    SELECT 'DIVIDEND' UNION ALL
    SELECT 'FEE'
) cf_type
WHERE UNIFORM(0, 1, RANDOM()) > 0.3;  -- 70% of possible cashflows

-- ============================================================================
-- Pipeline B: Trade Sources (Depends on Portfolio data from Pipeline A)
-- ============================================================================

-- Securities table (50 securities across different asset classes)
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
-- Equities - Technology (10)
('SEC001', 'AAPL', 'Apple Inc.', 'STOCK', 'EQUITY', 'Technology', 'Consumer Electronics', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC002', 'MSFT', 'Microsoft Corporation', 'STOCK', 'EQUITY', 'Technology', 'Software', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC003', 'GOOGL', 'Alphabet Inc.', 'STOCK', 'EQUITY', 'Technology', 'Internet Services', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC004', 'AMZN', 'Amazon.com Inc.', 'STOCK', 'EQUITY', 'Technology', 'E-Commerce', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC005', 'NVDA', 'NVIDIA Corporation', 'STOCK', 'EQUITY', 'Technology', 'Semiconductors', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC006', 'META', 'Meta Platforms Inc.', 'STOCK', 'EQUITY', 'Technology', 'Social Media', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC007', 'TSLA', 'Tesla Inc.', 'STOCK', 'EQUITY', 'Technology', 'Electric Vehicles', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC008', 'AMD', 'Advanced Micro Devices', 'STOCK', 'EQUITY', 'Technology', 'Semiconductors', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC009', 'CRM', 'Salesforce Inc.', 'STOCK', 'EQUITY', 'Technology', 'Software', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC010', 'ORCL', 'Oracle Corporation', 'STOCK', 'EQUITY', 'Technology', 'Software', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Equities - Financials (10)
('SEC011', 'JPM', 'JPMorgan Chase & Co.', 'STOCK', 'EQUITY', 'Financials', 'Banking', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC012', 'BAC', 'Bank of America Corp.', 'STOCK', 'EQUITY', 'Financials', 'Banking', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC013', 'WFC', 'Wells Fargo & Co.', 'STOCK', 'EQUITY', 'Financials', 'Banking', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC014', 'GS', 'Goldman Sachs Group', 'STOCK', 'EQUITY', 'Financials', 'Investment Banking', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC015', 'MS', 'Morgan Stanley', 'STOCK', 'EQUITY', 'Financials', 'Investment Banking', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC016', 'BLK', 'BlackRock Inc.', 'STOCK', 'EQUITY', 'Financials', 'Asset Management', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC017', 'C', 'Citigroup Inc.', 'STOCK', 'EQUITY', 'Financials', 'Banking', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC018', 'AXP', 'American Express Co.', 'STOCK', 'EQUITY', 'Financials', 'Credit Services', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC019', 'V', 'Visa Inc.', 'STOCK', 'EQUITY', 'Financials', 'Payment Processing', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC020', 'MA', 'Mastercard Inc.', 'STOCK', 'EQUITY', 'Financials', 'Payment Processing', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Equities - Healthcare (8)
('SEC021', 'JNJ', 'Johnson & Johnson', 'STOCK', 'EQUITY', 'Healthcare', 'Pharmaceuticals', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC022', 'UNH', 'UnitedHealth Group', 'STOCK', 'EQUITY', 'Healthcare', 'Insurance', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC023', 'PFE', 'Pfizer Inc.', 'STOCK', 'EQUITY', 'Healthcare', 'Pharmaceuticals', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC024', 'ABBV', 'AbbVie Inc.', 'STOCK', 'EQUITY', 'Healthcare', 'Pharmaceuticals', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC025', 'MRK', 'Merck & Co.', 'STOCK', 'EQUITY', 'Healthcare', 'Pharmaceuticals', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC026', 'LLY', 'Eli Lilly & Co.', 'STOCK', 'EQUITY', 'Healthcare', 'Pharmaceuticals', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC027', 'TMO', 'Thermo Fisher Scientific', 'STOCK', 'EQUITY', 'Healthcare', 'Lab Equipment', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC028', 'DHR', 'Danaher Corporation', 'STOCK', 'EQUITY', 'Healthcare', 'Lab Equipment', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Equities - Energy & Industrials (7)
('SEC029', 'XOM', 'Exxon Mobil Corporation', 'STOCK', 'EQUITY', 'Energy', 'Oil & Gas', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC030', 'CVX', 'Chevron Corporation', 'STOCK', 'EQUITY', 'Energy', 'Oil & Gas', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC031', 'COP', 'ConocoPhillips', 'STOCK', 'EQUITY', 'Energy', 'Oil & Gas', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC032', 'CAT', 'Caterpillar Inc.', 'STOCK', 'EQUITY', 'Industrials', 'Machinery', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC033', 'BA', 'Boeing Company', 'STOCK', 'EQUITY', 'Industrials', 'Aerospace', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC034', 'HON', 'Honeywell International', 'STOCK', 'EQUITY', 'Industrials', 'Conglomerate', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC035', 'UPS', 'United Parcel Service', 'STOCK', 'EQUITY', 'Industrials', 'Logistics', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- ETFs (5)
('SEC036', 'SPY', 'SPDR S&P 500 ETF', 'ETF', 'EQUITY', 'N/A', 'Index Fund', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC037', 'QQQ', 'Invesco QQQ Trust', 'ETF', 'EQUITY', 'N/A', 'Index Fund', 'USD', 'NASDAQ', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC038', 'IWM', 'iShares Russell 2000', 'ETF', 'EQUITY', 'N/A', 'Index Fund', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC039', 'AGG', 'iShares Core US Aggregate Bond', 'ETF', 'FIXED_INCOME', 'N/A', 'Bond Fund', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC040', 'LQD', 'iShares iBoxx Investment Grade', 'ETF', 'FIXED_INCOME', 'N/A', 'Bond Fund', 'USD', 'NYSE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Bonds (10)
('SEC041', 'BOND_AAPL_25', 'Apple 3.5% 2025', 'BOND', 'FIXED_INCOME', 'Corporate', 'Technology', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC042', 'BOND_MSFT_26', 'Microsoft 2.9% 2026', 'BOND', 'FIXED_INCOME', 'Corporate', 'Technology', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC043', 'BOND_JPM_27', 'JPMorgan 4.1% 2027', 'BOND', 'FIXED_INCOME', 'Corporate', 'Banking', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC044', 'BOND_GS_28', 'Goldman Sachs 3.8% 2028', 'BOND', 'FIXED_INCOME', 'Corporate', 'Banking', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC045', 'BOND_JNJ_26', 'J&J 2.5% 2026', 'BOND', 'FIXED_INCOME', 'Corporate', 'Healthcare', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC046', 'BOND_XOM_27', 'Exxon 3.2% 2027', 'BOND', 'FIXED_INCOME', 'Corporate', 'Energy', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC047', 'UST_10Y', 'US Treasury 10-Year', 'BOND', 'FIXED_INCOME', 'Government', 'Treasury', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC048', 'UST_30Y', 'US Treasury 30-Year', 'BOND', 'FIXED_INCOME', 'Government', 'Treasury', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC049', 'HY_BOND_1', 'High Yield Corporate 1', 'BOND', 'FIXED_INCOME', 'High Yield', 'Corporate', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('SEC050', 'HY_BOND_2', 'High Yield Corporate 2', 'BOND', 'FIXED_INCOME', 'High Yield', 'Corporate', 'USD', 'OTC', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

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
('BRK003', 'JPMorgan', 'FULL_SERVICE', 'NORTH_AMERICA', TRUE, 0.0022, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BRK004', 'Interactive Brokers', 'DISCOUNT', 'GLOBAL', TRUE, 0.0005, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BRK005', 'Charles Schwab', 'DISCOUNT', 'NORTH_AMERICA', TRUE, 0.0000, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BRK006', 'Barclays', 'FULL_SERVICE', 'EUROPE', TRUE, 0.0020, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BRK007', 'UBS', 'FULL_SERVICE', 'EUROPE', TRUE, 0.0021, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BRK008', 'Credit Suisse', 'FULL_SERVICE', 'EUROPE', TRUE, 0.0019, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Trades table (HEAVY: 50,000+ trades)
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

-- Generate 5 years of heavy trade data
INSERT INTO trades
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2019-01-01')::DATE AS trade_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1826))  -- 5 years
    WHERE DAYOFWEEK(trade_date) NOT IN (0, 6)  -- Exclude weekends
),
trade_gen AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY p.portfolio_id, d.trade_date, s.security_id, RANDOM()) AS trade_num,
        p.portfolio_id,
        s.security_id,
        b.broker_id,
        d.trade_date,
        DATEADD(day, 2, d.trade_date) AS settlement_date,
        CASE WHEN UNIFORM(0, 1, RANDOM()) > 0.45 THEN 'BUY' ELSE 'SELL' END AS trade_type,
        UNIFORM(100, 5000, RANDOM()) AS quantity,
        CASE
            WHEN s.security_id LIKE 'SEC00%' THEN UNIFORM(100, 500, RANDOM())
            WHEN s.security_id LIKE 'SEC01%' THEN UNIFORM(50, 300, RANDOM())
            WHEN s.security_id LIKE 'SEC02%' THEN UNIFORM(100, 400, RANDOM())
            WHEN s.security_id LIKE 'SEC03%' THEN UNIFORM(200, 500, RANDOM())
            WHEN s.security_id LIKE 'BOND%' OR s.security_id LIKE 'UST%' OR s.security_id LIKE 'HY%' THEN UNIFORM(95, 105, RANDOM())
            ELSE UNIFORM(50, 200, RANDOM())
        END AS price,
        b.commission_rate
    FROM portfolios p
    CROSS JOIN date_spine d
    CROSS JOIN securities s
    CROSS JOIN brokers b
    WHERE d.trade_date >= p.inception_date
    AND UNIFORM(0, 1, RANDOM()) > 0.992  -- ~0.8% of combinations = heavy trades
)
SELECT
    CONCAT('TR', LPAD(trade_num::VARCHAR, 8, '0')) AS trade_id,
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
    ROUND(UNIFORM(1, 15, RANDOM()), 2) AS fees,
    ROUND(
        CASE WHEN trade_type = 'BUY'
        THEN quantity * price + (quantity * price * commission_rate) + UNIFORM(1, 15, RANDOM())
        ELSE -(quantity * price - (quantity * price * commission_rate) - UNIFORM(1, 15, RANDOM()))
        END, 2
    ) AS net_amount,
    'USD' AS currency,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM trade_gen;

-- Market prices table (HEAVY: 5 years × 50 securities × ~250 trading days)
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

-- Generate 5 years of daily price data for all securities
INSERT INTO market_prices
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2019-01-01')::DATE AS price_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1826))
    WHERE DAYOFWEEK(price_date) NOT IN (0, 6)
)
SELECT
    s.security_id,
    d.price_date,
    CASE
        WHEN s.security_id LIKE 'SEC00%' THEN UNIFORM(100, 500, RANDOM())
        WHEN s.security_id LIKE 'SEC01%' THEN UNIFORM(50, 300, RANDOM())
        WHEN s.security_id LIKE 'SEC02%' THEN UNIFORM(100, 400, RANDOM())
        WHEN s.security_id LIKE 'BOND%' OR s.security_id LIKE 'UST%' THEN UNIFORM(95, 105, RANDOM())
        ELSE UNIFORM(50, 200, RANDOM())
    END AS open_price,
    CASE
        WHEN s.security_id LIKE 'SEC00%' THEN UNIFORM(100, 520, RANDOM())
        ELSE UNIFORM(50, 210, RANDOM())
    END AS high_price,
    CASE
        WHEN s.security_id LIKE 'SEC00%' THEN UNIFORM(95, 500, RANDOM())
        ELSE UNIFORM(45, 200, RANDOM())
    END AS low_price,
    CASE
        WHEN s.security_id LIKE 'SEC00%' THEN UNIFORM(100, 500, RANDOM())
        WHEN s.security_id LIKE 'SEC01%' THEN UNIFORM(50, 300, RANDOM())
        WHEN s.security_id LIKE 'SEC02%' THEN UNIFORM(100, 400, RANDOM())
        WHEN s.security_id LIKE 'BOND%' OR s.security_id LIKE 'UST%' THEN UNIFORM(95, 105, RANDOM())
        ELSE UNIFORM(50, 200, RANDOM())
    END AS close_price,
    UNIFORM(1000000, 100000000, RANDOM()) AS volume,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM securities s
CROSS JOIN date_spine d;

-- ============================================================================
-- Pipeline C: Portfolio Analytics Sources (Depends on A and B)
-- ============================================================================

-- Valuations table (HEAVY: 5 years of daily NAV for 20 portfolios)
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

-- Generate 5 years of daily NAV data
INSERT INTO valuations
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2019-01-01')::DATE AS val_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1826))
    WHERE DAYOFWEEK(val_date) NOT IN (0, 6)
)
SELECT
    CONCAT('VAL', LPAD(ROW_NUMBER() OVER (ORDER BY p.portfolio_id, d.val_date)::VARCHAR, 8, '0')) AS valuation_id,
    p.portfolio_id,
    d.val_date AS valuation_date,
    UNIFORM(100000000, 2000000000, RANDOM()) AS nav,
    UNIFORM(95, 150, RANDOM()) AS nav_per_share,
    UNIFORM(1000000, 20000000, RANDOM()) AS shares_outstanding,
    UNIFORM(110000000, 2100000000, RANDOM()) AS gross_assets,
    UNIFORM(5000000, 100000000, RANDOM()) AS total_liabilities,
    UNIFORM(100000000, 2000000000, RANDOM()) AS net_assets,
    'USD' AS currency,
    1.0 AS fx_rate_to_usd,
    UNIFORM(100000000, 2000000000, RANDOM()) AS nav_usd,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM portfolios p
CROSS JOIN date_spine d
WHERE d.val_date >= p.inception_date;

-- Positions daily table (HEAVY: daily position snapshots)
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

-- Generate position snapshots (selected dates for manageability)
INSERT INTO positions_daily
WITH date_spine AS (
    -- Month-end snapshots only for positions (still heavy data)
    SELECT LAST_DAY(DATEADD(month, SEQ4(), '2019-01-01'))::DATE AS pos_date
    FROM TABLE(GENERATOR(ROWCOUNT => 72))  -- 6 years of month-ends
),
positions AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY p.portfolio_id, s.security_id, d.pos_date) AS pos_num,
        p.portfolio_id,
        s.security_id,
        d.pos_date AS position_date,
        UNIFORM(1000, 100000, RANDOM()) AS quantity,
        UNIFORM(50, 300, RANDOM()) AS cost_basis_price,
        UNIFORM(50, 350, RANDOM()) AS market_price
    FROM portfolios p
    CROSS JOIN securities s
    CROSS JOIN date_spine d
    WHERE d.pos_date >= p.inception_date
    AND UNIFORM(0, 1, RANDOM()) > 0.6  -- 40% of portfolio-security combos have positions
)
SELECT
    CONCAT('POS', LPAD(pos_num::VARCHAR, 8, '0')) AS position_id,
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
FROM positions;

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
('BM003', 'NASDAQ 100 Index', 'NDX', 'EQUITY', 'NORTH_AMERICA', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM004', 'Bloomberg US Aggregate Bond Index', 'AGG', 'FIXED_INCOME', 'NORTH_AMERICA', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM005', 'MSCI World Index', 'MXWO', 'EQUITY', 'GLOBAL', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM006', 'MSCI Emerging Markets Index', 'MXEF', 'EQUITY', 'EMERGING_MARKETS', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM007', 'Bloomberg High Yield Corporate Bond Index', 'HY', 'FIXED_INCOME', 'NORTH_AMERICA', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('BM008', 'HFRI Fund Weighted Composite', 'HFRI', 'ALTERNATIVE', 'GLOBAL', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Benchmark returns table (HEAVY: 5 years of daily returns)
CREATE OR REPLACE TABLE benchmark_returns (
    benchmark_id VARCHAR(50),
    return_date DATE,
    daily_return DECIMAL(10,6),
    index_level DECIMAL(18,4),
    total_return_index DECIMAL(18,4),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

INSERT INTO benchmark_returns
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2019-01-01')::DATE AS ret_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1826))
    WHERE DAYOFWEEK(ret_date) NOT IN (0, 6)
)
SELECT
    b.benchmark_id,
    d.ret_date AS return_date,
    UNIFORM(-0.04, 0.04, RANDOM()) AS daily_return,
    CASE b.benchmark_id
        WHEN 'BM001' THEN UNIFORM(3500, 5000, RANDOM())
        WHEN 'BM002' THEN UNIFORM(1500, 2500, RANDOM())
        WHEN 'BM003' THEN UNIFORM(10000, 18000, RANDOM())
        ELSE UNIFORM(1000, 5000, RANDOM())
    END AS index_level,
    CASE b.benchmark_id
        WHEN 'BM001' THEN UNIFORM(7000, 10000, RANDOM())
        ELSE UNIFORM(5000, 15000, RANDOM())
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
-- Equity portfolios use S&P 500
('PF001', 'BM001', TRUE, '2018-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF002', 'BM001', TRUE, '2018-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF003', 'BM002', TRUE, '2019-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF004', 'BM001', TRUE, '2019-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF005', 'BM003', TRUE, '2020-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Fixed income use AGG
('PF006', 'BM004', TRUE, '2018-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF007', 'BM007', TRUE, '2018-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF008', 'BM004', TRUE, '2019-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF009', 'BM007', TRUE, '2019-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF010', 'BM004', TRUE, '2020-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Multi-asset use MSCI World
('PF011', 'BM005', TRUE, '2017-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF012', 'BM006', TRUE, '2018-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF013', 'BM005', TRUE, '2018-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF014', 'BM005', TRUE, '2019-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF015', 'BM005', TRUE, '2019-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Alternatives use HFRI
('PF016', 'BM008', TRUE, '2018-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF017', 'BM008', TRUE, '2018-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF018', 'BM008', TRUE, '2019-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF019', 'BM008', TRUE, '2019-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF020', 'BM008', TRUE, '2020-01-01', NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

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
-- Top level funds
('FD001', 'Bain Capital Public Equity', 'FUND', NULL, 1, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('FD002', 'Bain Capital Credit', 'FUND', NULL, 1, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('FD003', 'Bain Capital Multi-Strategy', 'FUND', NULL, 1, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('FD004', 'Bain Capital Alternatives', 'FUND', NULL, 1, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Strategies under funds
('ST001', 'Large Cap Strategy', 'STRATEGY', 'FD001', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST002', 'Growth Strategy', 'STRATEGY', 'FD001', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST003', 'Small Cap Strategy', 'STRATEGY', 'FD001', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST004', 'Investment Grade Strategy', 'STRATEGY', 'FD002', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST005', 'High Yield Strategy', 'STRATEGY', 'FD002', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST006', 'Balanced Strategy', 'STRATEGY', 'FD003', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST007', 'EM Strategy', 'STRATEGY', 'FD003', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('ST008', 'Real Assets Strategy', 'STRATEGY', 'FD004', 2, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
-- Portfolios under strategies (linking to portfolio table)
('PF001', 'Bain Growth Fund I', 'PORTFOLIO', 'ST002', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF002', 'Bain Value Fund II', 'PORTFOLIO', 'ST001', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF003', 'Bain Small Cap Fund', 'PORTFOLIO', 'ST003', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF004', 'Bain Large Cap Fund', 'PORTFOLIO', 'ST001', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF005', 'Bain Tech Focus Fund', 'PORTFOLIO', 'ST002', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF006', 'Credit Opportunities Fund', 'PORTFOLIO', 'ST004', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF007', 'High Yield Strategy', 'PORTFOLIO', 'ST005', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF008', 'Investment Grade Fund', 'PORTFOLIO', 'ST004', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF009', 'Distressed Debt Fund', 'PORTFOLIO', 'ST005', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF010', 'Convertible Bond Fund', 'PORTFOLIO', 'ST004', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF011', 'Multi-Asset Balanced', 'PORTFOLIO', 'ST006', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF012', 'Emerging Markets Fund', 'PORTFOLIO', 'ST007', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF013', 'Global Macro Fund', 'PORTFOLIO', 'ST006', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF014', 'Tactical Allocation Fund', 'PORTFOLIO', 'ST006', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF015', 'Risk Parity Fund', 'PORTFOLIO', 'ST006', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF016', 'Real Estate Income Fund', 'PORTFOLIO', 'ST008', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF017', 'Infrastructure Fund', 'PORTFOLIO', 'ST008', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF018', 'Private Equity Co-Invest', 'PORTFOLIO', 'ST008', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF019', 'Commodities Fund', 'PORTFOLIO', 'ST008', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('PF020', 'Hedge Fund Solutions', 'PORTFOLIO', 'ST008', 3, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- ============================================================================
-- Verification queries - Show data volumes
-- ============================================================================

SELECT 'DATA VOLUMES CREATED:' AS title;

SELECT 'portfolios' AS table_name, COUNT(*) AS row_count FROM portfolios
UNION ALL SELECT 'cashflows', COUNT(*) FROM cashflows
UNION ALL SELECT 'securities', COUNT(*) FROM securities
UNION ALL SELECT 'brokers', COUNT(*) FROM brokers
UNION ALL SELECT 'trades', COUNT(*) FROM trades
UNION ALL SELECT 'market_prices', COUNT(*) FROM market_prices
UNION ALL SELECT 'valuations', COUNT(*) FROM valuations
UNION ALL SELECT 'positions_daily', COUNT(*) FROM positions_daily
UNION ALL SELECT 'benchmarks', COUNT(*) FROM benchmarks
UNION ALL SELECT 'benchmark_returns', COUNT(*) FROM benchmark_returns
UNION ALL SELECT 'portfolio_benchmarks', COUNT(*) FROM portfolio_benchmarks
UNION ALL SELECT 'fund_hierarchy', COUNT(*) FROM fund_hierarchy
ORDER BY table_name;

SELECT 'Pipeline Dependencies:' AS title;
SELECT 'Pipeline A (Cashflows): No dependencies - uses raw.portfolios, raw.cashflows' AS dependency
UNION ALL SELECT 'Pipeline B (Trading): Depends on Pipeline A fact_cashflow_summary'
UNION ALL SELECT 'Pipeline C (Analytics): Depends on Pipeline A + Pipeline B outputs';

-- Portfolio Performance KPI Query
-- Calculates portfolio-level metrics and performance indicators
-- Optimized: Partitioned by date and clustered by portfolio_id
-- Focuses on recent 90 days to limit scan size

WITH portfolio_holdings AS (
    SELECT
        portfolio_id,
        DATE(timestamp) as transaction_date,
        symbol,
        sector,
        transaction_type,
        
        -- Cumulative quantity (simplistic position calculation)
        SUM(CASE
            WHEN transaction_type = 'BUY' THEN quantity
            WHEN transaction_type = 'SELL' THEN -quantity
            ELSE 0
        END) OVER (PARTITION BY portfolio_id, symbol ORDER BY timestamp) as position_quantity,
        
        -- Cost basis tracking
        SUM(CASE
            WHEN transaction_type = 'BUY' THEN total_amount + fees
            WHEN transaction_type = 'SELL' THEN -(total_amount - fees)
            ELSE 0
        END) OVER (PARTITION BY portfolio_id, symbol ORDER BY timestamp) as cumulative_cost_basis,
        
        CASE WHEN transaction_type = 'DIVIDEND' THEN total_amount ELSE 0 END as dividend_amount
    
    FROM `{dataset_id}.portfolio_transactions`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),

portfolio_summary AS (
    SELECT
        portfolio_id,
        transaction_date,
        COUNT(DISTINCT symbol) as num_positions,
        COUNT(DISTINCT sector) as num_sectors,
        
        -- Investment metrics
        SUM(CASE WHEN transaction_type = 'BUY' THEN total_amount + fees ELSE 0 END) as total_invested,
        SUM(CASE WHEN transaction_type = 'SELL' THEN total_amount - fees ELSE 0 END) as total_divested,
        SUM(CASE WHEN transaction_type = 'DIVIDEND' THEN total_amount ELSE 0 END) as total_dividends,
        
        -- Cost basis
        ROUND(SUM(cumulative_cost_basis), 2) as total_cost_basis,
        
        -- Fees
        SUM(CASE WHEN transaction_type IN ('BUY', 'SELL') THEN fees ELSE 0 END) as total_fees,
        
        -- Sector allocation
        ARRAY_AGG(
            STRUCT(sector, COUNT(*) as sector_count),
            LIMIT 10
        ) as sector_breakdown
    
    FROM portfolio_holdings
    GROUP BY portfolio_id, transaction_date
)

SELECT
    portfolio_id,
    transaction_date,
    num_positions,
    num_sectors,
    total_invested,
    total_divested,
    total_dividends,
    ROUND(total_dividends / NULLIF(total_invested, 0) * 100, 2) as dividend_yield_pct,
    total_cost_basis,
    total_fees,
    ROUND(total_fees / NULLIF(total_invested + total_divested, 0) * 100, 2) as fee_ratio_pct,
    sector_breakdown

FROM portfolio_summary
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY transaction_date DESC, portfolio_id

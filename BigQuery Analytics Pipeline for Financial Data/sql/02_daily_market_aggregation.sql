-- Daily Market Aggregation Query
-- Aggregates trades by symbol and date for KPI generation
-- Optimized: Clustering by symbol and date ensures efficient scans
-- Reduces data volume for downstream analytical queries

SELECT
    DATE(timestamp) as trade_date,
    symbol,
    COUNT(*) as trade_count,
    
    -- Price metrics
    MIN(price) as min_price,
    MAX(price) as max_price,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(STDDEV(price), 4) as price_volatility,
    
    -- Volume metrics
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) as buy_volume,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) as sell_volume,
    SUM(quantity) as total_volume,
    
    -- Value metrics
    SUM(CASE WHEN side = 'BUY' THEN trade_value ELSE 0 END) as buy_value,
    SUM(CASE WHEN side = 'SELL' THEN trade_value ELSE 0 END) as sell_value,
    SUM(trade_value) as total_value,
    
    -- Ratio calculations (optimized with SAFE divide)
    ROUND(SAFE_DIVIDE(
        SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END),
        SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END)
    ), 2) as buy_sell_volume_ratio,
    
    -- Trade distribution
    COUNTIF(side = 'BUY') as buy_trade_count,
    COUNTIF(side = 'SELL') as sell_trade_count,
    
    -- Exchange diversity
    COUNT(DISTINCT exchange) as exchange_count,
    ARRAY_AGG(DISTINCT exchange IGNORE NULLS) as exchanges
    
FROM `{dataset_id}.market_trades`
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY trade_date, symbol
ORDER BY trade_date DESC, total_volume DESC

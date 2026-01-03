-- Stock Price Trend Analysis Query
-- Calculates 7-day, 30-day, and 90-day moving averages with trend detection
-- Optimized: Uses window functions, limited to recent dates, pre-partitioned data
-- Query cost optimization: Processes only necessary columns and date ranges

SELECT
    date,
    symbol,
    close,
    volume,
    -- Calculate moving averages using window functions
    ROUND(AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as sma_7,
    
    ROUND(AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) as sma_30,
    
    ROUND(AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ), 2) as sma_90,
    
    -- Calculate price momentum
    ROUND(
        ((close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)) / 
         LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)) * 100,
        2
    ) as daily_return_pct,
    
    -- Trend classification
    CASE
        WHEN close > AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
        THEN 'UPTREND'
        WHEN close < AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
        THEN 'DOWNTREND'
        ELSE 'NEUTRAL'
    END as trend_status
    
FROM `{dataset_id}.stock_prices`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
AND symbol IN UNNEST(@symbols)
ORDER BY symbol, date DESC

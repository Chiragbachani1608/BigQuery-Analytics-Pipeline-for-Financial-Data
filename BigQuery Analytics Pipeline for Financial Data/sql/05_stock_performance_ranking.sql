-- Comparative Stock Performance Ranking
-- Ranks stocks by various performance metrics for competitive analysis
-- Optimized: Uses window functions efficiently; partitions data by date

SELECT
    date,
    symbol,
    close as closing_price,
    volume,
    price_change_pct,
    
    -- Performance ranking (higher close price change = better)
    RANK() OVER (PARTITION BY date ORDER BY price_change_pct DESC) as performance_rank,
    
    -- Volume ranking
    RANK() OVER (PARTITION BY date ORDER BY volume DESC) as volume_rank,
    
    -- Price change percentile
    PERCENT_RANK() OVER (PARTITION BY date ORDER BY price_change_pct) as performance_percentile,
    
    -- Volatility ranking
    RANK() OVER (PARTITION BY date ORDER BY volatility DESC) as volatility_rank,
    
    -- Top 3 performers check
    CASE
        WHEN RANK() OVER (PARTITION BY date ORDER BY price_change_pct DESC) <= 3
        THEN TRUE
        ELSE FALSE
    END as is_top_performer,
    
    -- Buy/sell signal (simplified momentum-based)
    CASE
        WHEN price_change_pct > 2 AND buy_sell_ratio > 1.1
        THEN 'BUY_SIGNAL'
        WHEN price_change_pct < -2 AND buy_sell_ratio < 0.9
        THEN 'SELL_SIGNAL'
        ELSE 'NEUTRAL'
    END as trading_signal

FROM `{dataset_id}.market_metrics`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY date DESC, performance_rank

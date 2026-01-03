-- Market Volume and Volatility Analysis
-- Identifies high-volatility stocks and volume anomalies
-- Optimized: Uses clustering for symbol filtering; aggregates at market_metrics level when possible

SELECT
    mm.date,
    mm.symbol,
    mm.avg_price,
    mm.price_range,
    mm.volatility,
    mm.total_volume,
    mm.buy_volume,
    mm.sell_volume,
    mm.buy_sell_ratio,
    mm.price_change_pct,
    
    -- Calculate volume anomaly (deviation from 30-day average)
    ROUND(
        mm.total_volume / AVG(mm.total_volume) OVER (
            PARTITION BY mm.symbol
            ORDER BY mm.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ),
        2
    ) as volume_anomaly_factor,
    
    -- Volatility trend (vs 30-day average)
    ROUND(
        mm.volatility / AVG(mm.volatility) OVER (
            PARTITION BY mm.symbol
            ORDER BY mm.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ),
        2
    ) as volatility_anomaly_factor,
    
    -- Risk classification
    CASE
        WHEN mm.volatility > PERCENTILE_CONT(mm.volatility, 0.75) OVER (PARTITION BY mm.symbol)
        THEN 'HIGH_VOLATILITY'
        WHEN mm.volatility < PERCENTILE_CONT(mm.volatility, 0.25) OVER (PARTITION BY mm.symbol)
        THEN 'LOW_VOLATILITY'
        ELSE 'NORMAL'
    END as volatility_classification,
    
    -- Volume classification
    CASE
        WHEN mm.total_volume > PERCENTILE_CONT(mm.total_volume, 0.75) OVER (PARTITION BY mm.symbol)
        THEN 'HIGH_VOLUME'
        WHEN mm.total_volume < PERCENTILE_CONT(mm.total_volume, 0.25) OVER (PARTITION BY mm.symbol)
        THEN 'LOW_VOLUME'
        ELSE 'NORMAL_VOLUME'
    END as volume_classification

FROM `{dataset_id}.market_metrics` mm
WHERE mm.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY mm.date DESC, mm.volatility DESC

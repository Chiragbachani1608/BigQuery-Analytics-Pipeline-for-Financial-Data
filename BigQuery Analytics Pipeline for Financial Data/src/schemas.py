"""
BigQuery table schemas for financial data analytics.
Optimized for analytical queries with appropriate partitioning and clustering.
"""
from typing import List, Dict, Any
from enum import Enum


class TableSchema:
    """Defines BigQuery table schemas and optimization strategies."""
    
    # Stock prices table - Time-series data, partitioned by date, clustered by symbol
    STOCK_PRICES = {
        "table_name": "stock_prices",
        "description": "Daily stock price data with OHLC values",
        "schema": [
            {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "Trading date"},
            {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "Stock ticker symbol"},
            {"name": "open", "type": "FLOAT64", "mode": "REQUIRED", "description": "Opening price"},
            {"name": "high", "type": "FLOAT64", "mode": "REQUIRED", "description": "Highest price"},
            {"name": "low", "type": "FLOAT64", "mode": "REQUIRED", "description": "Lowest price"},
            {"name": "close", "type": "FLOAT64", "mode": "REQUIRED", "description": "Closing price"},
            {"name": "volume", "type": "INT64", "mode": "REQUIRED", "description": "Trading volume"},
            {"name": "adjusted_close", "type": "FLOAT64", "mode": "REQUIRED", "description": "Adjusted closing price"},
        ],
        "partitioning": {"type": "TIME", "field": "date", "expiration_ms": None},
        "clustering": {"fields": ["symbol", "date"]},
        "optimization_notes": "Partitioned by date for date-range queries; clustered by symbol for efficient symbol lookups"
    }
    
    # Market trades - Transaction-level data
    MARKET_TRADES = {
        "table_name": "market_trades",
        "description": "Individual trade transactions with timestamp precision",
        "schema": [
            {"name": "trade_id", "type": "STRING", "mode": "REQUIRED", "description": "Unique trade identifier"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Trade execution time (UTC)"},
            {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "Trade date"},
            {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "Stock ticker symbol"},
            {"name": "price", "type": "FLOAT64", "mode": "REQUIRED", "description": "Trade price"},
            {"name": "quantity", "type": "INT64", "mode": "REQUIRED", "description": "Quantity traded"},
            {"name": "side", "type": "STRING", "mode": "REQUIRED", "description": "BUY or SELL"},
            {"name": "trade_value", "type": "FLOAT64", "mode": "REQUIRED", "description": "Total trade value (price * quantity)"},
            {"name": "exchange", "type": "STRING", "mode": "REQUIRED", "description": "Trading exchange"},
        ],
        "partitioning": {"type": "TIME", "field": "date", "expiration_ms": None},
        "clustering": {"fields": ["symbol", "date", "side"]},
        "optimization_notes": "Fine-grained partitioning by date; clustered for fast symbol/side filtering"
    }
    
    # Market metrics - Aggregated KPIs
    MARKET_METRICS = {
        "table_name": "market_metrics",
        "description": "Aggregated market-level metrics and indicators",
        "schema": [
            {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "Metric date"},
            {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "Stock ticker symbol"},
            {"name": "avg_price", "type": "FLOAT64", "mode": "REQUIRED", "description": "Average price for the day"},
            {"name": "price_range", "type": "FLOAT64", "mode": "REQUIRED", "description": "High - Low range"},
            {"name": "volatility", "type": "FLOAT64", "mode": "REQUIRED", "description": "Price volatility (std dev)"},
            {"name": "total_volume", "type": "INT64", "mode": "REQUIRED", "description": "Total daily volume"},
            {"name": "buy_volume", "type": "INT64", "mode": "REQUIRED", "description": "Total buy volume"},
            {"name": "sell_volume", "type": "INT64", "mode": "REQUIRED", "description": "Total sell volume"},
            {"name": "buy_sell_ratio", "type": "FLOAT64", "mode": "REQUIRED", "description": "Ratio of buy to sell volume"},
            {"name": "price_change_pct", "type": "FLOAT64", "mode": "REQUIRED", "description": "Percentage change from previous close"},
            {"name": "sma_20", "type": "FLOAT64", "mode": "NULLABLE", "description": "20-day simple moving average"},
            {"name": "sma_50", "type": "FLOAT64", "mode": "NULLABLE", "description": "50-day simple moving average"},
            {"name": "rsi_14", "type": "FLOAT64", "mode": "NULLABLE", "description": "14-day relative strength index"},
            {"name": "market_cap_change", "type": "FLOAT64", "mode": "NULLABLE", "description": "Change in market cap estimate"},
        ],
        "partitioning": {"type": "TIME", "field": "date", "expiration_ms": None},
        "clustering": {"fields": ["symbol", "date"]},
        "optimization_notes": "Pre-aggregated data reduces scan size for analytical queries"
    }
    
    # Portfolio transactions - User transactions
    PORTFOLIO_TRANSACTIONS = {
        "table_name": "portfolio_transactions",
        "description": "User portfolio transaction history",
        "schema": [
            {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED", "description": "Unique transaction ID"},
            {"name": "portfolio_id", "type": "STRING", "mode": "REQUIRED", "description": "Portfolio identifier"},
            {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "Transaction date"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Transaction timestamp"},
            {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "Stock ticker"},
            {"name": "transaction_type", "type": "STRING", "mode": "REQUIRED", "description": "BUY, SELL, or DIVIDEND"},
            {"name": "quantity", "type": "INT64", "mode": "REQUIRED", "description": "Quantity"},
            {"name": "price", "type": "FLOAT64", "mode": "REQUIRED", "description": "Price per share"},
            {"name": "total_amount", "type": "FLOAT64", "mode": "REQUIRED", "description": "Total transaction amount"},
            {"name": "fees", "type": "FLOAT64", "mode": "REQUIRED", "description": "Transaction fees"},
            {"name": "sector", "type": "STRING", "mode": "REQUIRED", "description": "Industry sector"},
        ],
        "partitioning": {"type": "TIME", "field": "date", "expiration_ms": None},
        "clustering": {"fields": ["portfolio_id", "symbol", "date"]},
        "optimization_notes": "Clustered by portfolio for cross-holding analysis"
    }
    
    @classmethod
    def get_all_schemas(cls) -> Dict[str, Dict[str, Any]]:
        """Returns all table schemas."""
        return {
            "stock_prices": cls.STOCK_PRICES,
            "market_trades": cls.MARKET_TRADES,
            "market_metrics": cls.MARKET_METRICS,
            "portfolio_transactions": cls.PORTFOLIO_TRANSACTIONS,
        }
    
    @classmethod
    def get_create_table_ddl(cls, schema_dict: Dict[str, Any], dataset_id: str) -> str:
        """Generates BigQuery CREATE TABLE DDL statement."""
        table_name = schema_dict["table_name"]
        description = schema_dict["description"]
        fields = schema_dict["schema"]
        clustering = schema_dict.get("clustering", {})
        
        # Build column definitions
        columns = []
        for field in fields:
            nullable = "NULLABLE" if field["mode"] == "NULLABLE" else "NOT NULL"
            columns.append(f"  {field['name']} {field['type']} {nullable}")
        
        columns_def = ",\n".join(columns)
        
        # Build DDL
        ddl = f"""CREATE TABLE IF NOT EXISTS `{dataset_id}.{table_name}` (
{columns_def}
)
PARTITION BY DATE(date)
CLUSTER BY {', '.join(clustering['fields'])}
OPTIONS(
  description="{description}",
  require_partition_filter=TRUE
);"""
        
        return ddl

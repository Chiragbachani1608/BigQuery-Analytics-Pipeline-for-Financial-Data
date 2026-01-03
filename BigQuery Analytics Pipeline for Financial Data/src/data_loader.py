"""
Data ingestion module for loading financial data into BigQuery.
Handles sample data generation and streaming/batch loading.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataGenerator:
    """Generates realistic financial market data for testing and demo purposes."""
    
    def __init__(self, seed: int = 42):
        np.random.seed(seed)
        self.symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "JPM"]
        self.exchanges = ["NASDAQ", "NYSE"]
        self.sectors = {"AAPL": "Technology", "GOOGL": "Technology", "MSFT": "Technology",
                       "AMZN": "Consumer", "TSLA": "Automotive", "META": "Technology",
                       "NVDA": "Technology", "JPM": "Finance"}
    
    def generate_stock_prices(self, days: int = 90) -> pd.DataFrame:
        """Generate historical stock price data (OHLC)."""
        records = []
        base_date = datetime.now().date() - timedelta(days=days)
        
        # Initialize price for each symbol
        base_prices = {symbol: np.random.uniform(50, 500) for symbol in self.symbols}
        
        for day_offset in range(days):
            current_date = base_date + timedelta(days=day_offset)
            
            # Skip weekends
            if current_date.weekday() >= 5:
                continue
            
            for symbol in self.symbols:
                base_price = base_prices[symbol]
                
                # Generate realistic OHLC data with slight daily variation
                daily_return = np.random.normal(0.0005, 0.02)
                open_price = base_price * (1 + daily_return)
                
                # High and low around open
                high = open_price * (1 + abs(np.random.normal(0, 0.01)))
                low = open_price * (1 - abs(np.random.normal(0, 0.01)))
                close = np.random.uniform(low, high)
                
                # Adjust base price for next day
                base_prices[symbol] = close
                
                volume = int(np.random.uniform(1_000_000, 100_000_000))
                
                records.append({
                    "date": current_date,
                    "symbol": symbol,
                    "open": round(open_price, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "close": round(close, 2),
                    "volume": volume,
                    "adjusted_close": round(close * 0.99, 2),  # Simplified adjustment
                })
        
        return pd.DataFrame(records)
    
    def generate_market_trades(self, stock_prices_df: pd.DataFrame, trades_per_day: int = 500) -> pd.DataFrame:
        """Generate individual trade transactions."""
        records = []
        trade_id_counter = 1
        
        for _, row in stock_prices_df.iterrows():
            date = row["date"]
            symbol = row["symbol"]
            price_range = (row["low"], row["high"])
            
            for _ in range(trades_per_day):
                # Random trade time during market hours (9:30 AM - 4:00 PM ET)
                hour = np.random.randint(9, 16)
                minute = np.random.randint(0, 59)
                second = np.random.randint(0, 59)
                
                timestamp = datetime.combine(date, datetime.min.time().replace(hour=hour, minute=minute, second=second))
                
                # Trade price within day range
                trade_price = np.random.uniform(*price_range)
                quantity = int(np.random.lognormal(10, 1.5))  # Lognormal for realistic volume
                side = np.random.choice(["BUY", "SELL"])
                
                records.append({
                    "trade_id": f"TRADE_{trade_id_counter:010d}",
                    "timestamp": timestamp,
                    "date": date,
                    "symbol": symbol,
                    "price": round(trade_price, 2),
                    "quantity": quantity,
                    "side": side,
                    "trade_value": round(trade_price * quantity, 2),
                    "exchange": np.random.choice(self.exchanges),
                })
                trade_id_counter += 1
        
        return pd.DataFrame(records)
    
    def generate_market_metrics(self, stock_prices_df: pd.DataFrame, trades_df: pd.DataFrame) -> pd.DataFrame:
        """Generate aggregated market metrics and technical indicators."""
        records = []
        
        # Group by date and symbol
        for (date, symbol), group in stock_prices_df.groupby(["date", "symbol"]):
            row = group.iloc[0]
            
            # Filter trades for this date and symbol
            trades = trades_df[(trades_df["date"] == date) & (trades_df["symbol"] == symbol)]
            
            if len(trades) == 0:
                continue
            
            # Calculate metrics
            avg_price = (row["open"] + row["close"] + row["high"] + row["low"]) / 4
            price_range = row["high"] - row["low"]
            volatility = np.random.uniform(0.01, 0.05)  # Simplified
            
            buy_volume = trades[trades["side"] == "BUY"]["quantity"].sum()
            sell_volume = trades[trades["side"] == "SELL"]["quantity"].sum()
            total_volume = buy_volume + sell_volume
            
            buy_sell_ratio = buy_volume / sell_volume if sell_volume > 0 else 0
            price_change_pct = ((row["close"] - row["open"]) / row["open"] * 100) if row["open"] > 0 else 0
            
            records.append({
                "date": date,
                "symbol": symbol,
                "avg_price": round(avg_price, 2),
                "price_range": round(price_range, 2),
                "volatility": round(volatility, 4),
                "total_volume": total_volume,
                "buy_volume": buy_volume,
                "sell_volume": sell_volume,
                "buy_sell_ratio": round(buy_sell_ratio, 2),
                "price_change_pct": round(price_change_pct, 2),
                "sma_20": None,  # Would be calculated with more historical data
                "sma_50": None,
                "rsi_14": round(np.random.uniform(30, 70), 2),  # Simplified
                "market_cap_change": round(np.random.uniform(-10, 10), 2),
            })
        
        return pd.DataFrame(records)
    
    def generate_portfolio_transactions(self, num_transactions: int = 1000) -> pd.DataFrame:
        """Generate user portfolio transaction history."""
        records = []
        
        base_date = datetime.now().date() - timedelta(days=180)
        portfolio_ids = [f"PORT_{i:06d}" for i in range(1, 51)]  # 50 portfolios
        
        for i in range(num_transactions):
            date = base_date + timedelta(days=np.random.randint(0, 180))
            symbol = np.random.choice(self.symbols)
            transaction_type = np.random.choice(["BUY", "SELL", "DIVIDEND"], p=[0.5, 0.4, 0.1])
            
            quantity = int(np.random.lognormal(5, 1.2)) if transaction_type != "DIVIDEND" else 0
            price = np.random.uniform(50, 500)
            
            if transaction_type == "DIVIDEND":
                total_amount = np.random.uniform(100, 5000)
                quantity = 0
            else:
                total_amount = quantity * price
            
            records.append({
                "transaction_id": f"TXN_{i:010d}",
                "portfolio_id": np.random.choice(portfolio_ids),
                "date": date,
                "timestamp": datetime.combine(date, datetime.min.time()),
                "symbol": symbol,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "price": round(price, 2),
                "total_amount": round(total_amount, 2),
                "fees": round(np.random.uniform(0, 50), 2),
                "sector": self.sectors.get(symbol, "Other"),
            })
        
        return pd.DataFrame(records)


class BigQueryDataLoader:
    """Handles loading data into BigQuery tables."""
    
    def __init__(self, client):
        """
        Initialize with BigQuery client.
        
        Args:
            client: google.cloud.bigquery.Client instance
        """
        self.client = client
        logger.info("BigQueryDataLoader initialized")
    
    def load_dataframe_to_bq(
        self,
        dataframe: pd.DataFrame,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
        job_config_kwargs: Dict[str, Any] = None
    ) -> None:
        """
        Load pandas DataFrame to BigQuery table.
        
        Args:
            dataframe: Pandas DataFrame to load
            table_id: Fully qualified table ID (project.dataset.table)
            write_disposition: WRITE_TRUNCATE or WRITE_APPEND
            job_config_kwargs: Additional job configuration options
        """
        from google.cloud.bigquery import LoadJobConfig
        
        job_config = LoadJobConfig(write_disposition=write_disposition)
        
        if job_config_kwargs:
            for key, value in job_config_kwargs.items():
                setattr(job_config, key, value)
        
        try:
            job = self.client.load_table_from_dataframe(
                dataframe,
                table_id,
                job_config=job_config
            )
            job.result()  # Wait for job to complete
            logger.info(f"Loaded {len(dataframe)} rows to {table_id}")
        except Exception as e:
            logger.error(f"Failed to load data to {table_id}: {str(e)}")
            raise
    
    def create_tables_if_not_exist(self, dataset_id: str, schemas: Dict[str, Any]) -> None:
        """
        Create BigQuery tables if they don't exist.
        
        Args:
            dataset_id: Dataset ID
            schemas: Dictionary of schema definitions
        """
        from google.cloud.bigquery import Schema, SchemaField, Table, TimePartitioning, Clustering
        
        for schema_name, schema_def in schemas.items():
            table_id = f"{self.client.project}.{dataset_id}.{schema_def['table_name']}"
            
            # Check if table exists
            try:
                self.client.get_table(table_id)
                logger.info(f"Table {table_id} already exists")
                continue
            except Exception:
                pass  # Table doesn't exist, will create it
            
            # Build schema
            schema_fields = [
                SchemaField(field["name"], field["type"], mode=field.get("mode", "NULLABLE"))
                for field in schema_def["schema"]
            ]
            
            # Create table
            table = Table(table_id, schema=Schema(schema_fields))
            
            # Add partitioning
            if "partitioning" in schema_def:
                partitioning_def = schema_def["partitioning"]
                table.time_partitioning = TimePartitioning(
                    type_=partitioning_def["type"],
                    field=partitioning_def["field"],
                    expiration_ms=partitioning_def.get("expiration_ms")
                )
            
            # Add clustering
            if "clustering" in schema_def:
                clustering_def = schema_def["clustering"]
                table.clustering_fields = clustering_def["fields"]
            
            # Create the table
            table = self.client.create_table(table)
            logger.info(f"Created table {table_id}")

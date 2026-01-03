"""
Test module for data generation and loading.
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.data_loader import DataGenerator


class TestDataGenerator:
    """Test suite for financial data generation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = DataGenerator(seed=42)
    
    def test_stock_prices_generation(self):
        """Test stock price data generation."""
        df = self.generator.generate_stock_prices(days=30)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "date" in df.columns
        assert "symbol" in df.columns
        assert "open" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
    
    def test_market_trades_generation(self):
        """Test market trade data generation."""
        stock_prices = self.generator.generate_stock_prices(days=10)
        trades = self.generator.generate_market_trades(stock_prices, trades_per_day=50)
        
        assert len(trades) > 0
        assert "trade_id" in trades.columns
        assert "timestamp" in trades.columns
        assert "symbol" in trades.columns
        assert "side" in trades.columns
        assert trades["side"].isin(["BUY", "SELL"]).all()
    
    def test_market_metrics_generation(self):
        """Test market metrics aggregation."""
        stock_prices = self.generator.generate_stock_prices(days=10)
        trades = self.generator.generate_market_trades(stock_prices, trades_per_day=50)
        metrics = self.generator.generate_market_metrics(stock_prices, trades)
        
        assert len(metrics) > 0
        assert "volatility" in metrics.columns
        assert "buy_volume" in metrics.columns
        assert "sell_volume" in metrics.columns
        assert "buy_sell_ratio" in metrics.columns
    
    def test_portfolio_transactions_generation(self):
        """Test portfolio transaction generation."""
        txns = self.generator.generate_portfolio_transactions(num_transactions=100)
        
        assert len(txns) == 100
        assert "portfolio_id" in txns.columns
        assert "symbol" in txns.columns
        assert "transaction_type" in txns.columns
        assert txns["transaction_type"].isin(["BUY", "SELL", "DIVIDEND"]).all()
    
    def test_data_consistency(self):
        """Test data consistency across generated datasets."""
        stock_prices = self.generator.generate_stock_prices(days=10)
        
        # Verify OHLC relationships
        assert (stock_prices["high"] >= stock_prices["low"]).all()
        assert (stock_prices["high"] >= stock_prices["open"]).all()
        assert (stock_prices["high"] >= stock_prices["close"]).all()
    
    def test_reproducibility(self):
        """Test that seed produces reproducible results."""
        gen1 = DataGenerator(seed=42)
        gen2 = DataGenerator(seed=42)
        
        df1 = gen1.generate_stock_prices(days=5)
        df2 = gen2.generate_stock_prices(days=5)
        
        assert df1.equals(df2)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

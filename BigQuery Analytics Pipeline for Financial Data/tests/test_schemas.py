"""
Test module for schemas and optimization strategies.
"""
import pytest
from src.schemas import TableSchema


class TestSchemas:
    """Test suite for table schema definitions."""
    
    def test_stock_prices_schema(self):
        """Verify stock_prices table schema."""
        schema = TableSchema.STOCK_PRICES
        
        assert schema["table_name"] == "stock_prices"
        assert len(schema["schema"]) == 8
        assert schema["partitioning"]["type"] == "TIME"
        assert "symbol" in schema["clustering"]["fields"]
    
    def test_market_trades_schema(self):
        """Verify market_trades table schema."""
        schema = TableSchema.MARKET_TRADES
        
        assert schema["table_name"] == "market_trades"
        assert any(f["name"] == "trade_id" for f in schema["schema"])
        assert schema["clustering"]["fields"] == ["symbol", "date", "side"]
    
    def test_market_metrics_schema(self):
        """Verify market_metrics table schema."""
        schema = TableSchema.MARKET_METRICS
        
        assert schema["table_name"] == "market_metrics"
        assert len(schema["schema"]) == 14
        assert "sma_20" in [f["name"] for f in schema["schema"]]
    
    def test_portfolio_transactions_schema(self):
        """Verify portfolio_transactions table schema."""
        schema = TableSchema.PORTFOLIO_TRANSACTIONS
        
        assert schema["table_name"] == "portfolio_transactions"
        assert "portfolio_id" in [f["name"] for f in schema["schema"]]
        assert "sector" in [f["name"] for f in schema["schema"]]
    
    def test_all_schemas_accessible(self):
        """Test that all schemas are accessible."""
        schemas = TableSchema.get_all_schemas()
        
        assert len(schemas) == 4
        assert "stock_prices" in schemas
        assert "market_trades" in schemas
        assert "market_metrics" in schemas
        assert "portfolio_transactions" in schemas
    
    def test_partitioning_strategy(self):
        """Verify partitioning is configured correctly."""
        for schema_name, schema in TableSchema.get_all_schemas().items():
            assert "partitioning" in schema
            assert schema["partitioning"]["type"] == "TIME"
            assert schema["partitioning"]["field"] == "date"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

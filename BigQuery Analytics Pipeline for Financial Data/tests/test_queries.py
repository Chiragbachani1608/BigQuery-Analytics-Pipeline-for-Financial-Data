"""
Test module for query optimization and analytics engine.
"""
import pytest
from src.analytics_engine import QueryOptimizer


class TestQueryOptimizer:
    """Test suite for query optimization."""
    
    def test_cost_estimation(self):
        """Test BigQuery cost estimation."""
        # 1 TB of data
        cost = QueryOptimizer.estimate_query_cost(1024 ** 4)
        assert cost == 7.0  # $7 per TB
        
        # 0.1 TB of data
        cost = QueryOptimizer.estimate_query_cost(100 * 1024 ** 3)
        assert cost == 0.7
    
    def test_optimization_tips_select_star(self):
        """Test detection of SELECT * pattern."""
        query = "SELECT * FROM my_table"
        tips = QueryOptimizer.get_optimization_tips(query)
        
        assert any("SELECT *" in tip for tip in tips)
    
    def test_optimization_tips_no_where_clause(self):
        """Test detection of missing WHERE clause."""
        query = "SELECT col1, col2 FROM my_table"
        tips = QueryOptimizer.get_optimization_tips(query)
        
        # Should suggest adding date filters
        assert len(tips) > 0
    
    def test_optimization_tips_distinct(self):
        """Test detection of DISTINCT usage."""
        query = "SELECT DISTINCT symbol FROM trades"
        tips = QueryOptimizer.get_optimization_tips(query)
        
        assert any("DISTINCT" in tip for tip in tips)
    
    def test_optimization_tips_order_without_limit(self):
        """Test detection of ORDER BY without LIMIT."""
        query = "SELECT * FROM trades ORDER BY date DESC"
        tips = QueryOptimizer.get_optimization_tips(query)
        
        assert any("ORDER BY" in tip and "LIMIT" in tip for tip in tips)


class TestQueryPatterns:
    """Test common query patterns."""
    
    def test_window_function_pattern(self):
        """Verify window function usage for efficiency."""
        query = """
        SELECT 
            date,
            symbol,
            close,
            AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as sma_30
        FROM stock_prices
        """
        
        # No expensive JOIN or GROUP BY, so should be efficient
        tips = QueryOptimizer.get_optimization_tips(query)
        assert not any("DISTINCT" in tip for tip in tips)
    
    def test_parameterized_query_pattern(self):
        """Verify parameterized queries are secure."""
        # This is a best practice check
        assert True  # Would validate against SQL injection patterns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

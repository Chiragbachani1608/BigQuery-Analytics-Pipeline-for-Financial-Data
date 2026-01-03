"""
BigQuery Analytics Engine
Provides query execution, caching, and result optimization for financial analytics.
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


class QueryOptimizer:
    """Provides query optimization recommendations and cost analysis."""
    
    @staticmethod
    def estimate_query_cost(bytes_processed: int) -> float:
        """
        Estimate query cost based on bytes processed.
        BigQuery: $7 per TB (as of 2026)
        
        Args:
            bytes_processed: Bytes scanned by the query
            
        Returns:
            Estimated cost in USD
        """
        TB_COST = 7.0  # USD per TB
        tb_processed = bytes_processed / (1024 ** 4)
        return tb_processed * TB_COST
    
    @staticmethod
    def get_optimization_tips(query: str) -> List[str]:
        """Provide query optimization suggestions."""
        tips = []
        query_upper = query.upper()
        
        if "SELECT *" in query_upper:
            tips.append("⚠️  SELECT * detected: specify only needed columns to reduce scan size")
        
        if "WITHOUT PARTITION FILTER" not in query_upper and "WHERE" not in query_upper:
            tips.append("⚠️  No WHERE clause: add date filters to leverage partitioning")
        
        if "TEMP TABLE" in query_upper or "CREATE TABLE" in query_upper:
            tips.append("✓ Materialized views or scheduled queries could improve performance")
        
        if "DISTINCT" in query_upper:
            tips.append("💡 DISTINCT can be expensive: use GROUP BY when possible")
        
        if "ORDER BY" in query_upper and "LIMIT" not in query_upper:
            tips.append("⚠️  ORDER BY without LIMIT: consider adding LIMIT if not needed for full sort")
        
        return tips


class BigQueryAnalytics:
    """Main analytics engine for executing queries and processing results."""
    
    def __init__(self, client, dataset_id: str, sql_dir: str = "./sql"):
        """
        Initialize analytics engine.
        
        Args:
            client: google.cloud.bigquery.Client instance
            dataset_id: BigQuery dataset ID
            sql_dir: Directory containing SQL query files
        """
        self.client = client
        self.dataset_id = dataset_id
        self.sql_dir = Path(sql_dir)
        self.query_cache: Dict[str, pd.DataFrame] = {}
        self.query_optimizer = QueryOptimizer()
        logger.info(f"BigQueryAnalytics initialized for dataset: {dataset_id}")
    
    def load_query_template(self, query_name: str) -> str:
        """
        Load SQL query from file and apply dataset substitutions.
        
        Args:
            query_name: Name of the query file (e.g., 'stock_trend_analysis')
            
        Returns:
            Query string with placeholders replaced
        """
        # Find matching query file
        query_files = list(self.sql_dir.glob(f"*{query_name}*.sql"))
        
        if not query_files:
            raise FileNotFoundError(f"Query file for '{query_name}' not found in {self.sql_dir}")
        
        query_file = query_files[0]
        
        with open(query_file, 'r') as f:
            query = f.read()
        
        # Replace dataset placeholder
        query = query.replace("{dataset_id}", self.dataset_id)
        
        logger.info(f"Loaded query template: {query_file.name}")
        return query
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        use_cache: bool = False,
        dry_run: bool = False,
        max_results: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Execute BigQuery query with optional caching and optimization.
        
        Args:
            query: SQL query string
            params: Query parameters for parameterized queries
            use_cache: Whether to cache results
            dry_run: If True, estimate bytes without executing
            max_results: Maximum results to return
            
        Returns:
            Results as pandas DataFrame
        """
        from google.cloud.bigquery import ScalarQueryParameter, job
        
        # Check cache
        cache_key = query + str(params)
        if use_cache and cache_key in self.query_cache:
            logger.info("Returning cached query results")
            return self.query_cache[cache_key]
        
        # Configure job
        job_config = job.QueryJobConfig(use_legacy_sql=False)
        
        # Add parameters if provided
        if params:
            query_params = []
            for key, value in params.items():
                if isinstance(value, list):
                    # For array parameters
                    query_params.append(ScalarQueryParameter(key, "STRING", ",".join(value)))
                else:
                    query_params.append(ScalarQueryParameter(key, "STRING", str(value)))
            job_config.query_parameters = query_params
        
        try:
            # Dry run to estimate cost
            if dry_run:
                job_config.dry_run = True
                query_job = self.client.query(query, job_config=job_config)
                bytes_processed = query_job.total_bytes_processed or 0
                cost = self.query_optimizer.estimate_query_cost(bytes_processed)
                
                logger.info(f"DRY RUN - Bytes processed: {bytes_processed:,} (~${cost:.4f})")
                logger.info(f"Optimization tips: {self.query_optimizer.get_optimization_tips(query)}")
                return pd.DataFrame()
            
            # Execute query
            logger.info("Executing query...")
            query_job = self.client.query(query, job_config=job_config)
            
            # Get results
            if max_results:
                results = query_job.result(max_results=max_results)
            else:
                results = query_job.result()
            
            df = results.to_dataframe()
            
            # Log statistics
            bytes_processed = query_job.total_bytes_processed or 0
            cost = self.query_optimizer.estimate_query_cost(bytes_processed)
            logger.info(f"Query completed - Rows: {len(df):,}, Bytes: {bytes_processed:,}, Cost: ${cost:.4f}")
            
            # Cache results
            if use_cache:
                self.query_cache[cache_key] = df
            
            return df
        
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def get_stock_trends(self, symbols: List[str] = None, days: int = 90) -> pd.DataFrame:
        """Get stock trend analysis with moving averages."""
        query = self.load_query_template("stock_trend")
        
        params = {
            "symbols": symbols or ["AAPL", "GOOGL", "MSFT"],
        }
        
        return self.execute_query(query, params=params)
    
    def get_daily_market_aggregation(self, days: int = 90) -> pd.DataFrame:
        """Get daily market aggregation metrics."""
        query = self.load_query_template("daily_market_aggregation")
        return self.execute_query(query)
    
    def get_portfolio_kpi(self) -> pd.DataFrame:
        """Get portfolio performance KPIs."""
        query = self.load_query_template("portfolio_performance")
        return self.execute_query(query)
    
    def get_volatility_analysis(self) -> pd.DataFrame:
        """Get volatility and volume analysis."""
        query = self.load_query_template("volatility_volume")
        return self.execute_query(query)
    
    def get_performance_ranking(self) -> pd.DataFrame:
        """Get stock performance rankings."""
        query = self.load_query_template("stock_performance_ranking")
        return self.execute_query(query)
    
    def get_custom_query(
        self,
        query: str,
        use_cache: bool = False,
        dry_run: bool = False
    ) -> pd.DataFrame:
        """Execute a custom SQL query."""
        return self.execute_query(query, use_cache=use_cache, dry_run=dry_run)
    
    def export_to_csv(self, df: pd.DataFrame, filename: str, output_dir: str = "./data") -> str:
        """
        Export DataFrame to CSV file.
        
        Args:
            df: DataFrame to export
            filename: Output filename
            output_dir: Output directory
            
        Returns:
            Full path to exported file
        """
        output_path = Path(output_dir) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(output_path, index=False)
        logger.info(f"Exported {len(df)} rows to {output_path}")
        
        return str(output_path)
    
    def get_query_statistics(self) -> Dict[str, Any]:
        """Get statistics about cached queries."""
        return {
            "cached_queries": len(self.query_cache),
            "cache_info": {
                query[:100]: len(df) for query, df in self.query_cache.items()
            }
        }
    
    def clear_cache(self) -> None:
        """Clear query cache."""
        self.query_cache.clear()
        logger.info("Query cache cleared")

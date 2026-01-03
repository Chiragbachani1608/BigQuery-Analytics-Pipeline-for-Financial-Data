"""
BigQuery Analytics Pipeline for Financial Data
Enterprise-grade analytics platform for market and portfolio analysis
"""

__version__ = "1.0.0"
__author__ = "Analytics Team"

from src.config import BigQueryConfig
from src.schemas import TableSchema
from src.data_loader import DataGenerator, BigQueryDataLoader
from src.analytics_engine import BigQueryAnalytics, QueryOptimizer
from src.looker_exporter import LookerDashboardBuilder, LookMLGenerator

__all__ = [
    "BigQueryConfig",
    "TableSchema",
    "DataGenerator",
    "BigQueryDataLoader",
    "BigQueryAnalytics",
    "QueryOptimizer",
    "LookerDashboardBuilder",
    "LookMLGenerator",
]

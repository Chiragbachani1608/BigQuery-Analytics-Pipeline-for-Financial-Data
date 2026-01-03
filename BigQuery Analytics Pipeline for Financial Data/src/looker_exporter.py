"""
Looker dashboard and visualization configuration generator.
Exports dashboard JSON configurations and LookML definitions.
"""
import json
from typing import List, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class LookerDashboardBuilder:
    """Builds Looker dashboard configurations for financial analytics."""
    
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dashboards = {}
    
    def create_stock_performance_dashboard(self) -> Dict[str, Any]:
        """Create dashboard for stock performance monitoring."""
        dashboard = {
            "name": "Stock Performance Dashboard",
            "title": "Daily Stock Performance & Trends",
            "description": "Real-time monitoring of stock prices, trends, and performance metrics",
            "elements": [
                {
                    "type": "looker_line",
                    "title": "Stock Price Trends (90 Days)",
                    "query": f"SELECT date, symbol, close FROM `{self.project_id}.{self.dataset_id}.stock_prices` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) ORDER BY date",
                    "dimensions": ["date", "symbol"],
                    "measures": ["close"],
                    "filters": [
                        {"field": "symbol", "condition": {"value": ["AAPL", "GOOGL", "MSFT", "AMZN"]}}
                    ]
                },
                {
                    "type": "looker_column",
                    "title": "Daily Trading Volume",
                    "query": f"SELECT DATE(timestamp) as date, symbol, SUM(quantity) as volume FROM `{self.project_id}.{self.dataset_id}.market_trades` WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) GROUP BY date, symbol",
                    "dimensions": ["date", "symbol"],
                    "measures": ["volume"]
                },
                {
                    "type": "looker_scatter",
                    "title": "Risk vs. Return (Volatility vs. Price Change)",
                    "query": f"SELECT symbol, volatility, price_change_pct FROM `{self.project_id}.{self.dataset_id}.market_metrics` WHERE date = CURRENT_DATE() - 1",
                    "dimensions": ["symbol"],
                    "measures": ["volatility", "price_change_pct"]
                },
                {
                    "type": "looker_table",
                    "title": "Top Performers",
                    "query": f"SELECT symbol, price_change_pct, total_volume, buy_sell_ratio FROM `{self.project_id}.{self.dataset_id}.market_metrics` WHERE date = CURRENT_DATE() - 1 ORDER BY price_change_pct DESC LIMIT 10",
                }
            ]
        }
        return dashboard
    
    def create_market_analysis_dashboard(self) -> Dict[str, Any]:
        """Create comprehensive market analysis dashboard."""
        dashboard = {
            "name": "Market Analysis Dashboard",
            "title": "Market-Wide Analysis & Insights",
            "description": "Aggregated market metrics, volume analysis, and volatility tracking",
            "elements": [
                {
                    "type": "looker_column",
                    "title": "Market Volume by Date",
                    "query": f"SELECT date, SUM(total_volume) as total_market_volume FROM `{self.project_id}.{self.dataset_id}.market_metrics` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) GROUP BY date ORDER BY date",
                },
                {
                    "type": "looker_line",
                    "title": "Volatility Trends",
                    "query": f"SELECT date, symbol, volatility FROM `{self.project_id}.{self.dataset_id}.market_metrics` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) ORDER BY date",
                    "measures": ["volatility"]
                },
                {
                    "type": "looker_pie",
                    "title": "Buy/Sell Distribution",
                    "query": f"SELECT SUM(buy_volume) as buy_volume, SUM(sell_volume) as sell_volume FROM `{self.project_id}.{self.dataset_id}.market_metrics` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)",
                },
                {
                    "type": "looker_gauge",
                    "title": "Market Health Index",
                    "query": f"SELECT AVG(CASE WHEN price_change_pct > 0 THEN 1 ELSE 0 END) * 100 as positive_count FROM `{self.project_id}.{self.dataset_id}.market_metrics` WHERE date = CURRENT_DATE() - 1",
                }
            ]
        }
        return dashboard
    
    def create_portfolio_dashboard(self) -> Dict[str, Any]:
        """Create portfolio monitoring dashboard."""
        dashboard = {
            "name": "Portfolio Dashboard",
            "title": "Portfolio Performance & Risk Analysis",
            "description": "Tracks portfolio holdings, performance metrics, and sector allocation",
            "elements": [
                {
                    "type": "looker_table",
                    "title": "Top Portfolios by Value",
                    "query": f"SELECT portfolio_id, SUM(total_amount) as portfolio_value, COUNT(DISTINCT symbol) as num_holdings FROM `{self.project_id}.{self.dataset_id}.portfolio_transactions` WHERE transaction_type = 'BUY' GROUP BY portfolio_id ORDER BY portfolio_value DESC LIMIT 20",
                },
                {
                    "type": "looker_pie",
                    "title": "Sector Allocation (Sample Portfolio)",
                    "query": f"SELECT sector, COUNT(*) as holdings FROM `{self.project_id}.{self.dataset_id}.portfolio_transactions` WHERE portfolio_id = 'PORT_000001' AND transaction_type IN ('BUY', 'DIVIDEND') GROUP BY sector",
                },
                {
                    "type": "looker_column",
                    "title": "Dividend Income",
                    "query": f"SELECT DATE(timestamp) as date, symbol, SUM(total_amount) as dividend FROM `{self.project_id}.{self.dataset_id}.portfolio_transactions` WHERE transaction_type = 'DIVIDEND' GROUP BY date, symbol ORDER BY date DESC LIMIT 30",
                },
                {
                    "type": "looker_single_value",
                    "title": "Total Fees (Last 90 Days)",
                    "query": f"SELECT SUM(fees) as total_fees FROM `{self.project_id}.{self.dataset_id}.portfolio_transactions` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)",
                }
            ]
        }
        return dashboard
    
    def create_kpi_dashboard(self) -> Dict[str, Any]:
        """Create KPI summary dashboard."""
        dashboard = {
            "name": "KPI Dashboard",
            "title": "Financial Analytics KPIs",
            "description": "Key performance indicators for financial market analysis",
            "elements": [
                {
                    "type": "looker_single_value",
                    "title": "Total Market Volume (Today)",
                    "query": f"SELECT SUM(total_volume) as market_volume FROM `{self.project_id}.{self.dataset_id}.market_metrics` WHERE date = CURRENT_DATE() - 1",
                },
                {
                    "type": "looker_single_value",
                    "title": "Average Volatility",
                    "query": f"SELECT AVG(volatility) as avg_volatility FROM `{self.project_id}.{self.dataset_id}.market_metrics` WHERE date = CURRENT_DATE() - 1",
                },
                {
                    "type": "looker_single_value",
                    "title": "Positive Price Change %",
                    "query": f"SELECT COUNT(IF(price_change_pct > 0, 1, NULL)) / COUNT(*) * 100 as pct_positive FROM `{self.project_id}.{self.dataset_id}.market_metrics` WHERE date = CURRENT_DATE() - 1",
                },
                {
                    "type": "looker_single_value",
                    "title": "Total Transactions",
                    "query": f"SELECT COUNT(*) as total_txns FROM `{self.project_id}.{self.dataset_id}.market_trades` WHERE DATE(timestamp) = CURRENT_DATE() - 1",
                }
            ]
        }
        return dashboard
    
    def export_dashboards(self, output_dir: str = "./looker") -> List[str]:
        """Export all dashboards to JSON files."""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        dashboards = [
            ("stock_performance", self.create_stock_performance_dashboard()),
            ("market_analysis", self.create_market_analysis_dashboard()),
            ("portfolio", self.create_portfolio_dashboard()),
            ("kpi_summary", self.create_kpi_dashboard()),
        ]
        
        exported_files = []
        for name, dashboard in dashboards:
            filename = f"{output_dir}/dashboard_{name}.json"
            with open(filename, 'w') as f:
                json.dump(dashboard, f, indent=2)
            exported_files.append(filename)
            logger.info(f"Exported dashboard: {filename}")
        
        return exported_files


class LookMLGenerator:
    """Generates LookML view and explore definitions."""
    
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
    
    def generate_stock_prices_view(self) -> str:
        """Generate LookML for stock_prices view."""
        lookml = f"""view: stock_prices {{
  sql_table_name: `{self.project_id}.{self.dataset_id}.stock_prices` ;;
  
  dimension: date {{
    type: date
    primary_key: yes
    sql: ${{TABLE}}.date ;;
  }}
  
  dimension: symbol {{
    type: string
    sql: ${{TABLE}}.symbol ;;
  }}
  
  dimension: price_range {{
    type: number
    sql: ${{TABLE}}.high - ${{TABLE}}.low ;;
  }}
  
  measure: count {{
    type: count
  }}
  
  measure: average_close {{
    type: average
    sql: ${{TABLE}}.close ;;
  }}
  
  measure: total_volume {{
    type: sum
    sql: ${{TABLE}}.volume ;;
  }}
}}
"""
        return lookml
    
    def generate_market_trades_view(self) -> str:
        """Generate LookML for market_trades view."""
        lookml = f"""view: market_trades {{
  sql_table_name: `{self.project_id}.{self.dataset_id}.market_trades` ;;
  
  dimension: trade_id {{
    primary_key: yes
    type: string
    sql: ${{TABLE}}.trade_id ;;
  }}
  
  dimension: symbol {{
    type: string
    sql: ${{TABLE}}.symbol ;;
  }}
  
  dimension: side {{
    type: string
    sql: ${{TABLE}}.side ;;
  }}
  
  dimension: timestamp {{
    type: time
    timeframes: [date, week, month, year]
    sql: ${{TABLE}}.timestamp ;;
  }}
  
  measure: count {{
    type: count
  }}
  
  measure: total_trade_value {{
    type: sum
    sql: ${{TABLE}}.trade_value ;;
  }}
}}
"""
        return lookml
    
    def export_lookml(self, output_dir: str = "./looker") -> List[str]:
        """Export LookML definitions."""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        views = [
            ("stock_prices.view.lkml", self.generate_stock_prices_view()),
            ("market_trades.view.lkml", self.generate_market_trades_view()),
        ]
        
        exported_files = []
        for filename, content in views:
            filepath = f"{output_dir}/{filename}"
            with open(filepath, 'w') as f:
                f.write(content)
            exported_files.append(filepath)
            logger.info(f"Exported LookML: {filepath}")
        
        return exported_files

#!/usr/bin/env python3
"""
BigQuery Analytics Pipeline for Financial Data - GUI Application
Fetches live stock data from Yahoo Finance, persists to CSV/SQL, runs analytics.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import sys
import os
from datetime import datetime
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from real_data_loader import fetch_yfinance_prices, compute_basic_indicators
from looker_exporter import LookerDashboardBuilder, LookMLGenerator

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np

# Optional Pillow for better image handling in Tkinter
try:
    from PIL import Image, ImageTk
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False


class AnalyticsPipelineApp:
    """Production-grade GUI for BigQuery analytics pipeline."""
    
    def __init__(self, root):
        self.root = root
        self.root.title("BigQuery Analytics Pipeline - Live Data Demo")
        self.root.geometry("1000x800")
        self.root.configure(bg='#f0f0f0')
        
        self.is_running = False
        self.last_data = None
        # Animation jobs
        self._pulse_job = None
        self._scroll_job = None
        self._pulse_state = False
        
        self._build_ui()
    
    def _build_ui(self):
        """Build the user interface."""
        # Header
        header_frame = tk.Frame(self.root, bg='#2c3e50', height=60)
        header_frame.pack(fill=tk.X)
        
        title = tk.Label(header_frame, text="BigQuery Analytics Pipeline - Live Data",
                        font=("Arial", 16, "bold"), bg='#2c3e50', fg='white', pady=10)
        title.pack()
        
        # Main content
        main_frame = tk.Frame(self.root, bg='#f0f0f0')
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Left panel: controls
        left_panel = tk.Frame(main_frame, bg='white', relief=tk.RIDGE, bd=1)
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=False, padx=(0, 10), ipadx=10, ipady=10)
        
        # Symbols selection
        tk.Label(left_panel, text="Stock Symbols", font=("Arial", 10, "bold"), bg='white').pack(anchor=tk.W, pady=(0, 5))
        symbols_default = "AAPL,MSFT,GOOGL,TSLA,NVDA,META,AMZN,JPM"
        self.symbols_entry = tk.Entry(left_panel, width=30, font=("Arial", 9))
        self.symbols_entry.insert(0, symbols_default)
        self.symbols_entry.pack(anchor=tk.W, pady=(0, 10))
        tk.Label(left_panel, text="(comma-separated)", font=("Arial", 8), bg='white', fg='gray').pack(anchor=tk.W, pady=(0, 10))
        
        # Period selection
        tk.Label(left_panel, text="Data Period", font=("Arial", 10, "bold"), bg='white').pack(anchor=tk.W, pady=(0, 5))
        self.period_var = tk.StringVar(value="3mo")
        for period in ["1mo", "3mo", "6mo", "1y"]:
            ttk.Radiobutton(left_panel, text=period, variable=self.period_var, value=period).pack(anchor=tk.W)
        
        # Separator
        ttk.Separator(left_panel, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10)
        
        # BigQuery options
        tk.Label(left_panel, text="BigQuery (Optional)", font=("Arial", 10, "bold"), bg='white').pack(anchor=tk.W, pady=(0, 5))
        
        tk.Label(left_panel, text="Project ID", font=("Arial", 9), bg='white').pack(anchor=tk.W, pady=(5, 0))
        self.project_entry = tk.Entry(left_panel, width=30, font=("Arial", 9))
        self.project_entry.pack(anchor=tk.W, pady=(0, 8))
        
        tk.Label(left_panel, text="Dataset ID", font=("Arial", 9), bg='white').pack(anchor=tk.W, pady=(5, 0))
        self.dataset_entry = tk.Entry(left_panel, width=30, font=("Arial", 9))
        self.dataset_entry.insert(0, "financial_data")
        self.dataset_entry.pack(anchor=tk.W, pady=(0, 10))
        
        # Separator
        ttk.Separator(left_panel, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10)
        
        # Action buttons
        tk.Label(left_panel, text="Actions", font=("Arial", 10, "bold"), bg='white').pack(anchor=tk.W, pady=(0, 5))
        
        self.fetch_btn = tk.Button(left_panel, text="📊 Fetch Live Data", bg='#27ae60', fg='white',
                                  font=("Arial", 10, "bold"), width=25, command=self._fetch_data,
                                  cursor="hand2", activebackground='#229954')
        self.fetch_btn.pack(pady=5)
        
        self.analyze_btn = tk.Button(left_panel, text="📈 Run Analytics", bg='#3498db', fg='white',
                                    font=("Arial", 10, "bold"), width=25, command=self._run_analytics,
                                    cursor="hand2", activebackground='#2980b9', state=tk.DISABLED)
        self.analyze_btn.pack(pady=5)
        
        self.export_btn = tk.Button(left_panel, text="📊 Export Dashboards", bg='#e74c3c', fg='white',
                                   font=("Arial", 10, "bold"), width=25, command=self._export_dashboards,
                                   cursor="hand2", activebackground='#c0392b', state=tk.DISABLED)
        self.export_btn.pack(pady=5)
        
        self.clear_btn = tk.Button(left_panel, text="🔄 Clear Results", bg='#95a5a6', fg='white',
                                  font=("Arial", 10, "bold"), width=25, command=self._clear_results,
                                  cursor="hand2", activebackground='#7f8c8d')
        self.clear_btn.pack(pady=5)

        # Refresh visuals button
        self.refresh_vis_btn = tk.Button(left_panel, text="🖼️ Refresh Visuals", bg='#8e44ad', fg='white',
                         font=("Arial", 10, "bold"), width=25, command=self._refresh_visuals,
                         cursor="hand2", activebackground='#7d3c98')
        self.refresh_vis_btn.pack(pady=5)

        # Prediction / Fine-tune controls
        ttk.Separator(left_panel, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10)
        tk.Label(left_panel, text="Prediction Parameters", font=("Arial", 10, "bold"), bg='white').pack(anchor=tk.W, pady=(5, 3))

        prm_frame = tk.Frame(left_panel, bg='white')
        prm_frame.pack(anchor=tk.W, pady=(0, 5))

        tk.Label(prm_frame, text='SMA short', bg='white').grid(row=0, column=0, sticky='w')
        self.sma_short_var = tk.IntVar(value=20)
        tk.Entry(prm_frame, textvariable=self.sma_short_var, width=6).grid(row=0, column=1, padx=6)

        tk.Label(prm_frame, text='SMA long', bg='white').grid(row=1, column=0, sticky='w')
        self.sma_long_var = tk.IntVar(value=50)
        tk.Entry(prm_frame, textvariable=self.sma_long_var, width=6).grid(row=1, column=1, padx=6)

        tk.Label(prm_frame, text='RSI period', bg='white').grid(row=2, column=0, sticky='w')
        self.rsi_period_var = tk.IntVar(value=14)
        tk.Entry(prm_frame, textvariable=self.rsi_period_var, width=6).grid(row=2, column=1, padx=6)

        tk.Label(prm_frame, text='RSI buy', bg='white').grid(row=3, column=0, sticky='w')
        self.rsi_buy_var = tk.IntVar(value=30)
        tk.Entry(prm_frame, textvariable=self.rsi_buy_var, width=6).grid(row=3, column=1, padx=6)

        tk.Label(prm_frame, text='RSI sell', bg='white').grid(row=4, column=0, sticky='w')
        self.rsi_sell_var = tk.IntVar(value=70)
        tk.Entry(prm_frame, textvariable=self.rsi_sell_var, width=6).grid(row=4, column=1, padx=6)

        self.predict_btn = tk.Button(left_panel, text="🔮 Compute Signals", bg='#f39c12', fg='white',
                         font=("Arial", 10, "bold"), width=25, command=self._compute_signals,
                         cursor="hand2", activebackground='#d78a0f')
        self.predict_btn.pack(pady=(8, 5))
        
        # Right panel: output
        right_panel = tk.Frame(main_frame, bg='white', relief=tk.RIDGE, bd=1)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # Use a Notebook so we can show both textual output and generated visuals
        tk.Label(right_panel, text="Output & Results", font=("Arial", 10, "bold"), bg='white').pack(anchor=tk.W, padx=10, pady=(10, 5))
        self.right_notebook = ttk.Notebook(right_panel)
        self.right_notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Text output tab
        out_tab = tk.Frame(self.right_notebook, bg='white')
        self.output = scrolledtext.ScrolledText(out_tab, font=("Courier", 9), bg='#f8f9fa',
                               fg='#2c3e50', height=30, wrap=tk.WORD)
        self.output.pack(fill=tk.BOTH, expand=True)
        self.right_notebook.add(out_tab, text='Output')

        # Visuals tab
        vis_tab = tk.Frame(self.right_notebook, bg='white')
        vis_frame = tk.Frame(vis_tab, bg='white')
        vis_frame.pack(fill=tk.BOTH, expand=True)

        # Left: list of images
        img_list_frame = tk.Frame(vis_frame, bg='white')
        img_list_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10), pady=5)
        tk.Label(img_list_frame, text='Visuals', bg='white', font=("Arial", 9, 'bold')).pack(anchor=tk.N)
        self.image_listbox = tk.Listbox(img_list_frame, height=20, width=30)
        self.image_listbox.pack(fill=tk.Y, expand=False)
        self.image_listbox.bind('<<ListboxSelect>>', self._on_image_select)

        # Right: image / JSON display container
        img_display_frame = tk.Frame(vis_frame, bg='white')
        img_display_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        self.visual_display_container = img_display_frame

        # Image label (for PNGs)
        self.image_label = tk.Label(img_display_frame, bg='white')
        self.image_label.pack(fill=tk.BOTH, expand=True)

        # JSON/text display (hidden by default)
        self.json_text = scrolledtext.ScrolledText(img_display_frame, font=("Courier", 10), bg='#ffffff', wrap=tk.NONE)
        self.json_text.pack(fill=tk.BOTH, expand=True)
        self.json_text.pack_forget()

        self.right_notebook.add(vis_tab, text='Visuals')

        # Storage for loaded images to prevent GC
        self._tk_images = {}
        
        # Footer
        footer_frame = tk.Frame(self.root, bg='#ecf0f1', height=40)
        footer_frame.pack(fill=tk.X)
        
        status_text = "Ready. Fetch live data from Yahoo Finance, analyze, and export to Looker dashboards."
        self.status_label = tk.Label(footer_frame, text=status_text, font=("Arial", 9), bg='#ecf0f1', fg='#7f8c8d', pady=10)
        self.status_label.pack(side=tk.LEFT, padx=10)
        
        # Log initial message
        self._log("=== BigQuery Analytics Pipeline ===")
        self._log(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._log("Ready to fetch live stock data from Yahoo Finance.")
        self._log("")
    
    def _log(self, message):
        """Log message to output."""
        self.output.insert(tk.END, message + "\n")
        self.output.see(tk.END)
        self.root.update()
    
    def _fetch_data(self):
        """Fetch live data from Yahoo Finance in background thread."""
        if self.is_running:
            messagebox.showwarning("Running", "Another operation is in progress.")
            return
        
        threading.Thread(target=self._fetch_data_thread, daemon=True).start()
    
    def _fetch_data_thread(self):
        """Background thread for fetching data."""
        self.is_running = True
        self.fetch_btn.config(state=tk.DISABLED, bg='#95a5a6')
        
        try:
            self._log("\n" + "="*50)
            self._log("📊 FETCHING LIVE DATA FROM YAHOO FINANCE")
            self._log("="*50)
            
            symbols = [s.strip().upper() for s in self.symbols_entry.get().split(',')]
            period = self.period_var.get()
            
            self._log(f"Symbols: {', '.join(symbols)}")
            self._log(f"Period: {period}")
            self._log("Fetching prices...")
            
            # Fetch data
            df = fetch_yfinance_prices(symbols, period=period, interval='1d')
            
            if df.empty:
                self._log("❌ Error: No data returned from Yahoo Finance.")
                messagebox.showerror("Error", "Failed to fetch data from Yahoo Finance.")
                self.is_running = False
                self.fetch_btn.config(state=tk.NORMAL, bg='#27ae60')
                return
            
            self._log(f"✓ Fetched {len(df)} records")
            self._log(f"Date range: {df['date'].min()} to {df['date'].max()}")
            self._log(f"Symbols found: {', '.join(df['symbol'].unique())}")
            
            # Compute indicators
            self._log("Computing technical indicators (SMA, RSI)...")
            df = compute_basic_indicators(df)
            self._log("✓ Indicators computed")
            
            # Save to CSV
            self._log("Saving to CSV...")
            csv_path = os.path.join(os.path.dirname(__file__), 'data', 'stock_prices.csv')
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            df.to_csv(csv_path, index=False)
            self._log(f"✓ Data saved to {csv_path}")
            
            # Generate SQL load script
            self._log("Generating SQL load script...")
            sql_path = os.path.join(os.path.dirname(__file__), 'sql', 'load_stock_prices.sql')
            self._generate_sql_load(df, sql_path)
            self._log(f"✓ SQL script generated at {sql_path}")
            
            # Store reference for analytics
            self.last_data = df
            
            self._log("\n✅ Live data fetch complete!")
            self._log("Next: Click 'Run Analytics' to analyze the data.")
            self.analyze_btn.config(state=tk.NORMAL, bg='#3498db')
            self._update_status("✓ Data fetched. Ready for analysis.")
            
        except Exception as e:
            self._log(f"\n❌ Error fetching data: {str(e)}")
            messagebox.showerror("Error", f"Failed to fetch data:\n{str(e)}")
        
        finally:
            self.is_running = False
            self.fetch_btn.config(state=tk.NORMAL, bg='#27ae60')
    
    def _generate_sql_load(self, df, sql_path):
        """Generate SQL INSERT statements from data."""
        os.makedirs(os.path.dirname(sql_path), exist_ok=True)
        
        with open(sql_path, 'w') as f:
            f.write("-- Auto-generated SQL load script\n")
            f.write("-- Insert stock prices into BigQuery\n\n")
            f.write("BEGIN TRANSACTION;\n\n")
            
            for _, row in df.iterrows():
                date_str = row['date']
                symbol = row['symbol']
                open_price = row.get('open', 0)
                high = row.get('high', 0)
                low = row.get('low', 0)
                close = row.get('close', 0)
                volume = int(row.get('volume', 0))
                sma_20 = row.get('sma_20', None)
                rsi_14 = row.get('rsi_14', None)
                
                sma_20_sql = f"{sma_20}" if sma_20 and not str(sma_20).lower() == 'nan' else "NULL"
                rsi_14_sql = f"{rsi_14}" if rsi_14 and not str(rsi_14).lower() == 'nan' else "NULL"
                
                f.write(f"""INSERT INTO `{{project}}.{{dataset}}.stock_prices`
  (date, symbol, open, high, low, close, volume, sma_20, rsi_14, fetch_timestamp)
VALUES
  ('{date_str}', '{symbol}', {open_price}, {high}, {low}, {close}, {volume}, {sma_20_sql}, {rsi_14_sql}, CURRENT_TIMESTAMP());\n""")
            
            f.write("\nCOMMIT TRANSACTION;\n")
            f.write("\n-- Replace {project} and {dataset} with your actual BigQuery project and dataset IDs\n")
    
    def _run_analytics(self):
        """Run analytics queries."""
        if self.is_running:
            messagebox.showwarning("Running", "Another operation is in progress.")
            return
        
        if self.last_data is None or self.last_data.empty:
            messagebox.showwarning("No Data", "Please fetch data first.")
            return
        
        threading.Thread(target=self._run_analytics_thread, daemon=True).start()
    
    def _run_analytics_thread(self):
        """Background thread for analytics."""
        self.is_running = True
        self.analyze_btn.config(state=tk.DISABLED, bg='#95a5a6')
        
        try:
            self._log("\n" + "="*50)
            self._log("📈 RUNNING ANALYTICS")
            self._log("="*50)
            
            df = self.last_data
            
            # Local analytics (no BigQuery required)
            self._log("\n1. PRICE STATISTICS")
            self._log("-" * 40)
            
            for symbol in df['symbol'].unique():
                sym_data = df[df['symbol'] == symbol]
                avg_close = sym_data['close'].mean()
                max_close = sym_data['close'].max()
                min_close = sym_data['close'].min()
                latest = sym_data.iloc[-1]['close']
                
                self._log(f"\n{symbol}:")
                self._log(f"  Latest: ${latest:.2f}")
                self._log(f"  Average: ${avg_close:.2f}")
                self._log(f"  High: ${max_close:.2f}")
                self._log(f"  Low: ${min_close:.2f}")
            
            # Technical indicators
            self._log("\n2. TECHNICAL INDICATORS")
            self._log("-" * 40)
            
            for symbol in df['symbol'].unique():
                sym_data = df[df['symbol'] == symbol].iloc[-1]
                sma = sym_data.get('sma_20', 'N/A')
                rsi = sym_data.get('rsi_14', 'N/A')
                
                sma_str = f"{sma:.2f}" if sma and str(sma).lower() != 'nan' else "N/A"
                rsi_str = f"{rsi:.2f}" if rsi and str(rsi).lower() != 'nan' else "N/A"
                
                self._log(f"{symbol}: SMA(20)={sma_str}, RSI(14)={rsi_str}")
            
            # Volume analysis
            self._log("\n3. VOLUME ANALYSIS")
            self._log("-" * 40)
            
            for symbol in df['symbol'].unique():
                sym_data = df[df['symbol'] == symbol]
                avg_vol = sym_data['volume'].mean()
                max_vol = sym_data['volume'].max()
                
                self._log(f"{symbol}: Avg Volume={avg_vol:,.0f}, Max={max_vol:,.0f}")
            
            self._log("\n✅ Analytics complete!")
            self._log("Next: Click 'Export Dashboards' to generate Looker configs.")
            self.export_btn.config(state=tk.NORMAL, bg='#e74c3c')
            self._update_status("✓ Analytics complete. Ready to export dashboards.")
            
        except Exception as e:
            self._log(f"\n❌ Error during analytics: {str(e)}")
            messagebox.showerror("Error", f"Analytics failed:\n{str(e)}")
        
        finally:
            self.is_running = False
            self.analyze_btn.config(state=tk.NORMAL, bg='#3498db')
    
    def _export_dashboards(self):
        """Export Looker dashboards."""
        if self.is_running:
            messagebox.showwarning("Running", "Another operation is in progress.")
            return
        
        if self.last_data is None or self.last_data.empty:
            messagebox.showwarning("No Data", "Please fetch data first.")
            return
        
        threading.Thread(target=self._export_dashboards_thread, daemon=True).start()
    
    def _export_dashboards_thread(self):
        """Background thread for dashboard export."""
        self.is_running = True
        self.export_btn.config(state=tk.DISABLED, bg='#95a5a6')
        
        try:
            self._log("\n" + "="*50)
            self._log("📊 EXPORTING LOOKER DASHBOARDS")
            self._log("="*50)
            
            # Use provided project/dataset or default to demo values
            project_id = self.project_entry.get() or "financial-analytics-demo"
            dataset_id = self.dataset_entry.get() or "financial_data"
            
            builder = LookerDashboardBuilder(project_id, dataset_id)

            # Generate visuals (candlesticks, summary charts) and attach to dashboards
            images_dir = os.path.join(os.path.dirname(__file__), 'looker', 'images')
            os.makedirs(images_dir, exist_ok=True)
            try:
                images = self._generate_visuals(self.last_data, images_dir)
                self._log(f"✓ Generated {len(images)} visualization images")
                # Load visuals into GUI (schedule on main thread)
                try:
                    self.root.after(100, lambda: self._load_visuals(images_dir))
                except Exception:
                    pass
            except Exception as e:
                self._log(f"⚠️ Warning: visual generation failed: {e}")
                images = {}
            
            # Stock Performance Dashboard
            self._log("Generating: Stock Performance Dashboard...")
            dashboard_stock = builder.create_stock_performance_dashboard()
            # attach summary image if exists
            if 'summary' in images:
                dashboard_stock['image'] = images['summary']
            stock_path = os.path.join(os.path.dirname(__file__), 'looker', 'dashboard_stock_performance.json')
            os.makedirs(os.path.dirname(stock_path), exist_ok=True)
            
            with open(stock_path, 'w') as f:
                json.dump(dashboard_stock, f, indent=2)
            self._log(f"✓ Saved: {stock_path}")
            
            # Market Analysis Dashboard
            self._log("Generating: Market Analysis Dashboard...")
            dashboard_market = builder.create_market_analysis_dashboard()
            if 'volatility' in images:
                dashboard_market['image'] = images['volatility']
            market_path = os.path.join(os.path.dirname(__file__), 'looker', 'dashboard_market_analysis.json')
            
            with open(market_path, 'w') as f:
                json.dump(dashboard_market, f, indent=2)
            self._log(f"✓ Saved: {market_path}")
            
            # Portfolio Dashboard
            self._log("Generating: Portfolio Dashboard...")
            dashboard_portfolio = builder.create_portfolio_dashboard()
            if 'portfolio' in images:
                dashboard_portfolio['image'] = images['portfolio']
            portfolio_path = os.path.join(os.path.dirname(__file__), 'looker', 'dashboard_portfolio.json')
            
            with open(portfolio_path, 'w') as f:
                json.dump(dashboard_portfolio, f, indent=2)
            self._log(f"✓ Saved: {portfolio_path}")
            
            # KPI Dashboard
            self._log("Generating: KPI Summary Dashboard...")
            dashboard_kpi = builder.create_kpi_dashboard()
            if 'kpi' in images:
                dashboard_kpi['image'] = images['kpi']
            kpi_path = os.path.join(os.path.dirname(__file__), 'looker', 'dashboard_kpi_summary.json')
            
            with open(kpi_path, 'w') as f:
                json.dump(dashboard_kpi, f, indent=2)
            self._log(f"✓ Saved: {kpi_path}")
            
            # LookML Views (use LookMLGenerator)
            self._log("Generating: LookML Views...")
            generator = LookMLGenerator(project_id, dataset_id)
            stock_view = generator.generate_stock_prices_view()
            view_path = os.path.join(os.path.dirname(__file__), 'looker', 'stock_prices.view.lkml')
            
            with open(view_path, 'w') as f:
                f.write(stock_view)
            self._log(f"✓ Saved: {view_path}")
            
            self._log("\n✅ All dashboards exported successfully!")
            self._log("Check the 'looker/' directory for JSON files and LookML views.")
            self._update_status("✓ Dashboards exported. Project complete!")
            
        except Exception as e:
            self._log(f"\n❌ Error exporting dashboards: {str(e)}")
            messagebox.showerror("Error", f"Dashboard export failed:\n{str(e)}")
        
        finally:
            self.is_running = False
            self.export_btn.config(state=tk.NORMAL, bg='#e74c3c')
    
    def _clear_results(self):
        """Clear output and reset state."""
        self.output.delete(1.0, tk.END)
        self.analyze_btn.config(state=tk.DISABLED, bg='#95a5a6')
        self.export_btn.config(state=tk.DISABLED, bg='#95a5a6')
        self._log("=== BigQuery Analytics Pipeline ===")
        self._log(f"Reset: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._log("Ready to fetch live stock data from Yahoo Finance.")
        self._update_status("Ready.")
    
    def _generate_visuals(self, df: pd.DataFrame, out_dir: str) -> dict:
        """Generate visualizations (candlestick, summary, volatility, kpi) and save PNGs.

        Returns dict mapping keys to relative image paths.
        """
        images = {}
        if df is None or df.empty:
            return images

        df = df.copy()
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])

        # Summary: multiple lines of close prices
        try:
            fig, ax = plt.subplots(figsize=(10, 4))
            for sym in df['symbol'].unique():
                s = df[df['symbol'] == sym].sort_values('date')
                ax.plot(s['date'], s['close'], label=sym, linewidth=1.1)
            ax.set_title('Market Close Prices')
            ax.set_xlabel('Date')
            ax.set_ylabel('Close')
            ax.legend(loc='upper left')
            summary_path = os.path.join(out_dir, 'market_summary.png')
            fig.tight_layout()
            fig.savefig(summary_path)
            plt.close(fig)
            images['summary'] = os.path.relpath(summary_path, os.path.dirname(__file__))
        except Exception:
            pass

        # Volatility chart
        try:
            vol = df.groupby('symbol')['close'].pct_change().std().reset_index()
            vol.columns = ['symbol', 'volatility']
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.bar(vol['symbol'], vol['volatility'], color='orange')
            ax.set_title('Volatility by Symbol')
            vol_path = os.path.join(out_dir, 'volatility.png')
            fig.tight_layout()
            fig.savefig(vol_path)
            plt.close(fig)
            images['volatility'] = os.path.relpath(vol_path, os.path.dirname(__file__))
        except Exception:
            pass

        # KPI summary image (simple text panel)
        try:
            latest = df.sort_values(['symbol', 'date']).groupby('symbol').tail(1)
            kpi_fig, kpi_ax = plt.subplots(figsize=(6, 2))
            kpi_ax.axis('off')
            lines = []
            for _, r in latest.iterrows():
                lines.append(f"{r['symbol']}: {r['close']:.2f}")
            kpi_ax.text(0.01, 0.98, '\n'.join(lines), va='top', fontsize=10, family='monospace')
            kpi_path = os.path.join(out_dir, 'kpi_summary.png')
            kpi_fig.tight_layout()
            kpi_fig.savefig(kpi_path)
            plt.close(kpi_fig)
            images['kpi'] = os.path.relpath(kpi_path, os.path.dirname(__file__))
        except Exception:
            pass

        # Candlesticks per symbol (save up to 6 symbols)
        try:
            symbols = list(df['symbol'].unique())[:6]
            for sym in symbols:
                s = df[df['symbol'] == sym].sort_values('date')
                fig, ax = plt.subplots(figsize=(8, 4))
                for _, row in s.iterrows():
                    o, h, l, c = row['open'], row['high'], row['low'], row['close']
                    color = 'green' if c >= o else 'red'
                    # wick
                    ax.vlines(row['date'], l, h, color='black', linewidth=0.6)
                    # body
                    rect = Rectangle((matplotlib.dates.date2num(row['date']) - 0.3, min(o, c)), 0.6, abs(c - o),
                                     facecolor=color, edgecolor='black')
                    ax.add_patch(rect)
                ax.set_title(f'Candlestick: {sym}')
                ax.set_xlim([s['date'].min(), s['date'].max()])
                ax.xaxis_date()
                fig.autofmt_xdate()
                cpath = os.path.join(out_dir, f'candlestick_{sym}.png')
                fig.tight_layout()
                fig.savefig(cpath)
                plt.close(fig)
                images[f'candle_{sym}'] = os.path.relpath(cpath, os.path.dirname(__file__))
        except Exception:
            pass

        return images
    
    def _load_visuals(self, images_dir: str):
        """Load images from `images_dir` into the visuals tab listbox."""
        try:
            if not os.path.isdir(images_dir):
                return

            files = [f for f in os.listdir(images_dir) if f.lower().endswith('.png')]
            files.sort()

            # Clear listbox
            self.image_listbox.delete(0, tk.END)
            self._tk_images.clear()

            # also include JSON and LookML files
            extra = [f for f in os.listdir(os.path.dirname(images_dir)) if f.lower().endswith(('.json', '.lkml'))]
            items = files + extra

            for fname in items:
                full = os.path.join(images_dir, fname) if fname in files else os.path.join(os.path.dirname(images_dir), fname)
                display_name = fname
                self.image_listbox.insert(tk.END, display_name)
                tkimg = None
                # Preload image thumbnails only for PNGs
                if fname.lower().endswith('.png'):
                    try:
                        if PIL_AVAILABLE:
                            img = Image.open(full)
                            img.thumbnail((900, 600))
                            tkimg = ImageTk.PhotoImage(img)
                        else:
                            tkimg = tk.PhotoImage(file=full)
                    except Exception:
                        tkimg = None

                ftype = 'png' if fname.lower().endswith('.png') else ('json' if fname.lower().endswith('.json') else 'lkml')
                self._tk_images[display_name] = (full, tkimg, ftype)

            # If at least one image, select the first
            if items:
                self.image_listbox.selection_set(0)
                self._show_file_by_name(items[0])
        except Exception:
            pass

    def _show_image_by_name(self, name: str):
        entry = self._tk_images.get(name)
        if not entry:
            return
        full, tkimg, ftype = entry
        # stop other animations
        self._stop_image_pulse()
        self._stop_json_scroll()

        if ftype == 'png':
            # show image widget
            self.json_text.pack_forget()
            self.image_label.pack(fill=tk.BOTH, expand=True)
            if tkimg:
                self.image_label.config(image=tkimg, text='')
                self.image_label.image = tkimg
            else:
                self.image_label.config(image='', text=full)

            # start a gentle pulse to draw attention
            self._start_image_pulse()
        else:
            # show JSON/LKML in text view
            self.image_label.pack_forget()
            self.json_text.pack(fill=tk.BOTH, expand=True)
            try:
                with open(full, 'r', encoding='utf-8') as fh:
                    content = fh.read()
                # try pretty JSON if possible
                if ftype == 'json':
                    try:
                        j = json.loads(content)
                        pretty = json.dumps(j, indent=2)
                    except Exception:
                        pretty = content
                else:
                    pretty = content

                self.json_text.delete(1.0, tk.END)
                self.json_text.insert(tk.END, pretty)
                self.json_text.see('1.0')
                # start auto-scroll animation
                self._start_json_scroll()
            except Exception:
                self.json_text.delete(1.0, tk.END)
                self.json_text.insert(tk.END, f"Failed to open {full}")

    def _on_image_select(self, _event=None):
        try:
            sel = self.image_listbox.curselection()
            if not sel:
                return
            idx = sel[0]
            name = self.image_listbox.get(idx)
            self._show_file_by_name(name)
        except Exception:
            pass

    def _refresh_visuals(self):
        images_dir = os.path.join(os.path.dirname(__file__), 'looker', 'images')
        self._load_visuals(images_dir)

    # --- Animations: image pulse and JSON auto-scroll ---
    def _start_image_pulse(self, interval: int = 600):
        try:
            self._stop_image_pulse()
            self._pulse_state = False

            def _pulse():
                try:
                    self._pulse_state = not self._pulse_state
                    color = '#ffffff' if self._pulse_state else '#f7f1ff'
                    self.image_label.config(bg=color)
                    self._pulse_job = self.root.after(interval, _pulse)
                except Exception:
                    pass

            _pulse()
        except Exception:
            pass

    def _stop_image_pulse(self):
        try:
            if self._pulse_job:
                self.root.after_cancel(self._pulse_job)
                self._pulse_job = None
                self.image_label.config(bg='white')
        except Exception:
            pass

    def _start_json_scroll(self, interval: int = 1200):
        try:
            self._stop_json_scroll()

            def _scroll():
                try:
                    # move view down a bit
                    self.json_text.yview_scroll(1, 'pages')
                    # wrap back to top when at end
                    if float(self.json_text.index('end-1c').split('.')[0]) <= float(self.json_text.index(tk.END).split('.')[0]):
                        pass
                    self._scroll_job = self.root.after(interval, _scroll)
                except Exception:
                    pass

            self._scroll_job = self.root.after(interval, _scroll)
        except Exception:
            pass

    def _stop_json_scroll(self):
        try:
            if self._scroll_job:
                self.root.after_cancel(self._scroll_job)
                self._scroll_job = None
        except Exception:
            pass

    def _show_file_by_name(self, name: str):
        """Generic dispatcher to show image or JSON by name."""
        entry = self._tk_images.get(name)
        if not entry:
            return
        full, tkimg, ftype = entry
        if ftype == 'png':
            self._show_image_by_name(name)
        else:
            self._show_image_by_name(name)

    def _compute_signals(self):
        """Run signal computation in background."""
        if self.is_running:
            messagebox.showwarning("Running", "Another operation is in progress.")
            return

        if self.last_data is None or self.last_data.empty:
            messagebox.showwarning("No Data", "Please fetch data first.")
            return

        threading.Thread(target=self._compute_signals_thread, daemon=True).start()

    def _calculate_rsi(self, series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
        avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
        rs = avg_gain / (avg_loss.replace(0, np.nan))
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50)

    def _compute_signals_thread(self):
        """Compute simple SMA crossover + RSI-based buy/hold/sell signals."""
        try:
            self._log("\n🔮 Computing buy/sell signals...")
            df = self.last_data.copy()
            sma_short = max(2, int(self.sma_short_var.get()))
            sma_long = max(sma_short + 1, int(self.sma_long_var.get()))
            rsi_period = max(2, int(self.rsi_period_var.get()))
            rsi_buy = int(self.rsi_buy_var.get())
            rsi_sell = int(self.rsi_sell_var.get())

            results = []
            for sym in df['symbol'].unique():
                s = df[df['symbol'] == sym].sort_values('date').reset_index(drop=True).copy()
                if len(s) < max(sma_long, rsi_period) + 1:
                    results.append((sym, 'HOLD', 'insufficient data'))
                    continue

                s['sma_short'] = s['close'].rolling(window=sma_short).mean()
                s['sma_long'] = s['close'].rolling(window=sma_long).mean()
                s['rsi'] = self._calculate_rsi(s['close'], period=rsi_period)

                last = s.iloc[-1]
                prev = s.iloc[-2]

                # SMA signal
                sma_signal = 'NEUTRAL'
                if last['sma_short'] > last['sma_long'] and prev['sma_short'] <= prev['sma_long']:
                    sma_signal = 'BUY'
                elif last['sma_short'] < last['sma_long'] and prev['sma_short'] >= prev['sma_long']:
                    sma_signal = 'SELL'
                else:
                    sma_signal = 'HOLD'

                # RSI signal
                rsi_signal = 'NEUTRAL'
                if last['rsi'] <= rsi_buy:
                    rsi_signal = 'BUY'
                elif last['rsi'] >= rsi_sell:
                    rsi_signal = 'SELL'
                else:
                    rsi_signal = 'HOLD'

                # Combine signals (simple rule): BUY if any BUY and no SELL; SELL if any SELL and no BUY; else HOLD
                final = 'HOLD'
                if sma_signal == 'BUY' or rsi_signal == 'BUY':
                    if not (sma_signal == 'SELL' or rsi_signal == 'SELL'):
                        final = 'BUY'
                if sma_signal == 'SELL' or rsi_signal == 'SELL':
                    if not (sma_signal == 'BUY' or rsi_signal == 'BUY'):
                        final = 'SELL'

                note = f"SMA:{sma_signal} RSI:{rsi_signal} (rsi={last['rsi']:.1f})"
                results.append((sym, final, note))

            # Print results
            self._log('\nSignal Results:')
            for sym, sig, note in results:
                self._log(f"  {sym}: {sig} — {note}")

            self._update_status("✓ Signals computed")
        except Exception as e:
            self._log(f"\n❌ Error computing signals: {e}")
        finally:
            pass

    def _update_status(self, message):
        """Update status bar."""
        self.status_label.config(text=message)


def main():
    """Main entry point."""
    root = tk.Tk()
    app = AnalyticsPipelineApp(root)
    root.mainloop()


if __name__ == '__main__':
    main()

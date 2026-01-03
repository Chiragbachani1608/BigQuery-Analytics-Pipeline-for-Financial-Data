"""
Real data loader using yfinance to fetch recent stock prices and compute basic indicators.
Provides optional BigQuery load via BigQueryDataLoader.
"""
from typing import List, Optional
import pandas as pd
import logging
import yfinance as yf
logger = logging.getLogger(__name__)


def fetch_yfinance_prices(symbols: List[str], period: str = "3mo", interval: str = "1d") -> pd.DataFrame:
    """Fetch recent OHLCV prices from Yahoo Finance for a list of symbols.

    Returns a DataFrame with columns: date, symbol, open, high, low, close, volume
    """
    try:
        import yfinance as yf
    except Exception as e:
        raise ImportError("yfinance is required to fetch live prices. Install with `pip install yfinance`") from e

    records = []
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period, interval=interval, auto_adjust=False)
            if hist.empty:
                logger.warning(f"No data returned for {symbol}")
                continue
            hist = hist.reset_index()
            hist['Date'] = pd.to_datetime(hist['Date']).dt.date
            for _, row in hist.iterrows():
                records.append({
                    'date': row['Date'],
                    'symbol': symbol,
                    'open': float(row.get('Open', 0)),
                    'high': float(row.get('High', 0)),
                    'low': float(row.get('Low', 0)),
                    'close': float(row.get('Close', 0)),
                    'volume': int(row.get('Volume', 0)),
                })
        except Exception as e:
            logger.error(f"Failed to fetch {symbol}: {e}")
            continue

    df = pd.DataFrame(records)
    if df.empty:
        logger.info("No price data fetched from yfinance")
    return df


def compute_basic_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute simple moving averages and RSI (fallback implementation).

    Adds `sma_20`, `sma_50`, and `rsi_14` columns when possible.
    """
    if df.empty:
        return df

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['symbol', 'date'])

    try:
        import pandas_ta as ta
        # pandas_ta can compute many indicators quickly
        out = []
        for symbol, group in df.groupby('symbol'):
            g = group.set_index('date')
            g['sma_20'] = ta.sma(g['close'], length=20)
            g['sma_50'] = ta.sma(g['close'], length=50)
            g['rsi_14'] = ta.rsi(g['close'], length=14)
            g = g.reset_index()
            g['symbol'] = symbol
            out.append(g)
        df = pd.concat(out, ignore_index=True)
    except Exception:
        # Fallback - simple rolling calculations
        out = []
        for symbol, group in df.groupby('symbol'):
            g = group.sort_values('date').copy()
            g['sma_20'] = g['close'].rolling(window=20, min_periods=1).mean()
            g['sma_50'] = g['close'].rolling(window=50, min_periods=1).mean()
            delta = g['close'].diff()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            roll_up = up.ewm(com=13, adjust=False).mean()
            roll_down = down.ewm(com=13, adjust=False).mean()
            rs = roll_up / roll_down
            g['rsi_14'] = 100.0 - (100.0 / (1.0 + rs))
            out.append(g)
        df = pd.concat(out, ignore_index=True)

    return df


def fetch_and_load_to_bq(client, project_id: str, dataset_id: str, table_name: str, symbols: List[str], period: str = "3mo", interval: str = "1d") -> int:
    """Fetch prices using yfinance, compute indicators, and load to BigQuery table.

    Returns number of rows loaded.
    """
    from src.data_loader import BigQueryDataLoader

    df = fetch_yfinance_prices(symbols, period=period, interval=interval)
    if df.empty:
        return 0
    df = compute_basic_indicators(df)

    loader = BigQueryDataLoader(client)
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    loader.load_dataframe_to_bq(df, table_id)
    return len(df)

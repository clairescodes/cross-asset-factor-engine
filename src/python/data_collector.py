import yfinance as yf
import pandas as pd
import time
import yaml
from fredapi import Fred
import sqlite3
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCollector:
    def __init__(self, config_path=None):
        if config_path is None:
            # Get the path relative to the project root, not the script location
            script_dir = Path(__file__).parent.parent.parent  # Go up to project root
            config_path = script_dir / "config" / "config.yaml"
        
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.fred = Fred(api_key=self.config['data_sources']['fred']['api_key'])
        self.etfs = self.config['etfs']['core']
        self.rate_limit = self.config['data_sources']['yahoo_finance']['rate_limit_seconds']

    def collect_etf_data(self, start_date='2019-01-01', end_date=None):
        """Collect ETF price data from Yahoo Finance"""
        logger.info(f"Starting ETF data collection for {len(self.etfs)} ETFs")
        
        all_data = {}
        failed_tickers = []
        
        for ticker in self.etfs:
            try:
                logger.info(f"Collecting {ticker}...")
                etf = yf.Ticker(ticker)
                hist = etf.history(start=start_date, end=end_date)
                
                if not hist.empty:
                    all_data[ticker] = hist
                    logger.info(f"✓ {ticker}: {len(hist)} days collected")
                else:
                    logger.warning(f"✗ {ticker}: No data returned")
                    failed_tickers.append(ticker)
                
                # Rate limiting
                time.sleep(self.rate_limit)
                
            except Exception as e:
                logger.error(f"✗ {ticker}: Error - {str(e)}")
                failed_tickers.append(ticker)
                time.sleep(self.rate_limit * 2)  # Extra delay on error
        
        logger.info(f"ETF collection complete. Success: {len(all_data)}, Failed: {len(failed_tickers)}")
        if failed_tickers:
            logger.warning(f"Failed tickers: {failed_tickers}")
            
        return all_data, failed_tickers
    
    def collect_economic_data(self, start_date='2019-01-01'):
        """Collect economic indicators from FRED"""
        logger.info("Starting economic data collection from FRED")
        
        indicators = {
            'DGS10': '10-Year Treasury Constant Maturity Rate',
            'DFF': 'Federal Funds Effective Rate',
            'VIXCLS': 'CBOE Volatility Index: VIX',
            'BAMLH0A0HYM2': 'ICE BofA US High Yield Index Option-Adjusted Spread'
        }
        
        econ_data = {}
        failed_indicators = []
        
        for series_id, description in indicators.items():
            try:
                logger.info(f"Collecting {series_id} ({description})...")
                data = self.fred.get_series(series_id, start=start_date)
                
                if not data.empty:
                    econ_data[series_id] = data
                    logger.info(f"✓ {series_id}: {len(data)} observations")
                else:
                    logger.warning(f"✗ {series_id}: No data returned")
                    failed_indicators.append(series_id)
                    
            except Exception as e:
                logger.error(f"✗ {series_id}: Error - {str(e)}")
                failed_indicators.append(series_id)
        
        logger.info(f"Economic data collection complete. Success: {len(econ_data)}, Failed: {len(failed_indicators)}")
        return econ_data, failed_indicators

if __name__ == "__main__":
    # Test the data collector
    collector = DataCollector()
    
    # Collect ETF data
    etf_data, etf_failures = collector.collect_etf_data()
    
    # Collect economic data
    econ_data, econ_failures = collector.collect_economic_data()
    
    # Quick preview
    if etf_data:
        print(f"\nETF Data Preview:")
        for ticker, data in etf_data.items():
            print(f"{ticker}: {data.index[0]} to {data.index[-1]} ({len(data)} days)")
    
    if econ_data:
        print(f"\nEconomic Data Preview:")
        for series, data in econ_data.items():
            print(f"{series}: {data.index[0]} to {data.index[-1]} ({len(data)} observations)")
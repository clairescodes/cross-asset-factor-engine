import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path="data/factor_engine.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database with schema"""
        # Create data directory if it doesn't exist
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Read and execute schema
        schema_path = Path("src/sql/create_tables.sql")
        if schema_path.exists():
            with open(schema_path, 'r') as file:
                schema = file.read()
            
            with sqlite3.connect(self.db_path) as conn:
                conn.executescript(schema)
            logger.info("Database initialized successfully")
        else:
            logger.warning("Schema file not found, creating basic tables")
            self._create_basic_tables()
    
    def _create_basic_tables(self):
        """Create basic tables if schema file doesn't exist"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS etf_prices (
                    date DATE, ticker TEXT, open_price REAL, high_price REAL,
                    low_price REAL, close_price REAL, volume INTEGER, adj_close REAL,
                    PRIMARY KEY (date, ticker)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS economic_indicators (
                    date DATE, indicator_name TEXT, value REAL,
                    PRIMARY KEY (date, indicator_name)
                )
            ''')
    
    def save_etf_data(self, etf_data_dict):
        """Save ETF data to database"""
        logger.info("Saving ETF data to database...")
        

        with sqlite3.connect(self.db_path) as conn:
            total_rows = 0
            
            for ticker, data in etf_data_dict.items():
                # Prepare data
                df = data.copy()
                df['ticker'] = ticker
                df['date'] = df.index.strftime('%Y-%m-%d')
                
                # Rename columns to match database schema
                df = df.rename(columns={
                    'Open': 'open_price',
                    'High': 'high_price', 
                    'Low': 'low_price',
                    'Close': 'close_price',
                    'Volume': 'volume',
                    'Adj Close': 'adj_close'
                })
                
                # Select only needed columns
                df = df[['date', 'ticker', 'open_price', 'high_price', 
                        'low_price', 'close_price', 'volume', 'adj_close']]
                
                # Save to database, replace existing data 
                df.to_sql('etf_prices', conn, if_exists='append', index=False)
                total_rows += len(df)
                logger.info(f"Saved {len(df)} rows for {ticker}")
            
            logger.info(f"Total ETF data saved: {total_rows} rows")
    
    def save_economic_data(self, econ_data_dict):
        """Save economic data to database"""
        logger.info("Saving economic data to database...")
        
        with sqlite3.connect(self, self.db_path) as conn:
            total_rows = 0
            
            for indicator, data in econ_data_dict.items():
                # Prepare data
                df = pd.DataFrame({
                    'date': data.index.strftime('%Y-%m-%d'),
                    'indicator_name': indicator,
                    'value': data.values
                })
                
                # Remove NaN values
                df = df.dropna()
                
                # Save to database
                df.to_sql('economic_indicators', conn, if_exists='append', index=False)
                total_rows += len(df)
                logger.info(f"Saved {len(df)} rows for {indicator}")
            
            logger.info(f"Total economic data saved: {total_rows} rows")
    
    def get_etf_prices(self, ticker=None, start_date=None, end_date=None):
        """Retrieve ETF prices from database"""
        query = "SELECT * FROM etf_prices WHERE 1=1"
        params = []
        
        if ticker:
            query += " AND ticker = ?"
            params.append(ticker)
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " ORDER BY date"
        
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=params, parse_dates=['date'])
    
    def get_available_tickers(self):
        """Get list of available tickers"""
        with sqlite3.connect(self.db_path) as conn:
            result = pd.read_sql_query("SELECT DISTINCT ticker FROM etf_prices ORDER BY ticker", conn)
            return result['ticker'].tolist()

# entry point 
if __name__ == "__main__":
    # Test database manager
    db = DatabaseManager()
    
    # Check if we have any data
    tickers = db.get_available_tickers()
    print(f"Available tickers: {tickers}")
    
    if tickers:
        # Get sample data
        sample_data = db.get_etf_prices(ticker=tickers[0], start_date='2024-01-01')
        print(f"\nSample data for {tickers[0]}:")
        print(sample_data.head())
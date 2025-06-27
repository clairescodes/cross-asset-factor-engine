import pandas as pd
import numpy as np
from database_manager import DatabaseManager
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, db_path="data/factor_engine.db"):
        self.db = DatabaseManager(db_path)
    
    def validate_etf_data(self):
        """Validate ETF data quality"""
        logger.info("Starting ETF data validation...")
        
        tickers = self.db.get_available_tickers()
        validation_results = {}
        
        for ticker in tickers:
            data = self.db.get_etf_prices(ticker=ticker)
            
            if data.empty:
                validation_results[ticker] = {"status": "FAIL", "reason": "No data"}
                continue
            
            # Check for required columns
            required_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'adj_close']
            missing_cols = [col for col in required_cols if col not in data.columns]
            
            if missing_cols:
                validation_results[ticker] = {"status": "FAIL", "reason": f"Missing columns: {missing_cols}"}
                continue
            
            # Check for reasonable price ranges (> 0)
            price_issues = []
            for col in ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close']:
                if (data[col] <= 0).any():
                    price_issues.append(f"{col} has non-positive values")
            
            # Check for missing data
            missing_data = data.isnull().sum()
            high_missing = missing_data[missing_data > len(data) * 0.05]  # More than 5% missing
            
            # Check date range
            data['date'] = pd.to_datetime(data['date'])
            date_range = data['date'].max() - data['date'].min()
            
            # Compile results
            issues = price_issues + (high_missing.index.tolist() if not high_missing.empty else [])
            
            validation_results[ticker] = {
                "status": "PASS" if not issues else "WARNING",
                "total_records": len(data),
                "date_range_days": date_range.days,
                "missing_data": missing_data.to_dict(),
                "issues": issues
            }
        
        logger.info("ETF data validation complete")
        return validation_results
    
    def print_validation_report(self):
        """Print a comprehensive validation report"""
        results = self.validate_etf_data()
        
        print("=" * 60)
        print("ETF DATA VALIDATION REPORT")
        print("=" * 60)
        
        for ticker, result in results.items():
            status_emoji = "✅" if result["status"] == "PASS" else "⚠️" if result["status"] == "WARNING" else "❌"
            print(f"\n{status_emoji} {ticker} - {result['status']}")
            
            if result["status"] != "FAIL":
                print(f"   Records: {result['total_records']:,}")
                print(f"   Date Range: {result['date_range_days']} days")
                
                if result.get("issues"):
                    print(f"   Issues: {'; '.join(result['issues'])}")
            else:
                print(f"   Error: {result['reason']}")
        
        # Summary
        total_tickers = len(results)
        passed = sum(1 for r in results.values() if r["status"] == "PASS")
        warnings = sum(1 for r in results.values() if r["status"] == "WARNING")
        failed = sum(1 for r in results.values() if r["status"] == "FAIL")
        
        print(f"\n" + "=" * 60)
        print(f"SUMMARY: {passed} Passed, {warnings} Warnings, {failed} Failed (Total: {total_tickers})")
        print("=" * 60)

if __name__ == "__main__":
    validator = DataValidator("../data/factor_engine.db")
    validator.print_validation_report()
from data_collector import DataCollector
from database_manager import DatabaseManager
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_full_data_collection():
    """Run complete data collection and save to database"""
    logger.info("Starting full data collection pipeline")
    
    # Initialize components
    collector = DataCollector()
    db = DatabaseManager()
    
    # Collect data
    etf_data, etf_failures = collector.collect_etf_data()
    econ_data, econ_failures = collector.collect_economic_data()
    
    # Save to database
    if etf_data: db.save_etf_data(etf_data)
    
    if econ_data: db.save_economic_data(econ_data)
    
    # Summary
    logger.info("Data collection pipeline complete!")
    logger.info(f"ETFs collected: {len(etf_data)}")
    logger.info(f"Economic indicators collected: {len(econ_data)}")
    
    if etf_failures or econ_failures:
        logger.warning(f"Some data collection failed:")
        if etf_failures: logger.warning(f"Failed ETFs: {etf_failures}")
        if econ_failures:logger.warning(f"Failed indicators: {econ_failures}")

if __name__ == "__main__":
    run_full_data_collection()
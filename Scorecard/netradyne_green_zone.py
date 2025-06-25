#!/usr/bin/env python3
"""
Netradyne Data Processing Script

This script can fetch Netradyne driver score data using three different methods:
1. API - Direct API calls to Netradyne
2. Scraper - Web scraping to download CSV files
3. Files - Processing existing Excel/CSV files from local directory

Uses modular utilities for clean separation of concerns.

Usage:
    python netradyne_green_zone.py <method> [options]

Methods:
    api, scraper, files

Options:
    --directory <path>    Specify directory for 'files' method.
    --dry-run             Perform a dry run without saving to the database.
    --debug               Enable debug mode.
    --help                Show this help message.
"""

import logging
import sys
import os
import argparse
from enum import Enum
from typing import Optional, Tuple, List, Dict, Any
from pathlib import Path

# Add project root to Python path to enable package-based imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .utils import (
    ConfigManager, 
    DatabaseManager, 
    NetradyneAPIClient, 
    NetradyneScraper, 
    FileProcessor
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/netradyne_processing.log') if os.path.exists('logs') else logging.StreamHandler()
    ]
)


def setup_directories():
    """Create necessary directories if they don't exist."""
    directories = [
        'netradyne_score_data',
        'netradyne_score_data/processed',
        'netradyne_score_data/errors',
        'logs'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)


class DataFetchMethod(Enum):
    """Enumeration of data fetching methods."""
    API = "api"
    SCRAPER = "scraper"
    FILES = "files"


class NetradyneDataProcessor:
    """Main processor that handles all three data fetching methods."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the data processor.
        
        Args:
            config_path: Optional path to configuration file.
        """
        self.config = ConfigManager(config_path)
        self.db_manager = None
        
    def fetch_data_via_api(self) -> Optional[Tuple[List[Dict[str, Any]], str]]:
        """
        Fetch data using the Netradyne API.
        
        Returns:
            Tuple of (scores, report_month) or None if failed.
        """
        logging.info("Fetching data via API...")
        
        try:
            api_client = NetradyneAPIClient(self.config)
            result = api_client.fetch_driver_scores()
            
            if result:
                scores, report_month = result
                logging.info(f"Successfully fetched {len(scores)} records via API")
                return scores, report_month
            else:
                logging.error("Failed to fetch data via API")
                return None
                
        except Exception as e:
            logging.error(f"Error in API data fetch: {e}")
            return None
    
    def fetch_data_via_scraper(self) -> Optional[Tuple[List[Dict[str, Any]], str]]:
        """
        Fetch data using web scraping to download CSV files.
        
        Returns:
            Tuple of (scores, report_month) or None if failed.
        """
        logging.info("Fetching data via web scraper...")
        
        try:
            # Download file using scraper
            scraper = NetradyneScraper(self.config)
            downloaded_file = scraper.download_netradyne_file()
            
            if not downloaded_file:
                logging.error("Failed to download file via scraper")
                return None
            
            logging.info(f"Successfully downloaded file: {downloaded_file}")
            
            # Process the downloaded file
            file_processor = FileProcessor(self.config)
            result = file_processor.process_file(downloaded_file)
            
            if result:
                report_month = file_processor.get_report_month()
                logging.info(f"Successfully processed {len(result)} records from downloaded file")
                return result, report_month
            else:
                logging.error("Failed to process downloaded file")
                return None
                
        except Exception as e:
            logging.error(f"Error in scraper data fetch: {e}")
            return None
    
    def fetch_data_via_files(self, directory: str = None) -> Optional[Tuple[List[Dict[str, Any]], str]]:
        """
        Fetch data by processing existing files in the directory.
        
        Args:
            directory: Directory containing data files. If None, uses default.
        
        Returns:
            Tuple of (scores, report_month) or None if failed.
        """
        logging.info("Fetching data via file processing...")
        
        try:
            file_processor = FileProcessor(self.config)
            result = file_processor.process_all_files(directory)
            
            if result:
                scores, report_month = result
                logging.info(f"Successfully processed {len(scores)} records from files")
                return scores, report_month
            else:
                logging.error("Failed to process files")
                return None
                
        except Exception as e:
            logging.error(f"Error in file data processing: {e}")
            return None
    
    def process_and_store_data(self, method: DataFetchMethod, directory: str = None, dry_run: bool = False) -> bool:
        """
        Main processing workflow that fetches data and optionally stores it in the database.
        
        Args:
            method: Data fetching method to use.
            directory: Directory for file processing (only used with FILES method).
            dry_run: If True, performs a dry run without storing data.
        
        Returns:
            Boolean indicating success.
        """
        if dry_run:
            logging.info("--- Starting DRY RUN ---")

        logging.info(f"Starting data processing using method: {method.value}")

        # Step 1: Fetch data
        result = None
        try:
            if method == DataFetchMethod.API:
                result = self.fetch_data_via_api()
            elif method == DataFetchMethod.SCRAPER:
                result = self.fetch_data_via_scraper()
            elif method == DataFetchMethod.FILES:
                result = self.fetch_data_via_files(directory)
            else:
                logging.error(f"Unknown method: {method}")
                return False
        except Exception as e:
            error_msg = f"An exception occurred during data fetching with method {method.value}: {e}"
            logging.error(error_msg)
            if not dry_run:
                self._update_status_on_failure(error_msg)
            return False

        if result is None:
            error_msg = f"Failed to retrieve data using method: {method.value}"
            logging.error(error_msg)
            if not dry_run:
                self._update_status_on_failure(error_msg)
            return False
            
        scores, report_month = result
        
        # Step 2: Handle dry run or store data
        if dry_run:
            logging.info(f"[DRY RUN] Fetched {len(scores)} records for report month {report_month}.")
            logging.info("[DRY RUN] Data would be inserted into the database.")
            logging.info("[DRY RUN] Script status would be updated to SUCCESS.")
            logging.info("--- DRY RUN Complete ---")
            return True
        
        # Step 3: Store data in the database
        logging.info("Storing data in the database...")
        try:
            with DatabaseManager(self.config) as db_manager:
                if not db_manager.connection:
                    logging.error("Database connection failure. Cannot store data.")
                    return False

                success = db_manager.insert_driver_scores(scores, report_month)
                
                if success:
                    db_manager.update_script_status(True)
                    logging.info(f"Successfully completed data processing using {method.value}")
                    return True
                else:
                    error_msg = "Failed to insert data into database"
                    db_manager.update_script_status(False, error_msg)
                    logging.error(error_msg)
                    return False
                    
        except Exception as e:
            error_msg = f"Unexpected error during database operation: {str(e)}"
            logging.error(error_msg)
            self._update_status_on_failure(error_msg)
            return False

    def _update_status_on_failure(self, error_msg: str):
        """Helper to update script status on failure, handling DB connection errors."""
        try:
            with DatabaseManager(self.config) as db_manager:
                if db_manager.connection:
                    db_manager.update_script_status(False, error_msg)
                else:
                    logging.warning("Could not connect to DB to update failure status.")
        except Exception as e:
            logging.error(f"Failed to update script status on failure: {e}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Netradyne Data Processing Script.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python netradyne_green_zone.py api
  python netradyne_green_zone.py scraper --dry-run
  python netradyne_green_zone.py files --directory ./netradyne_score_data
  python netradyne_green_zone.py --debug
        """
    )
    
    # Use a subparsers for methods to handle arguments more cleanly
    subparsers = parser.add_subparsers(dest='method', help='The data fetching method to use.')
    subparsers.required = True
    
    # Add parser for each method
    subparsers.add_parser('api', help='Fetch data using the Netradyne API.')
    subparsers.add_parser('scraper', help='Fetch data using the web scraper.')
    
    files_parser = subparsers.add_parser('files', help='Process data from local files.')
    files_parser.add_argument(
        '--directory', 
        '-d',
        default=None,
        help="Directory for 'files' method. Required for this method."
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without writing to the database."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run in debug mode to test components."
    )

    args = parser.parse_args()
    
    # If debug is passed as the main command
    if len(sys.argv) > 1 and sys.argv[1] == '--debug':
        setup_directories()
        return debug_mode()

    if args.method == DataFetchMethod.FILES.value and not args.directory:
        parser.error("The 'files' method requires the --directory argument.")
        return 1

    # Setup directories
    setup_directories()

    processor = NetradyneDataProcessor()

    try:
        method = DataFetchMethod(args.method)
    except ValueError:
        # This should not be reached due to argparse choices, but good for safety
        logging.error(f"Internal error: Invalid method {args.method} passed validation.")
        return 1
    
    # For 'files' method, the directory comes from its own namespace
    directory = args.directory if hasattr(args, 'directory') else None
    
    success = processor.process_and_store_data(method, directory, args.dry_run)
    return 0 if success else 1


def debug_mode():
    """Debug mode to test all components."""
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Running in DEBUG mode - testing all components")
    
    processor = NetradyneDataProcessor()
    
    try:
        # Test database connection
        logging.debug("Testing database connection...")
        with DatabaseManager(processor.config) as db_manager:
            if db_manager.connection:
                logging.debug("Database connection successful")
            else:
                logging.error("Database connection failed")
                return 1
        
        # Test file processor
        logging.debug("Testing file processor...")
        file_processor = FileProcessor(processor.config)
        files = file_processor.find_data_files()
        logging.debug(f"Found {len(files)} data files")
        
        # Test API client (without actually making calls)
        logging.debug("Testing API client...")
        api_client = NetradyneAPIClient(processor.config)
        timestamp_ms, report_month = api_client.get_previous_month_details()
        logging.debug(f"Target month: {report_month}, Timestamp: {timestamp_ms}")
        
        logging.info("Debug tests completed successfully")
        return 0
        
    except Exception as e:
        logging.error(f"Debug mode error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())



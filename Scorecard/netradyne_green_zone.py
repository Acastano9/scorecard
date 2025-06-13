#!/usr/bin/env python3
"""
Netradyne Data Processing Script

This script can fetch Netradyne driver score data using three different methods:
1. API - Direct API calls to Netradyne
2. Scraper - Web scraping to download CSV files
3. Files - Processing existing Excel/CSV files from local directory

Uses modular utilities for clean separation of concerns.
"""

import logging
import sys
import os
from enum import Enum
from typing import Optional, Tuple, List, Dict, Any

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import (
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
        logging.FileHandler('netradyne_processing.log') if os.path.exists('logs') else logging.StreamHandler()
    ]
)


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
    
    def process_and_store_data(self, method: DataFetchMethod, directory: str = None) -> bool:
        """
        Main processing workflow that fetches data and stores it in the database.
        
        Args:
            method: Data fetching method to use.
            directory: Directory for file processing (only used with FILES method).
        
        Returns:
            Boolean indicating success.
        """
        logging.info(f"Starting data processing using method: {method.value}")
        
        with DatabaseManager(self.config) as db_manager:
            self.db_manager = db_manager
            
            try:
                # Create database connection
                if not db_manager.connection:
                    error_msg = "Database connection failure"
                    logging.error(f"Exiting due to {error_msg}.")
                    return False
                
                # Fetch data based on method
                if method == DataFetchMethod.API:
                    result = self.fetch_data_via_api()
                elif method == DataFetchMethod.SCRAPER:
                    result = self.fetch_data_via_scraper()
                elif method == DataFetchMethod.FILES:
                    result = self.fetch_data_via_files(directory)
                else:
                    logging.error(f"Unknown method: {method}")
                    return False
                
                if result is None:
                    error_msg = f"Failed to retrieve data using method: {method.value}"
                    logging.error(error_msg)
                    db_manager.update_script_status(False, error_msg)
                    return False
                
                scores, report_month = result
                
                # Store data in database
                success = db_manager.insert_driver_scores(scores, report_month)
                
                # Update status
                if success:
                    db_manager.update_script_status(True)
                    logging.info(f"Successfully completed data processing using {method.value}")
                    return True
                else:
                    error_msg = f"Failed to insert data into database"
                    db_manager.update_script_status(False, error_msg)
                    logging.error(error_msg)
                    return False
                    
            except Exception as e:
                error_msg = f"Unexpected error in {method.value} processing: {str(e)}"
                logging.error(error_msg)
                db_manager.update_script_status(False, error_msg)
                return False


def main():
    """Main execution function."""
    processor = NetradyneDataProcessor()
    
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python netradyne_green_zone.py <method> [directory]")
        print("Methods: api, scraper, files")
        print("Directory: (optional) for files method, specify directory containing data files")
        return 1
    
    method_str = sys.argv[1].lower()
    directory = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Validate method
    try:
        method = DataFetchMethod(method_str)
    except ValueError:
        logging.error(f"Invalid method: {method_str}. Valid methods: api, scraper, files")
        return 1
    
    # Process data
    success = processor.process_and_store_data(method, directory)
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


def interactive_mode():
    """Interactive mode for user-guided execution."""
    print("Netradyne Data Processing - Interactive Mode")
    print("=" * 50)
    
    processor = NetradyneDataProcessor()
    
    while True:
        print("\nSelect data fetching method:")
        print("1. API - Fetch data directly from Netradyne API")
        print("2. Scraper - Download CSV files using web scraper")
        print("3. Files - Process existing Excel/CSV files")
        print("4. Debug - Test all components")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            success = processor.process_and_store_data(DataFetchMethod.API)
            print(f"API processing: {'SUCCESS' if success else 'FAILED'}")
            
        elif choice == '2':
            success = processor.process_and_store_data(DataFetchMethod.SCRAPER)
            print(f"Scraper processing: {'SUCCESS' if success else 'FAILED'}")
            
        elif choice == '3':
            directory = input("Enter directory path (or press Enter for default): ").strip()
            directory = directory if directory else None
            success = processor.process_and_store_data(DataFetchMethod.FILES, directory)
            print(f"File processing: {'SUCCESS' if success else 'FAILED'}")
            
        elif choice == '4':
            debug_mode()
            
        elif choice == '5':
            print("Exiting...")
            break
            
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    # Check for special modes
    if len(sys.argv) > 1:
        if sys.argv[1] == '--debug':
            exit_code = debug_mode()
        elif sys.argv[1] == '--interactive':
            interactive_mode()
            exit_code = 0
        else:
            exit_code = main()
    else:
        # Default to interactive mode if no arguments
        interactive_mode()
        exit_code = 0
    
    sys.exit(exit_code)



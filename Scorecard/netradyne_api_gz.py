#!/usr/bin/env python3
"""
Netradyne API Data Import Script

This script fetches Netradyne driver score data using the API and stores it in the database.
Uses modular utilities for clean separation of concerns.
"""

import logging
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import ConfigManager, DatabaseManager, NetradyneAPIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('netradyne_api.log') if os.path.exists('logs') else logging.StreamHandler()
    ]
)


def main():
    """Main execution function for API-based data import."""
    logging.info("Starting Netradyne API data import script.")
    
    # Initialize configuration
    config = ConfigManager()
    
    # Initialize database manager
    with DatabaseManager(config) as db_manager:
        try:
            # Create database connection
            if not db_manager.connection:
                error_msg = "Database connection failure"
                logging.error(f"Exiting script due to {error_msg}.")
                return 1
            
            # Initialize API client
            api_client = NetradyneAPIClient(config)
            
            # Fetch driver scores
            result = api_client.fetch_driver_scores()
            if result is None:
                error_msg = "Failed to retrieve scores from API"
                logging.error(f"Exiting script because {error_msg}.")
                db_manager.update_script_status(False, error_msg)
                return 1
            
            scores, report_month = result
            
            # Insert scores into database
            success = db_manager.insert_driver_scores(scores, report_month)
            
            # Update status based on insertion result
            if success:
                db_manager.update_script_status(True)
                logging.info("Script finished successfully.")
                return 0
            else:
                error_msg = "Failed to insert scores into database"
                db_manager.update_script_status(False, error_msg)
                logging.error("Script finished with errors during database insertion.")
                return 1
                
        except Exception as e:
            # Catch any unexpected exceptions
            error_msg = f"Unexpected error: {str(e)}"
            logging.error(error_msg)
            db_manager.update_script_status(False, error_msg)
            return 1


def debug_mode():
    """Debug mode with additional logging and error handling."""
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Running in DEBUG mode")
    
    try:
        # Initialize configuration
        config = ConfigManager()
        
        # Test database connection
        logging.debug("Testing database connection...")
        with DatabaseManager(config) as db_manager:
            if db_manager.connection:
                logging.debug("Database connection successful")
            else:
                logging.error("Database connection failed")
                return 1
        
        # Test API client
        logging.debug("Testing API client...")
        api_client = NetradyneAPIClient(config)
        
        # Test token retrieval
        token = api_client.get_auth_token()
        if token:
            logging.debug(f"Token retrieved successfully: {token[:20]}...")
        else:
            logging.error("Failed to retrieve API token")
            return 1
        
        # Test month calculation
        timestamp_ms, report_month = api_client.get_previous_month_details()
        logging.debug(f"Target month: {report_month}, Timestamp: {timestamp_ms}")
        
        logging.info("Debug tests completed successfully")
        return 0
        
    except Exception as e:
        logging.error(f"Debug mode error: {e}")
        return 1


if __name__ == "__main__":
    # Check for debug flag
    if len(sys.argv) > 1 and sys.argv[1] == '--debug':
        exit_code = debug_mode()
    else:
        exit_code = main()
    
    sys.exit(exit_code) 
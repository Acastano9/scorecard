#!/usr/bin/env python3
"""
HOS Violations ETL Script

This script processes Hours of Service (HOS) violations data from JSON and Excel files,
storing the results in the database using modular utilities.

Usage:
    python hos_violations_etl.py [options]
    
Options:
    --directory <path>    Specify directory containing HOS violation files
    --debug              Enable debug mode with additional logging
    --file <path>       Process a specific file instead of directory
    --dry-run           Perform a dry run without saving to the database
    --help              Show this help message

Examples:
    python hos_violations_etl.py
    python hos_violations_etl.py --directory ./custom_data --debug
    python hos_violations_etl.py --file violations.json
    python hos_violations_etl.py --dry-run
"""

import logging
import sys
import os
import argparse
from pathlib import Path
import json

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Add .utils to path to enable direct import
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '.utils'))

from config_utils import ConfigManager
from database_utils import DatabaseManager
from hos_violations_utils import HOSViolationsProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/hos_violations.log') if os.path.exists('logs') else logging.StreamHandler()
    ]
)


def setup_directories():
    """Create necessary directories if they don't exist."""
    directories = [
        'hos_violations_data',
        'hos_violations_data/processed',
        'hos_violations_data/errors',
        'logs'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        

def process_hos_violations(directory=None, specific_file=None, dry_run=False):
    """
    Main function to process HOS violations data.
    
    Args:
        directory: Directory containing HOS violation files
        specific_file: Specific file to process
        dry_run: If True, performs a dry run without saving to the database
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logging.info("Starting HOS violations ETL process")
    
    try:
        # Initialize configuration
        config = ConfigManager()
        
        # Initialize processor
        processor = HOSViolationsProcessor(config)
        
        # Process data
        violations = []
        report_month = processor.get_report_month()
        
        if specific_file:
            # Process single file
            logging.info(f"Processing specific file: {specific_file}")
            
            if not os.path.exists(specific_file):
                logging.error(f"File not found: {specific_file}")
                return 1
            
            file_path = Path(specific_file)
            if file_path.suffix.lower() == '.json':
                violations = processor.parse_json_file(specific_file)
            elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                df = processor.read_hos_excel_file(specific_file)
                if df is not None:
                    violations = processor.process_excel_dataframe(df)
                else:
                    logging.error(f"Could not process Excel file: {specific_file}")
                    return 1
            else:
                logging.error(f"Unsupported file format: {file_path.suffix}")
                return 1
                
        else:
            # Process directory
            result = processor.process_all_hos_files(directory)
            if result is None:
                logging.error("No HOS violation data found to process")
                return 1
            
            violations, report_month = result
        
        if not violations:
            logging.warning("No valid HOS violation data found")
            return 1
        
        if dry_run:
            logging.info("Performing a dry run. Data will not be saved to the database.")
            print("\n" + "="*60)
            print("DRY RUN - PROCESSED HOS VIOLATIONS")
            print("="*60)
            print(json.dumps(violations, indent=4, default=str))
            print(f"\nFound {len(violations)} violations.")
            print("="*60)
            logging.info("Dry run complete. Data was not saved.")
            return 0
        
        # Store in database
        with DatabaseManager(config) as db_manager:
            if not db_manager.connection:
                logging.error("Database connection failure")
                return 1
            
            success = db_manager.insert_hos_violations(violations, report_month)
            
            if not success:
                logging.error("Failed to insert HOS violations into database")
                return 1
        
        logging.info("HOS violations ETL process completed successfully")
        return 0
        
    except Exception as e:
        logging.error(f"Unexpected error in HOS violations ETL: {e}")
        return 1


def debug_mode():
    """Run HOS violations ETL in debug mode."""
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Running HOS violations ETL in DEBUG mode")
    
    try:
        # Test configuration
        logging.debug("Testing configuration...")
        config = ConfigManager()
        logging.debug("Configuration loaded successfully")
        
        # Test database connection
        logging.debug("Testing database connection...")
        with DatabaseManager(config) as db_manager:
            if db_manager.connection:
                logging.debug("Database connection successful")
                
                # Test HOS violations query
                summary = db_manager.get_hos_violations_summary()
                logging.debug(f"Found {len(summary)} drivers with violations in database")
            else:
                logging.error("Database connection failed")
                return 1
        
        # Test processor
        logging.debug("Testing HOS violations processor...")
        processor = HOSViolationsProcessor(config)
        
        # Test file discovery
        files = processor.find_hos_files()
        logging.debug(f"Found files: Excel={len(files['excel'])}, JSON={len(files['json'])}")
        
        logging.info("Debug tests completed successfully")
        return 0
        
    except Exception as e:
        logging.error(f"Debug mode error: {e}")
        return 1


def main():
    """Main entry point for HOS violations ETL script."""
    parser = argparse.ArgumentParser(
        description="HOS Violations ETL Script - Process violations data from JSON and Excel files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python hos_violations_etl.py
  python hos_violations_etl.py --directory ./custom_data --debug
  python hos_violations_etl.py --file violations.json
  python hos_violations_etl.py --dry-run
        """
    )
    
    parser.add_argument('--directory', '-d', help='Directory containing HOS violation files')
    parser.add_argument('--file', '-f', help='Process specific file instead of directory')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without saving to the database')
    
    args = parser.parse_args()
    
    # Setup directories
    setup_directories()
    
    # Handle different modes
    if args.debug:
        return debug_mode()
    else:
        return process_hos_violations(
            directory=args.directory,
            specific_file=args.file,
            dry_run=args.dry_run
        )


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 
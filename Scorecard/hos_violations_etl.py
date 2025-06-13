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
    --interactive        Run in interactive mode with menu
    --analyze           Perform analysis of processed violations
    --file <path>       Process a specific file instead of directory
    --help              Show this help message

Examples:
    python hos_violations_etl.py
    python hos_violations_etl.py --directory ./custom_data --debug
    python hos_violations_etl.py --file violations.json --analyze
    python hos_violations_etl.py --interactive
"""

import logging
import sys
import os
import argparse
from pathlib import Path

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
        

def process_hos_violations(directory=None, specific_file=None, analyze=False):
    """
    Main function to process HOS violations data.
    
    Args:
        directory: Directory containing HOS violation files
        specific_file: Specific file to process
        analyze: Whether to perform analysis
    
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
        
        # Store in database
        with DatabaseManager(config) as db_manager:
            if not db_manager.connection:
                logging.error("Database connection failure")
                return 1
            
            success = db_manager.insert_hos_violations(violations, report_month)
            
            if not success:
                logging.error("Failed to insert HOS violations into database")
                return 1
            
            # Perform analysis if requested
            if analyze:
                logging.info("Performing HOS violations analysis...")
                analysis = processor.analyze_violations(violations)
                
                if analysis:
                    print("\n" + "="*60)
                    print("HOS VIOLATIONS ANALYSIS REPORT")
                    print("="*60)
                    print(f"Total violations: {analysis['total_violations']}")
                    print(f"Unique drivers: {analysis['unique_drivers']}")
                    print(f"Unique violation types: {analysis['unique_violation_types']}")
                    print(f"Unique terminals: {analysis['unique_terminals']}")
                    
                    print(f"\nDate range: {analysis['date_range']['earliest']} to {analysis['date_range']['latest']}")
                    
                    print("\nTop 5 drivers with most violations:")
                    for i, (driver, count) in enumerate(analysis['top_drivers'][:5], 1):
                        print(f"  {i}. {driver}: {count} violations")
                    
                    print("\nViolation type distribution:")
                    for violation_type, count in analysis['violation_type_distribution']:
                        print(f"  {violation_type}: {count} violations")
                    
                    if analysis['top_terminals']:
                        print("\nTop terminals with violations:")
                        for i, (terminal, count) in enumerate(analysis['top_terminals'][:5], 1):
                            print(f"  {i}. {terminal}: {count} violations")
                    
                    print("="*60)
                
                # Also get database summary
                summary = db_manager.get_hos_violations_summary()
                if summary:
                    print("\nDatabase Summary (All Time):")
                    print(f"Total drivers with violations: {len(summary)}")
                    for i, driver_info in enumerate(summary[:10], 1):
                        print(f"  {i}. {driver_info['Driver_Name']} ({driver_info['Driver_ID']}): "
                              f"{driver_info['violation_count']} violations")
        
        logging.info("HOS violations ETL process completed successfully")
        return 0
        
    except Exception as e:
        logging.error(f"Unexpected error in HOS violations ETL: {e}")
        return 1


def interactive_mode():
    """Run HOS violations ETL in interactive mode."""
    print("\n" + "="*60)
    print("HOS VIOLATIONS ETL - INTERACTIVE MODE")
    print("="*60)
    
    while True:
        print("\nAvailable options:")
        print("1. Process all files in default directory")
        print("2. Process files in custom directory")
        print("3. Process specific file")
        print("4. Process with analysis")
        print("5. View current database summary")
        print("6. Exit")
        
        choice = input("\nSelect an option (1-6): ").strip()
        
        if choice == '1':
            print("\nProcessing all files in 'hos_violations_data' directory...")
            exit_code = process_hos_violations()
            print(f"Process completed with exit code: {exit_code}")
            
        elif choice == '2':
            directory = input("Enter directory path: ").strip()
            if directory:
                print(f"\nProcessing all files in '{directory}' directory...")
                exit_code = process_hos_violations(directory=directory)
                print(f"Process completed with exit code: {exit_code}")
            else:
                print("Invalid directory path")
                
        elif choice == '3':
            file_path = input("Enter file path: ").strip()
            if file_path:
                print(f"\nProcessing file '{file_path}'...")
                exit_code = process_hos_violations(specific_file=file_path)
                print(f"Process completed with exit code: {exit_code}")
            else:
                print("Invalid file path")
                
        elif choice == '4':
            directory = input("Enter directory path (or press Enter for default): ").strip()
            directory = directory if directory else None
            print(f"\nProcessing with analysis...")
            exit_code = process_hos_violations(directory=directory, analyze=True)
            print(f"Process completed with exit code: {exit_code}")
            
        elif choice == '5':
            try:
                config = ConfigManager()
                with DatabaseManager(config) as db_manager:
                    if db_manager.connection:
                        summary = db_manager.get_hos_violations_summary()
                        if summary:
                            print(f"\nHOS Violations Database Summary:")
                            print(f"Total drivers with violations: {len(summary)}")
                            print("\nTop 10 drivers:")
                            for i, driver_info in enumerate(summary[:10], 1):
                                print(f"  {i}. {driver_info['Driver_Name']} ({driver_info['Driver_ID']}): "
                                      f"{driver_info['violation_count']} violations - "
                                      f"Latest: {driver_info['latest_violation']}")
                        else:
                            print("No HOS violations found in database")
                    else:
                        print("Could not connect to database")
            except Exception as e:
                print(f"Error retrieving database summary: {e}")
                
        elif choice == '6':
            print("Exiting...")
            break
            
        else:
            print("Invalid option. Please select 1-6.")


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
  python hos_violations_etl.py --file violations.json --analyze
  python hos_violations_etl.py --interactive
        """
    )
    
    parser.add_argument('--directory', '-d', help='Directory containing HOS violation files')
    parser.add_argument('--file', '-f', help='Process specific file instead of directory')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--interactive', '-i', action='store_true', help='Run in interactive mode')
    parser.add_argument('--analyze', '-a', action='store_true', help='Perform analysis of processed violations')
    
    args = parser.parse_args()
    
    # Setup directories
    setup_directories()
    
    # Handle different modes
    if args.debug:
        return debug_mode()
    elif args.interactive:
        interactive_mode()
        return 0
    else:
        return process_hos_violations(
            directory=args.directory,
            specific_file=args.file,
            analyze=args.analyze
        )


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 
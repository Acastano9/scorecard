#!/usr/bin/env python3
"""
Programmed Maintenance ETL Script

This script processes Excel files that users manually place in the 'programmed_maintenance' folder.
It replaces the win32com automation by requiring users to manually download and place Excel files.

Usage:
    python3 programmed_maintenance_etl.py                      # Process all files in default directory
    python3 programmed_maintenance_etl.py --file report.xlsx   # Process specific file
    python3 programmed_maintenance_etl.py --analyze           # Process with analysis
    python3 programmed_maintenance_etl.py --interactive       # Interactive mode
    python3 programmed_maintenance_etl.py --debug             # Debug mode
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# Add the .utils directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '.utils'))

from config_utils import ConfigManager
from maintenance_utils import MaintenanceProcessor
from database_utils import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/programmed_maintenance_etl.log'),
        logging.StreamHandler()
    ]
)

def setup_directories():
    """Create necessary directories if they don't exist."""
    directories = ['programmed_maintenance', 'logs']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def process_programmed_maintenance(directory=None, specific_file=None, analyze=False):
    """
    Main function to process programmed maintenance data.
    
    Args:
        directory: Directory containing maintenance Excel files
        specific_file: Specific file to process
        analyze: Whether to perform analysis
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logging.info("Starting Programmed Maintenance ETL process")
    
    try:
        # Initialize configuration
        config = ConfigManager()
        
        # Initialize processor
        processor = MaintenanceProcessor(config)
        
        # Use default directory if none specified
        if directory is None:
            directory = "programmed_maintenance"
        
        # Process data
        maintenance_records = []
        process_date = processor.get_current_date()
        metrics = {}
        
        if specific_file:
            # Process single file
            logging.info(f"Processing specific file: {specific_file}")
            
            if not os.path.exists(specific_file):
                logging.error(f"File not found: {specific_file}")
                return 1
            
            file_path = Path(specific_file)
            if file_path.suffix.lower() not in ['.xlsx', '.xls']:
                logging.error(f"Unsupported file format: {file_path.suffix}")
                return 1
            
            df = processor.read_maintenance_file(specific_file)
            if df is not None:
                maintenance_records = processor.process_maintenance_dataframe(df)
                if analyze:
                    metrics = processor.calculate_maintenance_metrics(maintenance_records)
            else:
                logging.error(f"Could not process file: {specific_file}")
                return 1
                
        else:
            # Process directory
            result = processor.process_all_maintenance_files(directory)
            if result is None:
                logging.error("No maintenance data found to process")
                return 1
            
            maintenance_records, process_date, metrics = result
        
        if not maintenance_records:
            logging.warning("No maintenance records found to process")
            return 1
        
        logging.info(f"Processed {len(maintenance_records)} maintenance records")
        
        # Display analysis if requested
        if analyze and metrics:
            display_analysis(metrics)
        
        # Store data in database
        db_manager = DatabaseManager(config)
        success = db_manager.store_maintenance_records(maintenance_records, process_date)
        
        if success:
            logging.info("Successfully stored maintenance data in database")
            return 0
        else:
            logging.error("Failed to store maintenance data in database")
            return 1
            
    except Exception as e:
        logging.error(f"Error in programmed maintenance processing: {e}")
        return 1

def display_analysis(metrics: Dict[str, Any]):
    """
    Display maintenance analysis results.
    
    Args:
        metrics: Maintenance metrics dictionary.
    """
    print("\n" + "="*60)
    print("PROGRAMMED MAINTENANCE ANALYSIS")
    print("="*60)
    
    print(f"Total Maintenance Items: {metrics.get('total_maintenance_items', 0)}")
    print(f"Overdue Items: {metrics.get('overdue_count', 0)}")
    print(f"Overdue Percentage: {metrics.get('overdue_percentage', 0):.1f}%")
    
    print(f"\nStatus Breakdown:")
    for status, count in metrics.get('status_breakdown', {}).items():
        print(f"  {status}: {count}")
    
    print(f"\nPriority Breakdown:")
    for priority, count in metrics.get('priority_breakdown', {}).items():
        print(f"  {priority}: {count}")
    
    print(f"\nMaintenance Type Breakdown:")
    for maint_type, count in metrics.get('type_breakdown', {}).items():
        print(f"  {maint_type}: {count}")
    
    print("="*60)

def interactive_mode():
    """Interactive mode for user-guided execution."""
    print("Programmed Maintenance ETL - Interactive Mode")
    print("=" * 50)
    
    while True:
        print("\nSelect processing option:")
        print("1. Process all Excel files in programmed_maintenance folder")
        print("2. Process files in custom directory")
        print("3. Process specific Excel file")
        print("4. Process with analysis")
        print("5. View processing instructions")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            print("\nProcessing all files in programmed_maintenance folder...")
            result = process_programmed_maintenance()
            print(f"Processing: {'SUCCESS' if result == 0 else 'FAILED'}")
            
        elif choice == '2':
            directory = input("Enter directory path: ").strip()
            if directory and os.path.exists(directory):
                print(f"\nProcessing files in {directory}...")
                result = process_programmed_maintenance(directory=directory)
                print(f"Processing: {'SUCCESS' if result == 0 else 'FAILED'}")
            else:
                print("Directory not found or invalid.")
                
        elif choice == '3':
            file_path = input("Enter Excel file path: ").strip()
            if file_path and os.path.exists(file_path):
                print(f"\nProcessing file: {file_path}")
                result = process_programmed_maintenance(specific_file=file_path)
                print(f"Processing: {'SUCCESS' if result == 0 else 'FAILED'}")
            else:
                print("File not found or invalid.")
                
        elif choice == '4':
            print("\nProcessing with analysis...")
            result = process_programmed_maintenance(analyze=True)
            print(f"Processing: {'SUCCESS' if result == 0 else 'FAILED'}")
            
        elif choice == '5':
            display_instructions()
            
        elif choice == '6':
            print("Exiting...")
            break
            
        else:
            print("Invalid choice. Please try again.")

def display_instructions():
    """Display instructions for using the ETL script."""
    print("\n" + "="*70)
    print("PROGRAMMED MAINTENANCE ETL INSTRUCTIONS")
    print("="*70)
    print("\n1. MANUAL FILE PLACEMENT:")
    print("   - Download maintenance Excel files from your system")
    print("   - Place them in the 'programmed_maintenance' folder")
    print("   - Supported formats: .xlsx, .xls")
    
    print("\n2. EXPECTED EXCEL COLUMNS:")
    print("   Required columns (flexible naming):")
    print("   - Vehicle ID (Vehicle ID, vehicle_id, Unit_ID, Truck_ID)")
    print("   - Maintenance Type (Maintenance Type, Service_Type, Work_Type)")
    print("   - Due Date (Due Date, Service_Due, Scheduled_Date)")
    print("   ")
    print("   Optional columns:")
    print("   - Vehicle Number, Last Service, Mileage, Status, Priority, Location")
    
    print("\n3. PROCESSING OPTIONS:")
    print("   - Interactive Mode: python3 programmed_maintenance_etl.py --interactive")
    print("   - Process All: python3 programmed_maintenance_etl.py")
    print("   - Specific File: python3 programmed_maintenance_etl.py --file report.xlsx")
    print("   - With Analysis: python3 programmed_maintenance_etl.py --analyze")
    
    print("\n4. OUTPUT:")
    print("   - Data stored in VTUtility.dbo.Maintenance_Records table")
    print("   - Processing logs in logs/programmed_maintenance_etl.log")
    print("   - Analysis includes overdue items, status breakdown, and metrics")
    
    print("\n5. NOTES:")
    print("   - Files are processed but not moved/deleted")
    print("   - Duplicate prevention based on vehicle ID and maintenance type")
    print("   - Database transaction safety with rollback on errors")
    print("="*70)

def debug_mode():
    """Debug mode for testing components."""
    print("Programmed Maintenance ETL - Debug Mode")
    print("=" * 50)
    
    try:
        # Test configuration
        print("Testing configuration...")
        config = ConfigManager()
        print("✓ Configuration loaded successfully")
        
        # Test processor
        print("Testing maintenance processor...")
        processor = MaintenanceProcessor(config)
        print("✓ Maintenance processor initialized")
        
        # Test directory access
        print("Testing directory access...")
        files = processor.find_maintenance_files("programmed_maintenance")
        print(f"✓ Found {len(files)} files in programmed_maintenance folder")
        
        # Test database connection
        print("Testing database connection...")
        db_manager = DatabaseManager(config)
        if db_manager.test_connection():
            print("✓ Database connection successful")
        else:
            print("✗ Database connection failed")
        
        print("\nDebug test completed successfully!")
        
    except Exception as e:
        print(f"✗ Debug test failed: {e}")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Programmed Maintenance ETL Script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 programmed_maintenance_etl.py
  python3 programmed_maintenance_etl.py --file maintenance_report.xlsx
  python3 programmed_maintenance_etl.py --analyze
  python3 programmed_maintenance_etl.py --interactive
  python3 programmed_maintenance_etl.py --debug
        """
    )
    
    parser.add_argument('--directory', '-d', type=str, 
                       help='Directory containing maintenance Excel files')
    parser.add_argument('--file', '-f', type=str,
                       help='Specific Excel file to process')
    parser.add_argument('--analyze', '-a', action='store_true',
                       help='Perform analysis on the processed data')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='Run in interactive mode')
    parser.add_argument('--debug', action='store_true',
                       help='Run in debug mode')
    
    args = parser.parse_args()
    
    # Setup directories
    setup_directories()
    
    # Handle different modes
    if args.debug:
        debug_mode()
    elif args.interactive:
        interactive_mode()
    else:
        # Standard processing
        exit_code = process_programmed_maintenance(
            directory=args.directory,
            specific_file=args.file,
            analyze=args.analyze
        )
        sys.exit(exit_code)

if __name__ == "__main__":
    main() 
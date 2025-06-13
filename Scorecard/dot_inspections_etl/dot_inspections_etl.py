#!/usr/bin/env python3
"""
DOT Inspections ETL Script

This script processes FMCSA DOT inspection XML files and imports them into the database.
It uses modular utilities for clean separation of concerns and maintainability.

Usage:
    python dot_inspections_etl.py -f file.xml        # Process single file
    python dot_inspections_etl.py -d directory       # Process directory
    python dot_inspections_etl.py --interactive      # Interactive mode
    python dot_inspections_etl.py --debug            # Debug mode
"""

import logging
import sys
import os
import argparse
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from .utils import (
        DOTConfigManager,
        DOTDatabaseManager,
        XMLProcessor,
        InspectionProcessor,
        FileManager
    )
except ImportError:
    from utils import (
        DOTConfigManager,
        DOTDatabaseManager,
        XMLProcessor,
        InspectionProcessor,
        FileManager
    )

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('dot_inspections_etl/logs/dot_inspections_etl.log') 
        if os.path.exists('dot_inspections_etl/logs') else logging.StreamHandler()
    ]
)


class DOTInspectionsETL:
    """Main ETL processor for DOT inspections."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the ETL processor.
        
        Args:
            config_path: Optional path to configuration file.
        """
        self.config = DOTConfigManager(config_path)
        self.xml_processor = XMLProcessor()
        self.inspection_processor = InspectionProcessor()
        self.file_manager = FileManager(self.config)
        
    def process_single_file(self, file_path: str) -> Dict[str, int]:
        """
        Process a single XML file.
        
        Args:
            file_path: Path to XML file to process.
        
        Returns:
            Dictionary with processing results.
        """
        results = {
            'total_inspections': 0,
            'processed_inspections': 0,
            'skipped_existing': 0,
            'driver_not_found': 0,
            'inserted_successfully': 0,
            'errors': 0
        }
        
        try:
            # Validate file
            if not self.file_manager.validate_file_path(file_path):
                logging.error(f"Invalid file path: {file_path}")
                results['errors'] = 1
                return results
            
            # Get file info
            file_info = self.file_manager.get_file_info(file_path)
            logging.info(f"Processing file: {file_info['filename']} ({file_info['size_mb']} MB)")
            
            # Parse XML
            xml_data = self.xml_processor.parse_xml_file(file_path)
            if not xml_data:
                error_msg = f"Failed to parse XML file: {file_path}"
                logging.error(error_msg)
                self.file_manager.move_error_file(file_path, error_msg)
                results['errors'] = 1
                return results
            
            # Extract inspections
            inspections = self.xml_processor.extract_inspections(xml_data)
            if not inspections:
                error_msg = f"No inspections found in XML file: {file_path}"
                logging.error(error_msg)
                self.file_manager.move_error_file(file_path, error_msg)
                results['errors'] = 1
                return results
            
            results['total_inspections'] = len(inspections)
            logging.info(f"Found {len(inspections)} inspections in XML file")
            
            # Process with database
            with DOTDatabaseManager(self.config) as db_manager:
                if not db_manager.connection:
                    error_msg = "Failed to connect to database"
                    logging.error(error_msg)
                    results['errors'] = 1
                    return results
                
                # Get existing inspections
                existing_inspections = db_manager.get_existing_inspections()
                
                # Process inspections
                processed_inspections = self.inspection_processor.process_inspections_batch(inspections)
                results['processed_inspections'] = len(processed_inspections)
                
                # Insert into database
                for inspection_data in processed_inspections:
                    inspection_id = inspection_data['inspection_id']
                    
                    # Skip if already exists
                    if inspection_id in existing_inspections:
                        logging.debug(f"Skipping existing inspection: {inspection_id}")
                        results['skipped_existing'] += 1
                        continue
                    
                    # Get driver ID
                    driver_id = db_manager.get_driver_id(inspection_data['license_number'])
                    if not driver_id:
                        logging.warning(f"Driver not found for license: {inspection_data['license_number']}")
                        results['driver_not_found'] += 1
                        continue
                    
                    # Add driver_id to inspection data
                    inspection_data['driver_id'] = driver_id
                    
                    # Validate before insertion
                    if not self.inspection_processor.validate_processed_inspection(inspection_data):
                        logging.warning(f"Invalid inspection data for ID: {inspection_id}")
                        results['errors'] += 1
                        continue
                    
                    # Insert into database
                    if db_manager.insert_inspection(inspection_data):
                        results['inserted_successfully'] += 1
                    else:
                        results['errors'] += 1
                
                # Update script status
                success = results['inserted_successfully'] > 0 and results['errors'] == 0
                error_msg = None if success else f"Processed with {results['errors']} errors"
                db_manager.update_script_status(success, error_msg)
            
            # Move processed file
            if results['inserted_successfully'] > 0:
                self.file_manager.move_processed_file(file_path)
            elif results['errors'] > 0:
                self.file_manager.move_error_file(file_path, f"Processing completed with {results['errors']} errors")
            
            return results
            
        except Exception as e:
            error_msg = f"Unexpected error processing file {file_path}: {str(e)}"
            logging.error(error_msg)
            self.file_manager.move_error_file(file_path, error_msg)
            results['errors'] = 1
            return results
    
    def process_directory(self, directory: str) -> Dict[str, Any]:
        """
        Process all XML files in a directory.
        
        Args:
            directory: Directory containing XML files.
        
        Returns:
            Dictionary with overall processing results.
        """
        overall_results = {
            'files_processed': 0,
            'files_failed': 0,
            'total_inspections': 0,
            'total_inserted': 0,
            'total_errors': 0
        }
        
        try:
            # Find XML files
            xml_files = self.file_manager.find_xml_files(directory)
            
            if not xml_files:
                logging.warning(f"No XML files found in directory: {directory}")
                return overall_results
            
            logging.info(f"Found {len(xml_files)} XML files to process")
            
            # Process each file
            for i, file_path in enumerate(xml_files, 1):
                logging.info(f"\n--- Processing file {i}/{len(xml_files)}: {Path(file_path).name} ---")
                
                results = self.process_single_file(file_path)
                
                # Update overall results
                if results['errors'] > 0:
                    overall_results['files_failed'] += 1
                else:
                    overall_results['files_processed'] += 1
                
                overall_results['total_inspections'] += results['total_inspections']
                overall_results['total_inserted'] += results['inserted_successfully']
                overall_results['total_errors'] += results['errors']
                
                # Log file results
                logging.info(f"File {i} results: {results}")
            
            # Log overall results
            logging.info(f"\n=== Overall Processing Results ===")
            logging.info(f"Files processed successfully: {overall_results['files_processed']}")
            logging.info(f"Files failed: {overall_results['files_failed']}")
            logging.info(f"Total inspections found: {overall_results['total_inspections']}")
            logging.info(f"Total inspections inserted: {overall_results['total_inserted']}")
            logging.info(f"Total errors: {overall_results['total_errors']}")
            
            return overall_results
            
        except Exception as e:
            logging.error(f"Error processing directory {directory}: {e}")
            overall_results['files_failed'] = 1
            overall_results['total_errors'] = 1
            return overall_results


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description='Process FMCSA DOT inspection XML files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Process single file
    python dot_inspections_etl.py -f inspections_2024.xml
    
    # Process directory
    python dot_inspections_etl.py -d /path/to/xml/files
    
    # Interactive mode
    python dot_inspections_etl.py --interactive
    
    # Debug mode
    python dot_inspections_etl.py --debug
        """
    )
    
    parser.add_argument('-f', '--file', help='Path to XML file to process')
    parser.add_argument('-d', '--dir', help='Directory containing XML files to process')
    parser.add_argument('--interactive', action='store_true', help='Run in interactive mode')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    
    return parser


def handle_file_not_found(file_path: str, file_manager: FileManager) -> None:
    """Handle file not found error with helpful suggestions."""
    file_path_obj = Path(file_path)
    
    error_msg = f"Error: File not found: {file_path}"
    
    # Look for similar files
    similar_files = file_manager.find_similar_files(file_path)
    
    if similar_files:
        suggestions = "\n".join([f"  - {Path(f).name}" for f in similar_files])
        error_msg += f"\n\nDid you mean one of these files?\n{suggestions}"
    
    print(error_msg)


def interactive_mode() -> None:
    """Run in interactive mode with user prompts."""
    print("DOT Inspections ETL - Interactive Mode")
    print("=" * 50)
    
    etl = DOTInspectionsETL()
    
    while True:
        print("\nSelect processing mode:")
        print("1. Process single file")
        print("2. Process directory")
        print("3. View file information")
        print("4. Debug mode")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            file_path = input("Enter path to XML file: ").strip()
            if file_path:
                results = etl.process_single_file(file_path)
                print(f"Processing results: {results}")
            else:
                print("No file path provided")
                
        elif choice == '2':
            directory = input("Enter directory path (or press Enter for default): ").strip()
            if not directory:
                directory = etl.config.file_paths['xml_data_dir']
            results = etl.process_directory(directory)
            print(f"Processing results: {results}")
            
        elif choice == '3':
            file_path = input("Enter path to file: ").strip()
            if file_path:
                info = etl.file_manager.get_file_info(file_path)
                print(f"File information: {info}")
            else:
                print("No file path provided")
                
        elif choice == '4':
            debug_mode()
            
        elif choice == '5':
            print("Exiting...")
            break
            
        else:
            print("Invalid choice. Please try again.")


def debug_mode() -> None:
    """Run in debug mode for testing components."""
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Running in DEBUG mode")
    
    try:
        etl = DOTInspectionsETL()
        
        # Test database connection
        logging.debug("Testing database connection...")
        with DOTDatabaseManager(etl.config) as db_manager:
            if db_manager.connection:
                logging.debug("Database connection successful")
                
                # Test getting existing inspections
                existing = db_manager.get_existing_inspections()
                logging.debug(f"Found {len(existing)} existing inspections")
            else:
                logging.error("Database connection failed")
                return
        
        # Test file manager
        logging.debug("Testing file manager...")
        xml_files = etl.file_manager.find_xml_files()
        logging.debug(f"Found {len(xml_files)} XML files in default directory")
        
        # Test XML processor with first file if available
        if xml_files:
            first_file = xml_files[0]
            logging.debug(f"Testing XML parsing with: {Path(first_file).name}")
            
            xml_data = etl.xml_processor.parse_xml_file(first_file)
            if xml_data:
                summary = etl.xml_processor.get_inspection_summary(xml_data)
                logging.debug(f"XML file summary: {summary}")
            else:
                logging.error("Failed to parse XML file")
        
        logging.info("Debug tests completed successfully")
        
    except Exception as e:
        logging.error(f"Debug mode error: {e}")


def main() -> int:
    """Main entry point."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Handle debug mode
    if args.debug:
        debug_mode()
        return 0
    
    # Handle interactive mode
    if args.interactive:
        interactive_mode()
        return 0
    
    # Initialize ETL processor
    etl = DOTInspectionsETL()
    
    # Handle file processing
    if args.file:
        file_path = Path(args.file)
        
        if not file_path.exists():
            handle_file_not_found(str(file_path), etl.file_manager)
            return 1
        
        if file_path.suffix.lower() != '.xml':
            print(f"Error: File does not have .xml extension: {file_path}")
            return 1
        
        print(f"Processing file: {file_path}")
        results = etl.process_single_file(str(file_path))
        
        print(f"Processing results: {results}")
        return 0 if results['errors'] == 0 else 1
    
    # Handle directory processing
    elif args.dir:
        dir_path = Path(args.dir)
        
        if not dir_path.exists() or not dir_path.is_dir():
            print(f"Error: {dir_path} is not a valid directory")
            return 1
        
        print(f"Processing directory: {dir_path}")
        results = etl.process_directory(str(dir_path))
        
        return 0 if results['total_errors'] == 0 else 1
    
    # No arguments provided
    else:
        print("Error: Please specify either a file (-f) or directory (-d) to process")
        print("Use --help for more information or --interactive for interactive mode")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 
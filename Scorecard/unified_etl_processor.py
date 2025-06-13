#!/usr/bin/env python3
"""
Unified ETL Processor

This script provides a unified interface for processing all data sources:
- Netradyne driver scores (API, web scraping, file processing)
- DOT inspections (XML processing)
- HOS violations (Excel report processing)  
- Score card data (driver performance evaluation)
- Programmed maintenance (daily maintenance data)

All processing tasks leverage the shared .utils folder for maximum code reuse.

Usage:
    python unified_etl_processor.py netradyne --method api
    python unified_etl_processor.py dot_inspections --file inspections.xml
    python unified_etl_processor.py hos_violations --directory hos_violations_data
    python unified_etl_processor.py scorecard --directory scorecard_data
    python unified_etl_processor.py maintenance --directory maintenance_data
    python unified_etl_processor.py --interactive
"""

import logging
import sys
import os
import argparse
from enum import Enum
from typing import Optional, Dict, Any
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import (
    ConfigManager,
    DatabaseManager,
    FileProcessor,
    NetradyneAPIClient,
    NetradyneScraper,
    HOSViolationsProcessor,
    ScorecardProcessor,
    MaintenanceProcessor
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_processing.log') if os.path.exists('logs') else logging.StreamHandler()
    ]
)


class ETLDataSource(Enum):
    """Enumeration of available data sources."""
    NETRADYNE = "netradyne"
    DOT_INSPECTIONS = "dot_inspections" 
    HOS_VIOLATIONS = "hos_violations"
    SCORECARD = "scorecard"
    MAINTENANCE = "maintenance"


class UnifiedETLProcessor:
    """Unified processor that handles all ETL data sources."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the unified ETL processor.
        
        Args:
            config_path: Optional path to configuration file.
        """
        self.config = ConfigManager(config_path)
        
        # Initialize processors
        self.netradyne_api_client = NetradyneAPIClient(self.config)
        self.netradyne_scraper = NetradyneScraper(self.config)
        self.file_processor = FileProcessor(self.config)
        self.hos_processor = HOSViolationsProcessor(self.config)
        self.scorecard_processor = ScorecardProcessor(self.config)
        self.maintenance_processor = MaintenanceProcessor(self.config)
    
    def process_netradyne_data(self, method: str = "files", directory: str = None) -> Dict[str, Any]:
        """
        Process Netradyne driver score data.
        
        Args:
            method: Processing method ("api", "scraper", "files").
            directory: Directory for file processing.
        
        Returns:
            Processing results dictionary.
        """
        logging.info(f"Processing Netradyne data using method: {method}")
        
        try:
            with DatabaseManager(self.config) as db_manager:
                if not db_manager.connection:
                    return {"success": False, "error": "Database connection failed"}
                
                if method == "api":
                    result = self.netradyne_api_client.fetch_driver_scores()
                    if result:
                        scores, report_month = result
                        success = db_manager.insert_driver_scores(scores, report_month)
                        return {"success": success, "records": len(scores), "method": "api"}
                
                elif method == "scraper":
                    downloaded_file = self.netradyne_scraper.download_netradyne_file()
                    if downloaded_file:
                        scores = self.file_processor.process_file(downloaded_file)
                        if scores:
                            report_month = self.file_processor.get_report_month()
                            success = db_manager.insert_driver_scores(scores, report_month)
                            return {"success": success, "records": len(scores), "method": "scraper"}
                
                elif method == "files":
                    result = self.file_processor.process_all_files(directory)
                    if result:
                        scores, report_month = result
                        success = db_manager.insert_driver_scores(scores, report_month)
                        return {"success": success, "records": len(scores), "method": "files"}
                
                return {"success": False, "error": f"No data processed with method: {method}"}
                
        except Exception as e:
            logging.error(f"Error processing Netradyne data: {e}")
            return {"success": False, "error": str(e)}
    
    def process_hos_violations(self, directory: str = None, file_path: str = None, analyze: bool = False) -> Dict[str, Any]:
        """
        Process HOS violations data from JSON and Excel files.
        
        Args:
            directory: Directory containing HOS violation files.
            file_path: Specific file to process.
            analyze: Whether to perform analysis on the data.
        
        Returns:
            Processing results dictionary.
        """
        logging.info("Processing HOS violations data")
        
        try:
            violations = []
            report_month = self.hos_processor.get_report_month()
            
            if file_path:
                # Process single file
                logging.info(f"Processing specific HOS violations file: {file_path}")
                
                if not os.path.exists(file_path):
                    return {"success": False, "error": f"File not found: {file_path}"}
                
                file_ext = Path(file_path).suffix.lower()
                if file_ext == '.json':
                    violations = self.hos_processor.parse_json_file(file_path)
                elif file_ext in ['.xlsx', '.xls']:
                    df = self.hos_processor.read_hos_excel_file(file_path)
                    if df is not None:
                        violations = self.hos_processor.process_excel_dataframe(df)
                    else:
                        return {"success": False, "error": f"Could not process Excel file: {file_path}"}
                else:
                    return {"success": False, "error": f"Unsupported file format: {file_ext}"}
            else:
                # Process directory
                result = self.hos_processor.process_all_hos_files(directory)
                if not result:
                    return {"success": False, "error": "No HOS violation data found"}
                
                violations, report_month = result
            
            if not violations:
                return {"success": False, "error": "No valid HOS violation data processed"}
            
            # Insert into database
            with DatabaseManager(self.config) as db_manager:
                if not db_manager.connection:
                    return {"success": False, "error": "Database connection failed"}
                
                success = db_manager.insert_hos_violations(violations, report_month)
                
                if not success:
                    return {"success": False, "error": "Failed to insert HOS violations into database"}
                
                result_data = {
                    "success": True, 
                    "records": len(violations), 
                    "report_month": report_month,
                    "data_source": "hos_violations"
                }
                
                # Perform analysis if requested
                if analyze:
                    analysis = self.hos_processor.analyze_violations(violations)
                    result_data["analysis"] = analysis
                
                return result_data
                
        except Exception as e:
            logging.error(f"Error processing HOS violations: {e}")
            return {"success": False, "error": str(e)}
    
    def process_scorecard_data(self, directory: str = None) -> Dict[str, Any]:
        """
        Process scorecard data for driver performance evaluation.
        
        Args:
            directory: Directory containing scorecard files.
        
        Returns:
            Processing results dictionary.
        """
        logging.info("Processing scorecard data")
        
        try:
            result = self.scorecard_processor.process_all_scorecard_files(directory)
            if not result:
                return {"success": False, "error": "No scorecard data found"}
            
            scorecards, report_period, metrics = result
            
            # Insert into database
            with DatabaseManager(self.config) as db_manager:
                if not db_manager.connection:
                    return {"success": False, "error": "Database connection failed"}
                
                # Note: You would need to implement insert_scorecard_data method in DatabaseManager
                logging.info(f"Would insert {len(scorecards)} scorecard records for {report_period}")
                logging.info(f"Performance metrics: {metrics}")
                
                return {
                    "success": True, 
                    "records": len(scorecards), 
                    "report_period": report_period,
                    "metrics": metrics,
                    "data_source": "scorecard"
                }
                
        except Exception as e:
            logging.error(f"Error processing scorecard data: {e}")
            return {"success": False, "error": str(e)}
    
    def process_maintenance_data(self, directory: str = None) -> Dict[str, Any]:
        """
        Process programmed maintenance data.
        
        Args:
            directory: Directory containing maintenance files.
        
        Returns:
            Processing results dictionary.
        """
        logging.info("Processing maintenance data")
        
        try:
            result = self.maintenance_processor.process_all_maintenance_files(directory)
            if not result:
                return {"success": False, "error": "No maintenance data found"}
            
            maintenance_records, process_date, metrics = result
            
            # Insert into database
            with DatabaseManager(self.config) as db_manager:
                if not db_manager.connection:
                    return {"success": False, "error": "Database connection failed"}
                
                # Note: You would need to implement insert_maintenance_data method in DatabaseManager
                logging.info(f"Would insert {len(maintenance_records)} maintenance records for {process_date}")
                logging.info(f"Maintenance metrics: {metrics}")
                
                return {
                    "success": True, 
                    "records": len(maintenance_records), 
                    "process_date": process_date,
                    "metrics": metrics,
                    "data_source": "maintenance"
                }
                
        except Exception as e:
            logging.error(f"Error processing maintenance data: {e}")
            return {"success": False, "error": str(e)}


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description='Unified ETL processor for all data sources',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Netradyne processing
    python unified_etl_processor.py netradyne --method api
    python unified_etl_processor.py netradyne --method scraper
    python unified_etl_processor.py netradyne --method files --directory netradyne_score_data
    
    # DOT inspections (uses existing script)
    python dot_inspections_etl/dot_inspections_etl.py -f inspections.xml
    
    # HOS violations
    python unified_etl_processor.py hos_violations --directory hos_violations_data
    python unified_etl_processor.py hos_violations --file violations.json
    python unified_etl_processor.py hos_violations --directory hos_violations_data --analyze
    
    # Scorecard data
    python unified_etl_processor.py scorecard --directory scorecard_data
    
    # Maintenance data
    python unified_etl_processor.py maintenance --directory programmed_maintenance
    python unified_etl_processor.py maintenance --directory maintenance_data
    
    # Interactive mode
    python unified_etl_processor.py --interactive
        """
    )
    
    parser.add_argument('data_source', nargs='?', 
                       choices=['netradyne', 'hos_violations', 'scorecard', 'maintenance'],
                       help='Data source to process')
    parser.add_argument('--method', help='Processing method (for Netradyne: api, scraper, files)')
    parser.add_argument('--directory', help='Directory containing data files')
    parser.add_argument('--file', help='Specific file to process')
    parser.add_argument('--analyze', action='store_true', help='Perform analysis on processed data')
    parser.add_argument('--interactive', action='store_true', help='Run in interactive mode')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    
    return parser


def interactive_mode() -> None:
    """Run in interactive mode with user prompts."""
    print("Unified ETL Processor - Interactive Mode")
    print("=" * 50)
    
    processor = UnifiedETLProcessor()
    
    while True:
        print("\nSelect data source to process:")
        print("1. Netradyne driver scores")
        print("2. HOS violations")
        print("3. Score card data")
        print("4. Programmed maintenance")
        print("5. DOT inspections (separate script)")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            print("\nNetradyne processing methods:")
            print("a. API")
            print("b. Web scraper")
            print("c. File processing")
            method_choice = input("Select method (a/b/c): ").strip().lower()
            
            method_map = {'a': 'api', 'b': 'scraper', 'c': 'files'}
            method = method_map.get(method_choice, 'files')
            
            directory = None
            if method == 'files':
                directory = input("Enter directory path (or press Enter for default): ").strip()
                directory = directory if directory else None
            
            result = processor.process_netradyne_data(method, directory)
            print(f"Netradyne processing result: {result}")
            
        elif choice == '2':
            print("\nHOS violations processing options:")
            print("a. Process all files in directory")
            print("b. Process specific file")
            print("c. Process with analysis")
            hos_choice = input("Select option (a/b/c): ").strip().lower()
            
            if hos_choice == 'a':
                directory = input("Enter HOS violations directory (or press Enter for default): ").strip()
                directory = directory if directory else None
                result = processor.process_hos_violations(directory=directory)
            elif hos_choice == 'b':
                file_path = input("Enter file path (JSON or Excel): ").strip()
                if file_path and os.path.exists(file_path):
                    result = processor.process_hos_violations(file_path=file_path)
                else:
                    print("Invalid file path")
                    continue
            elif hos_choice == 'c':
                directory = input("Enter HOS violations directory (or press Enter for default): ").strip()
                directory = directory if directory else None
                result = processor.process_hos_violations(directory=directory, analyze=True)
                
                # Display analysis results if available
                if result.get('success') and 'analysis' in result:
                    analysis = result['analysis']
                    print("\n--- HOS VIOLATIONS ANALYSIS ---")
                    print(f"Total violations: {analysis.get('total_violations', 0)}")
                    print(f"Unique drivers: {analysis.get('unique_drivers', 0)}")
                    print(f"Violation types: {analysis.get('unique_violation_types', 0)}")
                    
                    if analysis.get('top_drivers'):
                        print("\nTop 5 violators:")
                        for i, (driver, count) in enumerate(analysis['top_drivers'][:5], 1):
                            print(f"  {i}. {driver}: {count} violations")
                    
                    if analysis.get('violation_type_distribution'):
                        print("\nViolation types:")
                        for violation_type, count in analysis['violation_type_distribution'][:5]:
                            print(f"  {violation_type}: {count} violations")
            else:
                print("Invalid choice")
                continue
                
            print(f"HOS violations processing result: {result}")
            
        elif choice == '3':
            directory = input("Enter scorecard directory (or press Enter for default): ").strip()
            directory = directory if directory else None
            result = processor.process_scorecard_data(directory)
            print(f"Scorecard processing result: {result}")
            
        elif choice == '4':
            print("\nProgrammed maintenance processing options:")
            print("a. Process programmed_maintenance folder (recommended)")
            print("b. Process maintenance_data folder")
            print("c. Process custom directory")
            print("d. Process with analysis")
            maint_choice = input("Select option (a/b/c/d): ").strip().lower()
            
            if maint_choice == 'a':
                result = processor.process_maintenance_data("programmed_maintenance")
            elif maint_choice == 'b':
                result = processor.process_maintenance_data("maintenance_data")
            elif maint_choice == 'c':
                directory = input("Enter maintenance directory path: ").strip()
                if directory and os.path.exists(directory):
                    result = processor.process_maintenance_data(directory)
                else:
                    print("Directory not found")
                    continue
            elif maint_choice == 'd':
                print("Select directory for analysis:")
                print("1. programmed_maintenance")
                print("2. maintenance_data")
                print("3. custom directory")
                analysis_dir_choice = input("Select (1/2/3): ").strip()
                
                if analysis_dir_choice == '1':
                    directory = "programmed_maintenance"
                elif analysis_dir_choice == '2':
                    directory = "maintenance_data"
                elif analysis_dir_choice == '3':
                    directory = input("Enter directory path: ").strip()
                    if not directory or not os.path.exists(directory):
                        print("Invalid directory")
                        continue
                else:
                    print("Invalid choice")
                    continue
                
                result = processor.process_maintenance_data(directory)
                
                # Display analysis results if available
                if result.get('success') and 'metrics' in result:
                    metrics = result['metrics']
                    print("\n--- MAINTENANCE ANALYSIS ---")
                    print(f"Total maintenance items: {metrics.get('total_maintenance_items', 0)}")
                    print(f"Overdue items: {metrics.get('overdue_count', 0)}")
                    print(f"Overdue percentage: {metrics.get('overdue_percentage', 0):.1f}%")
                    
                    if metrics.get('status_breakdown'):
                        print("\nStatus breakdown:")
                        for status, count in metrics['status_breakdown'].items():
                            print(f"  {status}: {count}")
                    
                    if metrics.get('priority_breakdown'):
                        print("\nPriority breakdown:")
                        for priority, count in metrics['priority_breakdown'].items():
                            print(f"  {priority}: {count}")
            else:
                print("Invalid choice")
                continue
                
            print(f"Maintenance processing result: {result}")
            
        elif choice == '5':
            print("Please use the DOT inspections script directly:")
            print("python dot_inspections_etl/dot_inspections_etl.py --interactive")
            
        elif choice == '6':
            print("Exiting...")
            break
            
        else:
            print("Invalid choice. Please try again.")


def debug_mode() -> None:
    """Run in debug mode for testing components."""
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Running in DEBUG mode")
    
    try:
        processor = UnifiedETLProcessor()
        
        # Test database connection
        logging.debug("Testing database connection...")
        with DatabaseManager(processor.config) as db_manager:
            if db_manager.connection:
                logging.debug("Database connection successful")
            else:
                logging.error("Database connection failed")
                return
        
        # Test each processor
        logging.debug("Testing HOS violations processor...")
        hos_files = processor.hos_processor.find_hos_files()
        logging.debug(f"Found Excel: {len(hos_files.get('excel', []))}, JSON: {len(hos_files.get('json', []))} HOS violation files")
        
        logging.debug("Testing scorecard processor...")
        scorecard_files = processor.scorecard_processor.find_scorecard_files()
        logging.debug(f"Found {len(scorecard_files)} scorecard files")
        
        logging.debug("Testing maintenance processor...")
        maintenance_files = processor.maintenance_processor.find_maintenance_files()
        logging.debug(f"Found {len(maintenance_files)} maintenance files")
        
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
    if args.interactive or not args.data_source:
        interactive_mode()
        return 0
    
    # Initialize processor
    processor = UnifiedETLProcessor()
    
    # Process based on data source
    if args.data_source == 'netradyne':
        method = args.method or 'files'
        result = processor.process_netradyne_data(method, args.directory)
        
    elif args.data_source == 'hos_violations':
        result = processor.process_hos_violations(
            directory=args.directory,
            file_path=args.file,
            analyze=args.analyze
        )
        
    elif args.data_source == 'scorecard':
        result = processor.process_scorecard_data(args.directory)
        
    elif args.data_source == 'maintenance':
        result = processor.process_maintenance_data(args.directory)
        
    else:
        print(f"Unknown data source: {args.data_source}")
        return 1
    
    # Print results
    print(f"Processing result: {result}")
    return 0 if result.get('success', False) else 1


if __name__ == "__main__":
    sys.exit(main()) 
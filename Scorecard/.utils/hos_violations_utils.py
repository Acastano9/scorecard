"""
HOS Violations processing utilities for Excel report and JSON data.
"""

import json
import logging
import pandas as pd
import datetime
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
from .config_utils import ConfigManager


@dataclass
class HOSViolation:
    """Data model representing a single HOS violation record."""
    
    id: str
    driver_id: str
    driver_name: str
    violation_start_time: datetime.datetime
    violation_end_time: Optional[datetime.datetime]
    driver_status: str
    terminal: str
    ruleset: str
    violation_type: str
    violation_duration: str
    start_time_and_driver: str
    
    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> 'HOSViolation':
        """Create a HOSViolation object from JSON data."""
        return cls(
            id=json_data["ID"],
            driver_id=json_data["Driver ID"],
            driver_name=json_data["Driver Name"],
            violation_start_time=datetime.datetime.fromisoformat(json_data["Violation Start Time"]),
            violation_end_time=datetime.datetime.fromisoformat(json_data["Violation End Time"]) if json_data.get("Violation End Time") else None,
            driver_status=json_data["Driver Status"],
            terminal=json_data["Terminal"],
            ruleset=json_data["Ruleset"],
            violation_type=json_data["Violation Type"],
            violation_duration=json_data["Violation Duration (HH:MM:SS)"],
            start_time_and_driver=json_data["Start Time and Driver"]
        )
    
    @classmethod
    def from_excel_row(cls, row_data: Dict[str, Any]) -> 'HOSViolation':
        """Create a HOSViolation object from Excel row data."""
        # Generate an ID if not present
        violation_id = row_data.get('id', f"{row_data.get('driver_id', '')}_{row_data.get('violation_date', '')}")
        
        # Parse date if it's a string
        violation_date = row_data.get('violation_date', '')
        if isinstance(violation_date, str):
            try:
                violation_start_time = datetime.datetime.strptime(violation_date, '%Y-%m-%d')
            except:
                violation_start_time = datetime.datetime.now()
        else:
            violation_start_time = violation_date or datetime.datetime.now()
        
        return cls(
            id=violation_id,
            driver_id=row_data.get('driver_id', ''),
            driver_name=row_data.get('driver_name', ''),
            violation_start_time=violation_start_time,
            violation_end_time=None,  # Excel format typically doesn't have end time
            driver_status=row_data.get('driver_status', ''),
            terminal=row_data.get('terminal', ''),
            ruleset=row_data.get('ruleset', ''),
            violation_type=row_data.get('violation_type', ''),
            violation_duration=row_data.get('violation_duration', ''),
            start_time_and_driver=f"{violation_start_time} - {row_data.get('driver_name', '')}"
        )
    
    def as_tuple(self) -> Tuple:
        """Convert to a tuple suitable for SQL insertion."""
        return (
            self.id,
            self.start_time_and_driver,
            self.driver_id,
            self.driver_name,
            self.violation_start_time,
            self.violation_end_time,
            self.driver_status,
            self.terminal,
            self.ruleset,
            self.violation_type,
            self.violation_duration
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            'id': self.id,
            'driver_id': self.driver_id,
            'driver_name': self.driver_name,
            'violation_start_time': self.violation_start_time,
            'violation_end_time': self.violation_end_time,
            'driver_status': self.driver_status,
            'terminal': self.terminal,
            'ruleset': self.ruleset,
            'violation_type': self.violation_type,
            'violation_duration': self.violation_duration,
            'start_time_and_driver': self.start_time_and_driver
        }


class HOSViolationsProcessor:
    """Processes HOS violations data from Excel reports and JSON files."""
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize HOS violations processor.
        
        Args:
            config_manager: Configuration manager instance.
        """
        self.config_manager = config_manager
        
    def find_hos_files(self, directory: str = "hos_violations_data") -> Dict[str, List[str]]:
        """
        Find HOS violation files in the specified directory.
        
        Args:
            directory: Directory to search for files.
        
        Returns:
            Dictionary with file types as keys and file paths as values.
        """
        directory_path = Path(directory)
        
        if not directory_path.exists():
            logging.warning(f"Directory does not exist: {directory}")
            return {"excel": [], "json": []}
        
        # Look for Excel and JSON files
        excel_files = list(directory_path.glob('*.xlsx'))
        excel_files.extend(list(directory_path.glob('*.xls')))
        json_files = list(directory_path.glob('*.json'))
        
        excel_paths = [str(file_path) for file_path in excel_files]
        json_paths = [str(file_path) for file_path in json_files]
        
        logging.info(f"Found {len(excel_paths)} Excel files and {len(json_paths)} JSON files in {directory}")
        
        return {"excel": excel_paths, "json": json_paths}
    
    def parse_json_file(self, file_path: str) -> List[HOSViolation]:
        """
        Parse JSON file and return list of HOSViolation objects.
        
        Args:
            file_path: Path to JSON file.
        
        Returns:
            List of HOSViolation objects.
        """
        try:
            logging.info(f"Parsing JSON file: {file_path}")
            
            if not Path(file_path).exists():
                logging.error(f"JSON file not found: {file_path}")
                return []
            
            with open(file_path, 'r') as f:
                json_data = json.load(f)
            
            violations = []
            for item in json_data:
                try:
                    violation = HOSViolation.from_json(item)
                    violations.append(violation)
                except (KeyError, ValueError) as e:
                    logging.warning(f"Error parsing violation: {e}")
                    continue
            
            logging.info(f"Successfully parsed {len(violations)} violations from JSON")
            return violations
            
        except Exception as e:
            logging.error(f"Error reading JSON file {file_path}: {e}")
            return []
    
    def read_hos_excel_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Read and process HOS violations Excel file.
        
        Args:
            file_path: Path to Excel file.
        
        Returns:
            Processed DataFrame or None if failed.
        """
        try:
            logging.info(f"Reading HOS violations Excel file: {file_path}")
            
            # Try reading different sheets and formats
            try:
                df = pd.read_excel(file_path)
            except:
                df = pd.read_excel(file_path, sheet_name=0)
            
            # HOS violations typically have these columns (adjust as needed)
            column_mapping = {
                'Driver ID': ['Driver ID', 'driver_id', 'DriverID', 'ID', 'Employee_ID'],
                'Driver Name': ['Driver Name', 'driver_name', 'DriverName', 'Name', 'Employee_Name'],
                'Violation Date': ['Violation Date', 'violation_date', 'ViolationDate', 'Date', 'Violation Start Time'],
                'Violation Type': ['Violation Type', 'violation_type', 'ViolationType', 'Type'],
                'Description': ['Description', 'description', 'Desc', 'Details', 'Violation Duration (HH:MM:SS)'],
                'Severity': ['Severity', 'severity', 'Level', 'Priority'],
                'Terminal': ['Terminal', 'terminal', 'Location', 'Base'],
                'Ruleset': ['Ruleset', 'ruleset', 'Rules', 'Rule_Set'],
                'Driver Status': ['Driver Status', 'driver_status', 'Status']
            }
            
            # Try to find and rename columns
            for target_col, possible_names in column_mapping.items():
                for possible_name in possible_names:
                    if possible_name in df.columns:
                        df = df.rename(columns={possible_name: target_col})
                        break
            
            # Check for required columns
            required_cols = ['Driver ID', 'Violation Date', 'Violation Type']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logging.error(f"Missing required columns: {missing_cols}")
                logging.info(f"Available columns: {list(df.columns)}")
                return None
            
            # Clean column names
            df.columns = [c.replace(' ', '_').lower() for c in df.columns]
            
            # Fill NaN values
            df = df.fillna('')
            
            logging.info(f"Successfully processed HOS Excel file. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logging.error(f"Error reading HOS Excel file {file_path}: {e}")
            return None
    
    def process_excel_dataframe(self, df: pd.DataFrame) -> List[HOSViolation]:
        """
        Convert Excel DataFrame to HOSViolation objects.
        
        Args:
            df: DataFrame containing HOS violations data.
        
        Returns:
            List of HOSViolation objects.
        """
        violations = []
        
        for index, row in df.iterrows():
            try:
                # Map Excel row to violation data
                violation_data = {
                    'id': f"{row.get('driver_id', '')}_{row.get('violation_date', '')}_{index}",
                    'driver_id': str(row.get('driver_id', '')).strip(),
                    'driver_name': str(row.get('driver_name', '')).strip(),
                    'violation_date': row.get('violation_date', ''),
                    'violation_type': str(row.get('violation_type', '')).strip(),
                    'description': str(row.get('description', '')).strip(),
                    'severity': str(row.get('severity', '')).strip(),
                    'terminal': str(row.get('terminal', '')).strip(),
                    'ruleset': str(row.get('ruleset', '')).strip(),
                    'driver_status': str(row.get('driver_status', '')).strip(),
                    'violation_duration': str(row.get('description', '')).strip()  # Use description as duration for Excel
                }
                
                # Validate required fields
                if violation_data['driver_id'] and violation_data['violation_date'] and violation_data['violation_type']:
                    violation = HOSViolation.from_excel_row(violation_data)
                    violations.append(violation)
                else:
                    logging.warning(f"Skipping row {index} due to missing required data")
                    
            except Exception as e:
                logging.warning(f"Skipping row {index} due to processing error: {e}")
                continue
        
        logging.info(f"Processed {len(violations)} valid HOS violations from Excel")
        return violations
    
    def get_report_month(self) -> str:
        """
        Calculate the report month for the previous month.
        
        Returns:
            Report month string in YYYY-MM-DD format.
        """
        today = datetime.datetime.now()
        first_day_last_month = datetime.date(today.year, today.month, 1) - datetime.timedelta(days=1)
        report_date = datetime.date(first_day_last_month.year, first_day_last_month.month, 1)
        return report_date.strftime('%Y-%m-%d')
    
    def process_all_hos_files(self, directory: str = None) -> Optional[Tuple[List[HOSViolation], str]]:
        """
        Process all HOS violation files in directory (both Excel and JSON).
        
        Args:
            directory: Directory containing files. If None, uses default.
        
        Returns:
            Tuple of (violations_list, report_month) or None if failed.
        """
        if directory is None:
            directory = "hos_violations_data"
        
        files = self.find_hos_files(directory)
        
        if not files["excel"] and not files["json"]:
            logging.warning("No HOS violation files found to process")
            return None
        
        all_violations = []
        report_month = self.get_report_month()
        
        # Process JSON files
        for file_path in files["json"]:
            try:
                file_name = Path(file_path).name
                logging.info(f"Processing HOS JSON file: {file_name}")
                
                violations = self.parse_json_file(file_path)
                all_violations.extend(violations)
                logging.info(f"Added {len(violations)} violations from JSON file {file_name}")
                
            except Exception as e:
                logging.error(f"Error processing JSON file {file_path}: {e}")
                continue
        
        # Process Excel files
        for file_path in files["excel"]:
            try:
                file_name = Path(file_path).name
                logging.info(f"Processing HOS Excel file: {file_name}")
                
                df = self.read_hos_excel_file(file_path)
                if df is not None:
                    violations = self.process_excel_dataframe(df)
                    all_violations.extend(violations)
                    logging.info(f"Added {len(violations)} violations from Excel file {file_name}")
                else:
                    logging.warning(f"No valid data found in Excel file {file_name}")
                    
            except Exception as e:
                logging.error(f"Error processing Excel file {file_path}: {e}")
                continue
        
        if all_violations:
            logging.info(f"Total HOS violations processed: {len(all_violations)}")
            return all_violations, report_month
        else:
            logging.error("No valid HOS violation data found in any files")
            return None
    
    def analyze_violations(self, violations: List[HOSViolation]) -> Dict[str, Any]:
        """
        Analyze violations and provide statistics.
        
        Args:
            violations: List of HOSViolation objects.
        
        Returns:
            Dictionary with analysis results.
        """
        if not violations:
            return {}
        
        # Group by driver
        driver_counts = {}
        violation_type_counts = {}
        terminal_counts = {}
        
        for violation in violations:
            # Count by driver
            driver_key = f"{violation.driver_name} ({violation.driver_id})"
            driver_counts[driver_key] = driver_counts.get(driver_key, 0) + 1
            
            # Count by violation type
            violation_type_counts[violation.violation_type] = violation_type_counts.get(violation.violation_type, 0) + 1
            
            # Count by terminal
            terminal_counts[violation.terminal] = terminal_counts.get(violation.terminal, 0) + 1
        
        # Get top violators
        top_drivers = sorted(driver_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        top_violation_types = sorted(violation_type_counts.items(), key=lambda x: x[1], reverse=True)
        top_terminals = sorted(terminal_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        analysis = {
            'total_violations': len(violations),
            'unique_drivers': len(driver_counts),
            'unique_violation_types': len(violation_type_counts),
            'unique_terminals': len(terminal_counts),
            'top_drivers': top_drivers,
            'violation_type_distribution': top_violation_types,
            'top_terminals': top_terminals,
            'date_range': {
                'earliest': min(v.violation_start_time for v in violations).isoformat(),
                'latest': max(v.violation_start_time for v in violations).isoformat()
            }
        }
        
        logging.info(f"Analysis complete: {analysis['total_violations']} violations, {analysis['unique_drivers']} drivers")
        return analysis 
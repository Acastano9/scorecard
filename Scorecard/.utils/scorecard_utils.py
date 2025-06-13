"""
Score Card processing utilities for driver performance evaluation.
"""

import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from .config_utils import ConfigManager


class ScorecardProcessor:
    """Processes driver performance scorecard data."""
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize scorecard processor.
        
        Args:
            config_manager: Configuration manager instance.
        """
        self.config_manager = config_manager
        
    def find_scorecard_files(self, directory: str = "scorecard_data") -> List[str]:
        """
        Find scorecard files in the specified directory.
        
        Args:
            directory: Directory to search for files.
        
        Returns:
            List of file paths found.
        """
        directory_path = Path(directory)
        
        if not directory_path.exists():
            logging.warning(f"Directory does not exist: {directory}")
            return []
        
        # Look for Excel and CSV files
        files = list(directory_path.glob('*.xlsx'))
        files.extend(list(directory_path.glob('*.xls')))
        files.extend(list(directory_path.glob('*.csv')))
        
        file_paths = [str(file_path) for file_path in files]
        
        logging.info(f"Found {len(file_paths)} scorecard files in {directory}")
        for file_path in file_paths:
            logging.debug(f"  - {Path(file_path).name}")
        
        return file_paths
    
    def read_scorecard_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Read and process scorecard file (Excel or CSV).
        
        Args:
            file_path: Path to file.
        
        Returns:
            Processed DataFrame or None if failed.
        """
        try:
            logging.info(f"Reading scorecard file: {file_path}")
            
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext == '.csv':
                df = pd.read_csv(file_path)
            elif file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                logging.error(f"Unsupported file format: {file_ext}")
                return None
            
            # Scorecard typically has these columns (adjust as needed)
            column_mapping = {
                'Driver ID': ['Driver ID', 'driver_id', 'DriverID', 'ID', 'Employee_ID'],
                'Driver Name': ['Driver Name', 'driver_name', 'DriverName', 'Name', 'Employee_Name'],
                'Period': ['Period', 'period', 'Report_Period', 'Month', 'Date'],
                'Safety Score': ['Safety Score', 'safety_score', 'SafetyScore', 'Safety'],
                'HOS Score': ['HOS Score', 'hos_score', 'HOSScore', 'HOS'],
                'Vehicle Score': ['Vehicle Score', 'vehicle_score', 'VehicleScore', 'Vehicle'],
                'Overall Score': ['Overall Score', 'overall_score', 'OverallScore', 'Total', 'Final_Score'],
                'Miles Driven': ['Miles Driven', 'miles_driven', 'MilesDriven', 'Miles', 'Total_Miles'],
                'Incidents': ['Incidents', 'incidents', 'Incident_Count', 'Total_Incidents'],
                'Violations': ['Violations', 'violations', 'Violation_Count', 'Total_Violations']
            }
            
            # Try to find and rename columns
            for target_col, possible_names in column_mapping.items():
                for possible_name in possible_names:
                    if possible_name in df.columns:
                        df = df.rename(columns={possible_name: target_col})
                        break
            
            # Check for required columns
            required_cols = ['Driver ID', 'Period', 'Overall Score']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logging.error(f"Missing required columns: {missing_cols}")
                logging.info(f"Available columns: {list(df.columns)}")
                return None
            
            # Clean column names
            df.columns = [c.replace(' ', '_').lower() for c in df.columns]
            
            # Fill NaN values
            df = df.fillna(0)
            
            logging.info(f"Successfully processed scorecard file. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logging.error(f"Error reading scorecard file {file_path}: {e}")
            return None
    
    def process_scorecard_dataframe(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convert DataFrame to standardized scorecard format.
        
        Args:
            df: DataFrame containing scorecard data.
        
        Returns:
            List of scorecard dictionaries.
        """
        scorecards = []
        
        for index, row in df.iterrows():
            try:
                scorecard_data = {
                    'driver_id': str(row.get('driver_id', '')).strip(),
                    'driver_name': str(row.get('driver_name', '')).strip(),
                    'period': str(row.get('period', '')).strip(),
                    'safety_score': float(row.get('safety_score', 0)),
                    'hos_score': float(row.get('hos_score', 0)),
                    'vehicle_score': float(row.get('vehicle_score', 0)),
                    'overall_score': float(row.get('overall_score', 0)),
                    'miles_driven': float(row.get('miles_driven', 0)),
                    'incidents': int(row.get('incidents', 0)),
                    'violations': int(row.get('violations', 0))
                }
                
                # Validate required fields
                if scorecard_data['driver_id'] and scorecard_data['period']:
                    scorecards.append(scorecard_data)
                else:
                    logging.warning(f"Skipping row {index} due to missing required data")
                    
            except (ValueError, TypeError) as e:
                logging.warning(f"Skipping row {index} due to data conversion error: {e}")
                continue
            except Exception as e:
                logging.warning(f"Skipping row {index} due to processing error: {e}")
                continue
        
        logging.info(f"Processed {len(scorecards)} valid scorecard records")
        return scorecards
    
    def calculate_performance_metrics(self, scorecards: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate performance metrics from scorecard data.
        
        Args:
            scorecards: List of scorecard dictionaries.
        
        Returns:
            Dictionary with performance metrics.
        """
        if not scorecards:
            return {}
        
        import statistics
        
        # Extract numeric scores
        overall_scores = [s['overall_score'] for s in scorecards if s['overall_score'] > 0]
        safety_scores = [s['safety_score'] for s in scorecards if s['safety_score'] > 0]
        total_miles = sum(s['miles_driven'] for s in scorecards)
        total_incidents = sum(s['incidents'] for s in scorecards)
        total_violations = sum(s['violations'] for s in scorecards)
        
        metrics = {
            'total_drivers': len(scorecards),
            'average_overall_score': statistics.mean(overall_scores) if overall_scores else 0,
            'average_safety_score': statistics.mean(safety_scores) if safety_scores else 0,
            'total_miles_driven': total_miles,
            'total_incidents': total_incidents,
            'total_violations': total_violations,
            'incidents_per_mile': (total_incidents / total_miles * 1000000) if total_miles > 0 else 0,  # Per million miles
            'violations_per_driver': total_violations / len(scorecards) if scorecards else 0
        }
        
        logging.info(f"Calculated performance metrics: {metrics}")
        return metrics
    
    def get_report_period(self) -> str:
        """
        Calculate the report period for the current month.
        
        Returns:
            Report period string in YYYY-MM format.
        """
        import datetime
        today = datetime.datetime.now()
        return today.strftime('%Y-%m')
    
    def process_all_scorecard_files(self, directory: str = None) -> Optional[Tuple[List[Dict[str, Any]], str, Dict[str, Any]]]:
        """
        Process all scorecard files in directory.
        
        Args:
            directory: Directory containing files. If None, uses default.
        
        Returns:
            Tuple of (scorecards_list, report_period, metrics) or None if failed.
        """
        if directory is None:
            directory = "scorecard_data"
        
        files = self.find_scorecard_files(directory)
        
        if not files:
            logging.warning("No scorecard files found to process")
            return None
        
        all_scorecards = []
        report_period = self.get_report_period()
        
        for file_path in files:
            try:
                file_name = Path(file_path).name
                logging.info(f"Processing scorecard file: {file_name}")
                
                df = self.read_scorecard_file(file_path)
                if df is not None:
                    scorecards = self.process_scorecard_dataframe(df)
                    all_scorecards.extend(scorecards)
                    logging.info(f"Added {len(scorecards)} scorecard records from {file_name}")
                else:
                    logging.warning(f"No valid data found in {file_name}")
                    
            except Exception as e:
                logging.error(f"Error processing scorecard file {file_path}: {e}")
                continue
        
        if all_scorecards:
            metrics = self.calculate_performance_metrics(all_scorecards)
            logging.info(f"Total scorecard records processed: {len(all_scorecards)}")
            return all_scorecards, report_period, metrics
        else:
            logging.error("No valid scorecard data found in any files")
            return None 
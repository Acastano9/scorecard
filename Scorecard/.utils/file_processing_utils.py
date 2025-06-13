"""
File processing utilities for Netradyne data files.
"""

import os
import glob
import datetime
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from .config_utils import ConfigManager


class FileProcessor:
    """Processes Netradyne data files (CSV and Excel)."""
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize file processor.
        
        Args:
            config_manager: Configuration manager instance.
        """
        self.config_manager = config_manager
        self.file_paths = config_manager.file_paths
    
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
    
    def find_data_files(self, directory: str = None) -> List[str]:
        """
        Find Netradyne data files in the specified directory.
        
        Args:
            directory: Directory to search. If None, uses default from config.
        
        Returns:
            List of file paths found.
        """
        if directory is None:
            directory = self.file_paths['netradyne_score_data']
        
        if not os.path.exists(directory):
            logging.warning(f"Directory does not exist: {directory}")
            return []
        
        # Look for both CSV and Excel files
        csv_files = glob.glob(os.path.join(directory, '*.csv'))
        excel_files = glob.glob(os.path.join(directory, '*.xlsx'))
        excel_files.extend(glob.glob(os.path.join(directory, '*.xls')))
        
        all_files = csv_files + excel_files
        
        logging.info(f"Found {len(all_files)} data files in {directory}")
        for file_path in all_files:
            logging.info(f"  - {os.path.basename(file_path)}")
        
        return all_files
    
    def read_csv_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Read and process a CSV file.
        
        Args:
            file_path: Path to the CSV file.
        
        Returns:
            Processed DataFrame or None if failed.
        """
        try:
            logging.info(f"Reading CSV file: {file_path}")
            
            # Try reading with skiprows first (original format)
            try:
                df = pd.read_csv(
                    file_path, 
                    skiprows=10, 
                    usecols=['Driver ID', 'Minutes Analyzed', 'Driver Score']
                )
            except:
                # If that fails, try reading without skiprows
                df = pd.read_csv(file_path)
                
                # Look for columns that match our expected data
                column_mapping = {
                    'Driver ID': ['Driver ID', 'driver_id', 'DriverID', 'ID'],
                    'Minutes Analyzed': ['Minutes Analyzed', 'minutes_analyzed', 'MinutesAnalyzed', 'Minutes'],
                    'Driver Score': ['Driver Score', 'driver_score', 'DriverScore', 'Score']
                }
                
                # Try to find and rename columns
                for target_col, possible_names in column_mapping.items():
                    for possible_name in possible_names:
                        if possible_name in df.columns:
                            df = df.rename(columns={possible_name: target_col})
                            break
                
                # Select only the columns we need
                required_cols = ['Driver ID', 'Minutes Analyzed', 'Driver Score']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    logging.error(f"Missing required columns: {missing_cols}")
                    return None
                
                df = df[required_cols]
            
            # Clean column names
            df.columns = [c.replace(' ', '_') for c in df.columns]
            
            # Fill NaN values
            df = df.fillna(0)
            
            logging.info(f"Successfully processed CSV file. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logging.error(f"Error reading CSV file {file_path}: {e}")
            return None
    
    def read_excel_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Read and process an Excel file.
        
        Args:
            file_path: Path to the Excel file.
        
        Returns:
            Processed DataFrame or None if failed.
        """
        try:
            logging.info(f"Reading Excel file: {file_path}")
            
            # Try reading different sheets and formats
            try:
                # First try default sheet
                df = pd.read_excel(file_path)
            except:
                # If that fails, try the first sheet explicitly
                df = pd.read_excel(file_path, sheet_name=0)
            
            # Look for columns that match our expected data
            column_mapping = {
                'Driver ID': ['Driver ID', 'driver_id', 'DriverID', 'ID'],
                'Minutes Analyzed': ['Minutes Analyzed', 'minutes_analyzed', 'MinutesAnalyzed', 'Minutes'],
                'Driver Score': ['Driver Score', 'driver_score', 'DriverScore', 'Score']
            }
            
            # Try to find and rename columns
            for target_col, possible_names in column_mapping.items():
                for possible_name in possible_names:
                    if possible_name in df.columns:
                        df = df.rename(columns={possible_name: target_col})
                        break
            
            # Select only the columns we need
            required_cols = ['Driver ID', 'Minutes Analyzed', 'Driver Score']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logging.error(f"Missing required columns: {missing_cols}")
                return None
            
            df = df[required_cols]
            
            # Clean column names
            df.columns = [c.replace(' ', '_') for c in df.columns]
            
            # Fill NaN values
            df = df.fillna(0)
            
            logging.info(f"Successfully processed Excel file. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logging.error(f"Error reading Excel file {file_path}: {e}")
            return None
    
    def process_dataframe_to_scores(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convert DataFrame to standardized score format.
        
        Args:
            df: DataFrame containing driver score data.
        
        Returns:
            List of score dictionaries.
        """
        scores = []
        
        for index, row in df.iterrows():
            try:
                driver_id = str(row['Driver_ID']).strip()
                minutes_analyzed = int(float(row['Minutes_Analyzed']))
                driver_score = int(float(row['Driver_Score']))
                
                if driver_id and driver_id != 'nan':
                    scores.append({
                        'driver_id': driver_id,
                        'minutes_analyzed': minutes_analyzed,
                        'driver_score': driver_score
                    })
                else:
                    logging.warning(f"Skipping row {index} due to missing driver ID")
                    
            except (ValueError, KeyError) as e:
                logging.warning(f"Skipping row {index} due to data error: {e}")
                continue
        
        logging.info(f"Converted {len(scores)} valid records from DataFrame")
        return scores
    
    def process_file(self, file_path: str) -> Optional[List[Dict[str, Any]]]:
        """
        Process a single file and return standardized score data.
        
        Args:
            file_path: Path to the file to process.
        
        Returns:
            List of score dictionaries or None if failed.
        """
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.csv':
            df = self.read_csv_file(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            df = self.read_excel_file(file_path)
        else:
            logging.error(f"Unsupported file format: {file_ext}")
            return None
        
        if df is None:
            return None
        
        scores = self.process_dataframe_to_scores(df)
        return scores
    
    def process_all_files(self, directory: str = None) -> Optional[Tuple[List[Dict[str, Any]], str]]:
        """
        Process all files in the directory and return combined results.
        
        Args:
            directory: Directory containing files. If None, uses default from config.
        
        Returns:
            Tuple of (combined_scores, report_month) or None if failed.
        """
        files = self.find_data_files(directory)
        
        if not files:
            logging.warning("No data files found to process")
            return None
        
        all_scores = []
        report_month = self.get_report_month()
        
        for file_path in files:
            try:
                file_name = os.path.basename(file_path)
                logging.info(f"Processing file: {file_name}")
                
                scores = self.process_file(file_path)
                if scores:
                    all_scores.extend(scores)
                    logging.info(f"Added {len(scores)} records from {file_name}")
                else:
                    logging.warning(f"No valid data found in {file_name}")
                    
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")
                continue
        
        if all_scores:
            logging.info(f"Total processed records: {len(all_scores)}")
            return all_scores, report_month
        else:
            logging.error("No valid data found in any files")
            return None 
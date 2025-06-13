"""
File management utilities for DOT inspections ETL.
"""

import os
import logging
import shutil
from pathlib import Path
from typing import List, Optional, Dict, Any
from .config_utils import DOTConfigManager


class FileManager:
    """Handles file operations and directory management for DOT inspections ETL."""
    
    def __init__(self, config_manager: DOTConfigManager):
        """
        Initialize file manager.
        
        Args:
            config_manager: Configuration manager instance.
        """
        self.config_manager = config_manager
        self.file_paths = config_manager.file_paths
        self.processing_config = config_manager.processing_config
        
        # Ensure directories exist
        self._create_directories()
    
    def _create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        directories = [
            self.file_paths['xml_data_dir'],
            self.file_paths['processed_dir'],
            self.file_paths['error_dir'],
            self.file_paths['logs_dir']
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logging.debug(f"Ensured directory exists: {directory}")
    
    def find_xml_files(self, directory: str = None) -> List[str]:
        """
        Find XML files in the specified directory.
        
        Args:
            directory: Directory to search. If None, uses default from config.
        
        Returns:
            List of XML file paths.
        """
        if directory is None:
            directory = self.file_paths['xml_data_dir']
        
        directory_path = Path(directory)
        
        if not directory_path.exists():
            logging.warning(f"Directory does not exist: {directory}")
            return []
        
        if not directory_path.is_dir():
            logging.warning(f"Path is not a directory: {directory}")
            return []
        
        # Find all XML files
        xml_files = list(directory_path.glob('*.xml'))
        xml_file_paths = [str(file_path) for file_path in xml_files]
        
        logging.info(f"Found {len(xml_file_paths)} XML files in {directory}")
        for file_path in xml_file_paths:
            logging.debug(f"  - {Path(file_path).name}")
        
        return xml_file_paths
    
    def validate_file_path(self, file_path: str) -> bool:
        """
        Validate that a file path exists and is an XML file.
        
        Args:
            file_path: Path to file to validate.
        
        Returns:
            Boolean indicating if file is valid.
        """
        file_path_obj = Path(file_path)
        
        if not file_path_obj.exists():
            logging.error(f"File does not exist: {file_path}")
            return False
        
        if not file_path_obj.is_file():
            logging.error(f"Path is not a file: {file_path}")
            return False
        
        if file_path_obj.suffix.lower() != '.xml':
            logging.error(f"File is not an XML file: {file_path}")
            return False
        
        return True
    
    def find_similar_files(self, file_path: str) -> List[str]:
        """
        Find files with similar names in the same directory.
        
        Args:
            file_path: Original file path to find similar files for.
        
        Returns:
            List of similar file paths.
        """
        file_path_obj = Path(file_path)
        directory = file_path_obj.parent
        filename_stem = file_path_obj.stem
        
        # Try common variations
        variations = [
            filename_stem.replace('inspections', 'inspection'),
            filename_stem.replace('inspection', 'inspections'),
            filename_stem.lower(),
            filename_stem.upper()
        ]
        
        similar_files = []
        
        for variation in variations:
            if variation != filename_stem:  # Don't include the original
                pattern = f"*{variation}*"
                matches = list(directory.glob(pattern))
                for match in matches:
                    if match.suffix.lower() == '.xml':
                        similar_files.append(str(match))
        
        # Remove duplicates and sort
        similar_files = sorted(list(set(similar_files)))
        
        if similar_files:
            logging.info(f"Found {len(similar_files)} similar files:")
            for similar_file in similar_files:
                logging.info(f"  - {Path(similar_file).name}")
        
        return similar_files
    
    def move_processed_file(self, file_path: str) -> Optional[str]:
        """
        Move a processed file to the processed directory.
        
        Args:
            file_path: Path to file to move.
        
        Returns:
            New file path or None if move failed.
        """
        if not self.processing_config.get('backup_processed_files', True):
            logging.debug("File backup is disabled, skipping move")
            return None
        
        try:
            source_path = Path(file_path)
            if not source_path.exists():
                logging.warning(f"Source file does not exist: {file_path}")
                return None
            
            # Create destination path with timestamp
            import datetime
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{source_path.stem}_{timestamp}{source_path.suffix}"
            destination_path = Path(self.file_paths['processed_dir']) / filename
            
            # Move the file
            shutil.move(str(source_path), str(destination_path))
            logging.info(f"Moved processed file: {source_path.name} -> {destination_path}")
            
            return str(destination_path)
            
        except Exception as e:
            logging.error(f"Failed to move processed file {file_path}: {e}")
            return None
    
    def move_error_file(self, file_path: str, error_message: str = None) -> Optional[str]:
        """
        Move a file that caused errors to the error directory.
        
        Args:
            file_path: Path to file to move.
            error_message: Optional error message to include in filename.
        
        Returns:
            New file path or None if move failed.
        """
        try:
            source_path = Path(file_path)
            if not source_path.exists():
                logging.warning(f"Source file does not exist: {file_path}")
                return None
            
            # Create destination path with timestamp and error indicator
            import datetime
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{source_path.stem}_ERROR_{timestamp}{source_path.suffix}"
            destination_path = Path(self.file_paths['error_dir']) / filename
            
            # Move the file
            shutil.move(str(source_path), str(destination_path))
            logging.warning(f"Moved error file: {source_path.name} -> {destination_path}")
            
            # Create error log file if error message provided
            if error_message:
                error_log_path = destination_path.with_suffix('.error.log')
                try:
                    with open(error_log_path, 'w') as f:
                        f.write(f"Error processing file: {source_path.name}\n")
                        f.write(f"Timestamp: {timestamp}\n")
                        f.write(f"Error message: {error_message}\n")
                    logging.info(f"Created error log: {error_log_path}")
                except Exception as log_error:
                    logging.error(f"Failed to create error log: {log_error}")
            
            return str(destination_path)
            
        except Exception as e:
            logging.error(f"Failed to move error file {file_path}: {e}")
            return None
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        Get information about a file.
        
        Args:
            file_path: Path to file.
        
        Returns:
            Dictionary with file information.
        """
        file_path_obj = Path(file_path)
        
        info = {
            'filename': file_path_obj.name,
            'directory': str(file_path_obj.parent),
            'size_bytes': 0,
            'modified_time': None,
            'exists': False,
            'is_xml': False
        }
        
        try:
            if file_path_obj.exists():
                info['exists'] = True
                info['size_bytes'] = file_path_obj.stat().st_size
                info['modified_time'] = file_path_obj.stat().st_mtime
                info['is_xml'] = file_path_obj.suffix.lower() == '.xml'
                
                # Convert size to human readable format
                size_mb = info['size_bytes'] / (1024 * 1024)
                info['size_mb'] = round(size_mb, 2)
                
                # Convert timestamp to readable format
                import datetime
                info['modified_time_str'] = datetime.datetime.fromtimestamp(
                    info['modified_time']
                ).strftime('%Y-%m-%d %H:%M:%S')
                
        except Exception as e:
            logging.error(f"Error getting file info for {file_path}: {e}")
        
        return info
    
    def cleanup_old_files(self, directory: str = None, days_old: int = 30) -> int:
        """
        Clean up old files in specified directory.
        
        Args:
            directory: Directory to clean. If None, uses processed directory.
            days_old: Number of days old files must be to be deleted.
        
        Returns:
            Number of files deleted.
        """
        if directory is None:
            directory = self.file_paths['processed_dir']
        
        directory_path = Path(directory)
        if not directory_path.exists():
            logging.warning(f"Cleanup directory does not exist: {directory}")
            return 0
        
        import datetime
        cutoff_time = datetime.datetime.now() - datetime.timedelta(days=days_old)
        cutoff_timestamp = cutoff_time.timestamp()
        
        deleted_count = 0
        
        try:
            for file_path in directory_path.iterdir():
                if file_path.is_file():
                    file_modified_time = file_path.stat().st_mtime
                    
                    if file_modified_time < cutoff_timestamp:
                        try:
                            file_path.unlink()
                            logging.debug(f"Deleted old file: {file_path.name}")
                            deleted_count += 1
                        except Exception as e:
                            logging.error(f"Failed to delete file {file_path}: {e}")
            
            if deleted_count > 0:
                logging.info(f"Cleaned up {deleted_count} old files from {directory}")
            
        except Exception as e:
            logging.error(f"Error during cleanup of {directory}: {e}")
        
        return deleted_count 
"""
Configuration management utilities for DOT inspections ETL.
"""

import os
from dotenv import load_dotenv
from typing import Optional


class DOTConfigManager:
    """Manages configuration settings and environment variables for DOT inspections ETL."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to .env file. If None, uses default paths.
        """
        if config_path:
            load_dotenv(config_path)
        else:
            # Try common paths
            common_paths = [
                r'C:\Users\APalacio\PycharmProjects\AMQP_Dev\config.env',
                '.env',
                'config.env'
            ]
            for path in common_paths:
                if os.path.exists(path):
                    load_dotenv(path)
                    break
    
    @property
    def database_config(self) -> dict:
        """Get database configuration."""
        return {
            'server': os.getenv('ServerName'),
            'user': os.getenv('dbUser'),
            'password': os.getenv('dbpassword'),
            'database': os.getenv('datab', 'VTUtility')  # Default to VTUtility if not specified
        }
    
    @property
    def database_tables(self) -> dict:
        """Get database table configuration."""
        return {
            'dot_inspections_table': '[VTUtility].[dbo].[DOT_Inspections]',
            'driver_table': 'lme_prod.dbo.driver',
            'script_status_table': '[VTUtility].[dbo].[Script_status]',
            'script_id': 12  # Assuming a unique ID for DOT inspections script
        }
    
    @property
    def file_paths(self) -> dict:
        """Get file path configuration."""
        return {
            'xml_data_dir': 'dot_inspections_etl/data/xml_files',
            'processed_dir': 'dot_inspections_etl/data/processed',
            'error_dir': 'dot_inspections_etl/data/errors',
            'logs_dir': 'dot_inspections_etl/logs'
        }
    
    @property
    def processing_config(self) -> dict:
        """Get processing configuration."""
        return {
            'company_id': 'TMS',  # Default company ID for driver lookup
            'batch_size': 100,    # Number of inspections to process in a batch
            'max_retries': 3,     # Maximum retries for failed operations
            'backup_processed_files': True
        } 
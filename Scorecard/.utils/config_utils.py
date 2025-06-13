"""
Configuration management utilities for Netradyne data processing.
"""

import os
from dotenv import load_dotenv
from typing import Optional


class ConfigManager:
    """Manages configuration settings and environment variables."""
    
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
            'database': 'VTOMNITRACS'
        }
    
    @property
    def netradyne_api_config(self) -> dict:
        """Get Netradyne API configuration."""
        return {
            'basic_auth': 'ODFhY2VmNDEtZjBlMi00ZTRhLWE0OGUtZDJmZGJmZmRjYzhjOkRCNUI3QzhBQTcyODAyNjgxNTA5Rjg0MzRBQTA2MzEzNjEzN0JDRDRGMzEyMjE3NzQxMjEzQjA3M0U2Q0NGOTQ=',
            'tenant': 'VERIHA',
            'auth_url': 'https://api.netradyne.com/driveri/v1/auth/token',
            'score_url_template': 'https://api.netradyne.com/driveri/v1/tenants/VERIHA/fleetscore'
        }
    
    @property
    def netradyne_web_config(self) -> dict:
        """Get Netradyne web scraping configuration."""
        return {
            'username': os.getenv('netrad_user'),
            'password': os.getenv('netrad_pass'),
            'login_url': 'https://idms.netradyne.com/console/#/login?redirectUrl=%2F',
            'download_dir': r'C:\Users\apalacio\Downloads'
        }
    
    @property
    def database_tables(self) -> dict:
        """Get database table configuration."""
        return {
            'target_table': '[VTUtility].[dbo].[Netradyne_Driver_Score]',
            'status_table': '[VTUtility].[dbo].[Script_status]',
            'script_id': 11
        }
    
    @property
    def file_paths(self) -> dict:
        """Get file path configuration."""
        return {
            'netradyne_score_data': 'netradyne_score_data',
            'temp_files': 'temp',
            'logs': 'logs'
        } 
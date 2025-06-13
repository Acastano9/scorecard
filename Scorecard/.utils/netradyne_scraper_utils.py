"""
Netradyne web scraping utilities for automated data download.
"""

import os
import time
import datetime
import logging
from typing import Optional, Tuple
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from .config_utils import ConfigManager


class NetradyneScraper:
    """Web scraper for downloading Netradyne data files."""
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize Netradyne scraper.
        
        Args:
            config_manager: Configuration manager instance.
        """
        self.config_manager = config_manager
        self.web_config = config_manager.netradyne_web_config
        self.driver: Optional[webdriver.Chrome] = None
    
    def setup_driver(self) -> webdriver.Chrome:
        """
        Setup and configure Chrome WebDriver.
        
        Returns:
            Configured Chrome WebDriver instance.
        """
        options = webdriver.ChromeOptions()
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(options=options)
        return self.driver
    
    def login_to_netradyne(self) -> bool:
        """
        Login to the Netradyne portal.
        
        Returns:
            Boolean indicating successful login.
        """
        try:
            self.driver.get(self.web_config['login_url'])
            
            # Enter username
            username_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[id='loginUserName']"))
            )
            username_field.send_keys(self.web_config['username'])
            
            # Click continue
            continue_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@id="login-submit-button"]'))
            )
            continue_button.click()
            
            # Enter password
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//input[@type="password"]'))
            )
            password_field.send_keys(self.web_config['password'])
            
            # Click continue again
            continue_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@id="login-submit-button"]'))
            )
            continue_button.click()
            
            logging.info("Successfully logged into Netradyne portal.")
            return True
            
        except Exception as e:
            logging.error(f"Failed to log into Netradyne portal: {e}")
            return False
    
    def handle_popups(self) -> None:
        """Handle any popups that might appear after login."""
        time.sleep(4)
        try:
            # Handle "I'll do this later" popup
            later_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//button[text()="I\'ll do this later"]'))
            )
            later_button.click()
            logging.info("Dismissed popup.")
        except:
            logging.info("No popup to dismiss.")
    
    def navigate_and_download(self) -> bool:
        """
        Navigate to drivers section and download the report.
        
        Returns:
            Boolean indicating successful download initiation.
        """
        try:
            # Navigate to drivers section
            drivers_icon = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//li[@id="navbar-drivers-icon"]'))
            )
            drivers_icon.click()
            
            # Click duration filter
            duration_filter = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//div[@id="tour-alert-panel-duration-filter"]'))
            )
            duration_filter.click()
            
            # Select "Last Month"
            last_month_option = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//span[text()="Last Month"]'))
            )
            last_month_option.click()
            
            time.sleep(5)  # Wait for data to load
            
            # Click export button twice (seems to be needed based on original code)
            export_button = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@id="tour-export-driver-data"]'))
            )
            export_button.click()
            
            # Second click
            export_button = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@id="tour-export-driver-data"]'))
            )
            export_button.click()
            
            time.sleep(4)  # Wait for download to initiate
            
            logging.info("Successfully initiated download from Netradyne portal.")
            return True
            
        except Exception as e:
            logging.error(f"Failed to navigate and download from Netradyne portal: {e}")
            return False
    
    def cleanup(self) -> None:
        """Close the WebDriver."""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logging.info("WebDriver closed.")
    
    def get_expected_filename(self) -> str:
        """
        Generate the expected filename for the downloaded CSV.
        
        Returns:
            Expected filename string.
        """
        today = datetime.datetime.now()
        formatted_date = today.strftime('%b-%d-%Y')
        return f'Drivers-Report({formatted_date}).csv'
    
    def move_downloaded_file(self, target_dir: str = 'netradyne_score_data') -> Optional[str]:
        """
        Move the downloaded file to the target directory.
        
        Args:
            target_dir: Target directory for the file.
        
        Returns:
            Path to the moved file or None if failed.
        """
        try:
            expected_filename = self.get_expected_filename()
            source_path = os.path.join(self.web_config['download_dir'], expected_filename)
            
            # Create target directory if it doesn't exist
            os.makedirs(target_dir, exist_ok=True)
            
            # Generate target filename with timestamp
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
            target_filename = f'netradyne_{yesterday}.csv'
            target_path = os.path.join(target_dir, target_filename)
            
            # Move the file
            if os.path.exists(source_path):
                os.rename(source_path, target_path)
                logging.info(f"Successfully moved file to: {target_path}")
                return target_path
            else:
                logging.error(f"Downloaded file not found: {source_path}")
                return None
                
        except Exception as e:
            logging.error(f"Failed to move downloaded file: {e}")
            return None
    
    def download_netradyne_file(self, target_dir: str = 'netradyne_score_data') -> Optional[str]:
        """
        Complete workflow to download Netradyne file.
        
        Args:
            target_dir: Directory to save the downloaded file.
        
        Returns:
            Path to the downloaded file or None if failed.
        """
        try:
            # Setup driver
            self.setup_driver()
            
            # Login
            if not self.login_to_netradyne():
                return None
            
            # Handle popups
            self.handle_popups()
            
            # Navigate and download
            if not self.navigate_and_download():
                return None
            
            # Close browser
            self.cleanup()
            
            # Move file to target location
            moved_file_path = self.move_downloaded_file(target_dir)
            return moved_file_path
            
        except Exception as e:
            logging.error(f"Error in download workflow: {e}")
            return None
        finally:
            self.cleanup()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup() 
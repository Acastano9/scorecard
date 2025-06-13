"""
Netradyne API utilities for data retrieval.
"""

import requests
import datetime
import time
import logging
from typing import Optional, List, Dict, Any, Tuple
from .config_utils import ConfigManager


class NetradyneAPIClient:
    """Client for interacting with the Netradyne API."""
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize Netradyne API client.
        
        Args:
            config_manager: Configuration manager instance.
        """
        self.config_manager = config_manager
        self.api_config = config_manager.netradyne_api_config
        
    def get_existing_tokens(self) -> Optional[str]:
        """
        Retrieves existing auth tokens from the Netradyne API.
        
        Returns:
            Access token string or None if no valid tokens found.
        """
        headers = {
            'Accept-Language': 'en-US',
            'Authorization': f'Basic {self.api_config["basic_auth"]}'
        }
        try:
            response = requests.get(
                self.api_config["auth_url"] + 's', 
                headers=headers, 
                timeout=30
            )
            response.raise_for_status()
            token_data = response.json()
            tokens = token_data.get('data', [])
            
            if tokens:
                logging.info(f"Found {len(tokens)} existing token(s).")
                # Return the token with the longest remaining lifetime
                valid_tokens = [t for t in tokens if t.get('expiresOn', 0) > (time.time() * 1000)]
                if valid_tokens:
                    # Sort by expiration time, descending (latest expiry first)
                    valid_tokens.sort(key=lambda x: x.get('expiresOn', 0), reverse=True)
                    selected_token = valid_tokens[0]
                    expiry_time = datetime.datetime.fromtimestamp(
                        selected_token.get('expiresOn', 0)/1000
                    ).strftime('%Y-%m-%d %H:%M:%S')
                    logging.info(f"Using existing token that expires at {expiry_time}")
                    return selected_token.get('accessToken')
            
            logging.info("No valid existing tokens found.")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error retrieving existing tokens: {e}")
            return None
    
    def create_new_token(self) -> Optional[str]:
        """
        Creates a new authentication token.
        
        Returns:
            Access token string or None if creation failed.
        """
        logging.info("Requesting new auth token...")
        headers = {
            'Accept-Language': 'en-US',
            'Authorization': f'Basic {self.api_config["basic_auth"]}'
        }
        try:
            response = requests.post(
                self.api_config["auth_url"], 
                headers=headers, 
                timeout=30
            )
            response.raise_for_status()
            token_data = response.json()
            access_token = token_data.get('data', {}).get('accessToken')
            
            if access_token:
                logging.info("Successfully created new Netradyne access token.")
                return access_token
            else:
                logging.error(f"Could not extract access token from response: {token_data}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error creating new auth token: {e}")
            return None
    
    def get_auth_token(self) -> Optional[str]:
        """
        Gets an auth token, reusing existing ones if available or creating a new one if needed.
        
        Returns:
            Access token string or None if authentication failed.
        """
        # First, try to get an existing token
        existing_token = self.get_existing_tokens()
        if existing_token:
            return existing_token
        
        # If no existing token is available, create a new one
        return self.create_new_token()
    
    def get_previous_month_details(self) -> Tuple[int, str]:
        """
        Calculates the start timestamp and date string for the previous month.
        
        Returns:
            Tuple of (timestamp_ms, report_month_str)
        """
        today = datetime.date.today()
        first_day_current_month = today.replace(day=1)
        last_day_previous_month = first_day_current_month - datetime.timedelta(days=1)
        first_day_previous_month = last_day_previous_month.replace(day=1)

        # Timestamp in milliseconds since epoch for the start of the previous month
        start_datetime = datetime.datetime.combine(first_day_previous_month, datetime.time.min)
        # Convert to UTC epoch time in milliseconds as required by the API
        timestamp_ms = int(start_datetime.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)

        report_month_str = first_day_previous_month.strftime('%Y-%m-%d')
        return timestamp_ms, report_month_str
    
    def get_fleet_scores(self, access_token: str, timestamp_ms: int) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches fleet scores for the specified month.
        
        Args:
            access_token: Valid authentication token.
            timestamp_ms: Timestamp in milliseconds for the target month.
        
        Returns:
            List of score dictionaries or None if retrieval failed.
        """
        headers = {
            'Accept-Language': 'en-US',
            'Authorization': f'Bearer {access_token}'
        }
        params = {
            'time': timestamp_ms,
            'interval': 'monthly',
            'limit': 1000  # Adjust if you have more than 1000 drivers
        }
        
        try:
            response = requests.get(
                self.api_config["score_url_template"], 
                headers=headers, 
                params=params, 
                timeout=60
            )
            response.raise_for_status()
            score_data = response.json()
            scores = score_data.get('data', {}).get('scores')
            
            if scores is not None:  # Check for None explicitly, as empty list is valid
                logging.info(f"Successfully retrieved {len(scores)} driver scores.")
                return scores
            else:
                logging.error(f"Could not extract scores from response: {score_data}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error getting fleet scores: {e}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing score data: {e}")
            return None
    
    def fetch_driver_scores(self) -> Optional[Tuple[List[Dict[str, Any]], str]]:
        """
        Complete workflow to fetch driver scores for the previous month.
        
        Returns:
            Tuple of (processed_scores, report_month) or None if failed.
        """
        try:
            # Get previous month details
            timestamp_ms, report_month = self.get_previous_month_details()
            logging.info(f"Targeting report month: {report_month} (Timestamp: {timestamp_ms})")
            
            # Get authentication token
            access_token = self.get_auth_token()
            if not access_token:
                logging.error("Failed to obtain authentication token")
                return None
            
            # Fetch scores
            raw_scores = self.get_fleet_scores(access_token, timestamp_ms)
            if raw_scores is None:
                logging.error("Failed to retrieve scores from API")
                return None
            
            # Process scores into standardized format
            processed_scores = []
            for score_entry in raw_scores:
                driver_info = score_entry.get('driver', {})
                driver_id = driver_info.get('driverId')
                driver_score = score_entry.get('score')
                minutes_analyzed = 0  # API does not provide this, defaulting to 0
                
                if driver_id is not None and driver_score is not None:
                    processed_scores.append({
                        'driver_id': driver_id,
                        'minutes_analyzed': minutes_analyzed,
                        'driver_score': driver_score
                    })
                else:
                    logging.warning(f"Skipping score entry due to missing data: {score_entry}")
            
            logging.info(f"Processed {len(processed_scores)} valid driver scores.")
            return processed_scores, report_month
            
        except Exception as e:
            logging.error(f"Error in fetch_driver_scores workflow: {e}")
            return None 
"""
Database utilities for DOT inspections ETL.
"""

import pyodbc
import logging
from typing import Optional, List, Dict, Any
from .config_utils import DOTConfigManager


class DOTDatabaseManager:
    """Manages database connections and operations for DOT inspections ETL."""
    
    def __init__(self, config_manager: DOTConfigManager):
        """
        Initialize database manager.
        
        Args:
            config_manager: Configuration manager instance.
        """
        self.config_manager = config_manager
        self.db_config = config_manager.database_config
        self.table_config = config_manager.database_tables
        self.processing_config = config_manager.processing_config
        self.connection: Optional[pyodbc.Connection] = None
        
    def create_connection(self) -> Optional[pyodbc.Connection]:
        """
        Creates and returns a pyodbc database connection.
        
        Returns:
            Database connection or None if failed.
        """
        try:
            conn_str = (
                f'Driver={{SQL Server}};'
                f'Server={self.db_config["server"]};'
                f'Database={self.db_config["database"]};'
                f'UID={self.db_config["user"]};'
                f'PWD={self.db_config["password"]};'
                'Encrypt=no;'
            )
            self.connection = pyodbc.connect(conn_str)
            logging.info("Successfully connected to the database.")
            return self.connection
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logging.error(f"Database connection failed. SQLSTATE: {sqlstate}. Error: {ex}")
            return None
    
    def close_connection(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logging.info("Database connection closed.")
    
    def get_existing_inspections(self) -> List[int]:
        """
        Get list of inspection IDs already in the database.
        
        Returns:
            List of existing inspection IDs.
        """
        if not self.connection:
            logging.error("No database connection available")
            return []
        
        try:
            cursor = self.connection.cursor()
            query = f"SELECT inspection_id FROM {self.table_config['dot_inspections_table']}"
            cursor.execute(query)
            results = cursor.fetchall()
            inspection_ids = [row[0] for row in results]
            logging.info(f"Found {len(inspection_ids)} existing inspections in database")
            return inspection_ids
        except Exception as e:
            logging.error(f"Error retrieving existing inspections: {e}")
            return []
        finally:
            cursor.close()
    
    def get_driver_id(self, license_number: str) -> Optional[str]:
        """
        Get driver ID from the database based on license number.
        
        Args:
            license_number: Driver license number.
        
        Returns:
            Driver ID or None if not found.
        """
        if not self.connection:
            logging.error("No database connection available")
            return None
        
        try:
            cursor = self.connection.cursor()
            company_id = self.processing_config['company_id']
            # Escape single quotes in license number for SQL safety
            license_escaped = license_number.replace("'", "''")
            
            query = f"""
                SELECT id FROM {self.table_config['driver_table']} 
                WHERE license_no = '{license_escaped}' AND company_id = '{company_id}'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                driver_id = result[0].strip() if isinstance(result[0], str) else str(result[0])
                logging.debug(f"Found driver ID {driver_id} for license {license_number}")
                return driver_id
            else:
                logging.debug(f"No driver found for license {license_number}")
                return None
                
        except Exception as e:
            logging.error(f"Error retrieving driver ID for license {license_number}: {e}")
            return None
        finally:
            cursor.close()
    
    def insert_inspection(self, inspection_data: Dict[str, Any]) -> bool:
        """
        Insert a single inspection record into the database.
        
        Args:
            inspection_data: Dictionary containing inspection data.
        
        Returns:
            Boolean indicating success.
        """
        if not self.connection:
            logging.error("No database connection available")
            return False
        
        cursor = None
        try:
            cursor = self.connection.cursor()
            
            # Escape single quotes in text fields for SQL safety
            def escape_sql_string(value):
                if value is None:
                    return 'NULL'
                return f"'{str(value).replace(chr(39), chr(39) + chr(39))}'"
            
            table_name = self.table_config['dot_inspections_table']
            inspection_id = inspection_data['inspection_id']
            post_date = escape_sql_string(inspection_data['post_date'])
            driver_name = escape_sql_string(inspection_data['driver_name'])
            tractor_id = escape_sql_string(inspection_data['tractor_id'])
            tractor_license = escape_sql_string(inspection_data['tractor_license'])
            trailer_id = escape_sql_string(inspection_data['trailer_id'])
            trailer_license = escape_sql_string(inspection_data['trailer_license'])
            violations = escape_sql_string(inspection_data['violations'])
            driver_id = escape_sql_string(inspection_data['driver_id'])
            
            query = f"""
                INSERT INTO {table_name}
                ([inspection_id], [post_date], [driver_name], [tractor_id], 
                [tractor_license], [trailer_id], [trailer_license], [violations], [driver_id])
                VALUES 
                ({inspection_id}, 
                CAST({post_date} AS DATE), 
                {driver_name}, 
                {tractor_id},
                {tractor_license}, 
                {trailer_id}, 
                {trailer_license}, 
                {violations}, 
                {driver_id})
            """
            
            logging.debug(f"Executing query: {query}")
            cursor.execute(query)
            self.connection.commit()
            logging.info(f"Successfully inserted inspection {inspection_data['inspection_id']}")
            return True
            
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logging.error(f"Database insert failed. SQLSTATE: {sqlstate}. Error: {ex}")
            try:
                self.connection.rollback()
            except:
                pass
            return False
        except Exception as e:
            logging.error(f"Unexpected error during inspection insert: {e}")
            try:
                self.connection.rollback()
            except:
                pass
            return False
        finally:
            if cursor:
                cursor.close()
    
    def insert_inspections_batch(self, inspections: List[Dict[str, Any]]) -> int:
        """
        Insert multiple inspection records in a batch.
        
        Args:
            inspections: List of inspection data dictionaries.
        
        Returns:
            Number of successfully inserted records.
        """
        success_count = 0
        for inspection in inspections:
            if self.insert_inspection(inspection):
                success_count += 1
        
        logging.info(f"Batch insert completed: {success_count}/{len(inspections)} records inserted")
        return success_count
    
    def update_script_status(self, success: bool = True, error_message: Optional[str] = None) -> bool:
        """
        Updates the script status table with execution results.
        
        Args:
            success: Boolean indicating successful execution.
            error_message: Error message in case of failure.
        
        Returns:
            Boolean indicating if the status update itself was successful.
        """
        cursor = None
        try:
            if self.connection is None:
                logging.error("Cannot update status: No database connection")
                return False
                
            cursor = self.connection.cursor()
            script_id = self.table_config['script_id']
            status_table = self.table_config['script_status_table']
            
            if success:
                query = f"UPDATE {status_table} SET result=1, Last_execution=GETDATE() WHERE ID = {script_id}"
            else:
                error_msg = error_message if error_message else "Unknown error"
                error_msg = error_msg.replace("'", "''")  # Escape single quotes for SQL
                query = f"UPDATE {status_table} SET result=0, comments='{error_msg}', Last_execution=GETDATE() WHERE ID = {script_id}"
            
            cursor.execute(query)
            self.connection.commit()
            logging.info(f"Successfully updated script status to {'SUCCESS' if success else 'FAILED'}")
            return True
        except Exception as e:
            logging.error(f"Failed to update script status: {e}")
            if self.connection:
                try:
                    self.connection.rollback()
                except:
                    pass
            return False
        finally:
            if cursor:
                cursor.close()
    
    def __enter__(self):
        """Context manager entry."""
        self.create_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_connection() 
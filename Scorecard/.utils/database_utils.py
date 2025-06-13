"""
Database utilities for Netradyne data processing.
"""

import pyodbc
import logging
from typing import Optional, List, Dict, Any
from .config_utils import ConfigManager


class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize database manager.
        
        Args:
            config_manager: Configuration manager instance.
        """
        self.config_manager = config_manager
        self.db_config = config_manager.database_config
        self.table_config = config_manager.database_tables
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
            status_table = self.table_config['status_table']
            
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
    
    def insert_driver_scores(self, scores: List[Dict[str, Any]], report_month: str) -> bool:
        """
        Inserts driver scores into the database.
        
        Args:
            scores: List of score dictionaries containing driver data.
            report_month: Report month string in YYYY-MM-DD format.
        
        Returns:
            Boolean indicating success.
        """
        if not scores:
            logging.info("No scores to insert.")
            return True
        
        cursor = None
        try:
            cursor = self.connection.cursor()
            values_list = []
            
            for score_entry in scores:
                driver_id = score_entry.get('driver_id')
                minutes_analyzed = score_entry.get('minutes_analyzed', 0)
                driver_score = score_entry.get('driver_score')
                
                if driver_id is not None and driver_score is not None:
                    driver_id_escaped = str(driver_id).replace("'", "''")
                    values_list.append(f"('{driver_id_escaped}', {minutes_analyzed}, {int(driver_score)}, '{report_month}')")
                else:
                    logging.warning(f"Skipping score entry due to missing data: {score_entry}")
            
            if not values_list:
                logging.warning("No valid score entries found to insert after processing.")
                return True
            
            target_table = self.table_config['target_table']
            insert_query = f"""
                INSERT INTO {target_table}
                ([driver_id], [minutes_analyzed], [driver_score], [reported_month])
                VALUES {', '.join(values_list)}
            """
            
            logging.info(f"Executing insert query for {len(values_list)} records...")
            cursor.execute(insert_query)
            self.connection.commit()
            logging.info(f"Successfully inserted {len(values_list)} records for {report_month}.")
            return True
            
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logging.error(f"Database insert failed. SQLSTATE: {sqlstate}. Error: {ex}")
            try:
                self.connection.rollback()
            except Exception as rollback_ex:
                logging.error(f"Failed to rollback transaction: {rollback_ex}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error during database insert: {e}")
            try:
                self.connection.rollback()
            except:
                pass
            return False
        finally:
            if cursor:
                cursor.close()
    
    def insert_hos_violations(self, violations, report_month, script_id=12) -> bool:
        """
        Insert HOS violations data into the database.
        
        Args:
            violations: List of HOSViolation objects
            report_month: Report month string
            script_id: Script ID for status tracking
        
        Returns:
            Boolean indicating success
        """
        try:
            if not self.connection:
                return False
            
            logging.info(f"Starting database insertion for {len(violations)} HOS violations")
            
            inserted_count = 0
            skipped_count = 0
            
            # Process in batches for better performance
            batch_size = 1000
            for i in range(0, len(violations), batch_size):
                batch = violations[i:i+batch_size]
                violations_to_insert = []
                
                # Check for duplicates in current batch
                for violation in batch:
                    if not self.check_hos_violation_exists(violation.id):
                        violations_to_insert.append(violation.as_tuple())
                    else:
                        skipped_count += 1
                
                if violations_to_insert:
                    # Insert batch of violations
                    sql = """
                    INSERT INTO VTUtility.dbo.HOS_Violations 
                    (ID, Start_Time_and_Driver, Driver_ID, Driver_Name, 
                     Violation_Start_Time, Violation_End_Time, Driver_Status, 
                     Terminal, Ruleset, Violation_Type, Violation_Duration)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    cursor = self.connection.cursor()
                    cursor.executemany(sql, violations_to_insert)
                    self.connection.commit()
                    cursor.close()
                    
                    inserted_count += len(violations_to_insert)
                    logging.info(f"Batch {i//batch_size + 1}: Inserted {len(violations_to_insert)} new violations. "
                               f"Processed: {min(i+batch_size, len(violations))}/{len(violations)}. "
                               f"Skipped: {skipped_count}")
                
                else:
                    logging.info(f"Batch {i//batch_size + 1}: Skipped {len(batch)} duplicate violations. "
                               f"Processed: {min(i+batch_size, len(violations))}/{len(violations)}. "
                               f"Total Skipped: {skipped_count}")
            
            # Log final summary
            logging.info(f"\n--- HOS Violations Import Summary ---")
            logging.info(f"Total violations processed: {len(violations)}")
            logging.info(f"New violations inserted: {inserted_count}")
            logging.info(f"Duplicate violations skipped: {skipped_count}")
            logging.info(f"Report month: {report_month}")
            
            success = inserted_count > 0 or len(violations) == skipped_count
            
            # Update script status
            if success:
                self.update_script_status(script_id, True)
                logging.info("HOS violations import completed successfully")
            else:
                self.update_script_status(script_id, False, "No valid data to insert")
                logging.warning("HOS violations import completed with warnings")
            
            return success
            
        except Exception as e:
            error_msg = f"Error inserting HOS violations: {str(e)}"
            logging.error(error_msg)
            self.update_script_status(script_id, False, error_msg)
            if self.connection:
                try:
                    self.connection.rollback()
                except:
                    pass
            return False
    
    def check_hos_violation_exists(self, violation_id: str) -> bool:
        """
        Check if HOS violation with given ID already exists.
        
        Args:
            violation_id: Unique violation ID
        
        Returns:
            Boolean indicating if violation exists
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM VTUtility.dbo.HOS_Violations WHERE ID = ?", (violation_id,))
            count = cursor.fetchone()[0]
            cursor.close()
            return count > 0
        except Exception as e:
            logging.warning(f"Error checking HOS violation duplicate: {e}")
            return False
    
    def get_hos_violations_by_driver(self, driver_id: str) -> List[Dict]:
        """
        Get all HOS violations for a specific driver.
        
        Args:
            driver_id: Driver ID to search for
        
        Returns:
            List of violation dictionaries
        """
        try:
            cursor = self.connection.cursor()
            sql = """
            SELECT * FROM VTUtility.dbo.HOS_Violations 
            WHERE Driver_ID = ? 
            ORDER BY Violation_Start_Time DESC
            """
            cursor.execute(sql, (driver_id,))
            columns = [column[0] for column in cursor.description]
            violations = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return violations
        except Exception as e:
            logging.error(f"Error retrieving HOS violations for driver {driver_id}: {e}")
            return []
    
    def get_hos_violations_summary(self) -> List[Dict]:
        """
        Get summary of HOS violations by driver.
        
        Returns:
            List of driver summary dictionaries
        """
        try:
            cursor = self.connection.cursor()
            sql = """
            SELECT 
                Driver_ID,
                Driver_Name,
                COUNT(*) as violation_count,
                MAX(Violation_Start_Time) as latest_violation
            FROM VTUtility.dbo.HOS_Violations
            GROUP BY Driver_ID, Driver_Name
            ORDER BY violation_count DESC
            """
            cursor.execute(sql)
            columns = [column[0] for column in cursor.description]
            summary = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return summary
        except Exception as e:
            logging.error(f"Error retrieving HOS violations summary: {e}")
            return []

    def store_maintenance_records(self, maintenance_records: List[Dict[str, Any]], process_date: str) -> bool:
        """
        Store maintenance records in the database.
        
        Args:
            maintenance_records: List of maintenance dictionaries
            process_date: Process date string
        
        Returns:
            Boolean indicating success
        """
        if not maintenance_records:
            logging.info("No maintenance records to store.")
            return True
        
        cursor = None
        try:
            if self.connection is None:
                logging.error("Cannot store maintenance records: No database connection")
                return False
                
            cursor = self.connection.cursor()
            
            # Check if table exists, create if it doesn't
            table_check_sql = """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Maintenance_Records' AND xtype='U')
            BEGIN
                CREATE TABLE VTUtility.dbo.Maintenance_Records (
                    ID int IDENTITY(1,1) PRIMARY KEY,
                    Vehicle_ID nvarchar(50),
                    Vehicle_Number nvarchar(50),
                    Maintenance_Type nvarchar(100),
                    Due_Date nvarchar(50),
                    Last_Service nvarchar(50),
                    Mileage float,
                    Service_Miles float,
                    Status nvarchar(50),
                    Priority nvarchar(50),
                    Location nvarchar(100),
                    Process_Date date,
                    Created_Date datetime DEFAULT GETDATE()
                )
            END
            """
            cursor.execute(table_check_sql)
            self.connection.commit()
            
            inserted_count = 0
            skipped_count = 0
            
            # Process in batches for better performance
            batch_size = 1000
            for i in range(0, len(maintenance_records), batch_size):
                batch = maintenance_records[i:i+batch_size]
                records_to_insert = []
                
                # Check for duplicates and prepare data
                for record in batch:
                    vehicle_id = record.get('vehicle_id', '')
                    maintenance_type = record.get('maintenance_type', '')
                    due_date = record.get('due_date', '')
                    
                    # Check for duplicates (vehicle_id + maintenance_type combination)
                    if not self.check_maintenance_record_exists(vehicle_id, maintenance_type, due_date):
                        record_tuple = (
                            vehicle_id,
                            record.get('vehicle_number', ''),
                            maintenance_type,
                            due_date,
                            record.get('last_service', ''),
                            record.get('mileage', 0),
                            record.get('service_miles', 0),
                            record.get('status', ''),
                            record.get('priority', ''),
                            record.get('location', ''),
                            process_date
                        )
                        records_to_insert.append(record_tuple)
                    else:
                        skipped_count += 1
                
                if records_to_insert:
                    # Insert batch of records
                    sql = """
                    INSERT INTO VTUtility.dbo.Maintenance_Records 
                    (Vehicle_ID, Vehicle_Number, Maintenance_Type, Due_Date, 
                     Last_Service, Mileage, Service_Miles, Status, 
                     Priority, Location, Process_Date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    cursor.executemany(sql, records_to_insert)
                    self.connection.commit()
                    
                    inserted_count += len(records_to_insert)
                    logging.info(f"Batch {i//batch_size + 1}: Inserted {len(records_to_insert)} new maintenance records")
                
                else:
                    logging.info(f"Batch {i//batch_size + 1}: Skipped {len(batch)} duplicate maintenance records")
            
            # Log final summary
            logging.info(f"\n--- Maintenance Records Import Summary ---")
            logging.info(f"Total records processed: {len(maintenance_records)}")
            logging.info(f"New records inserted: {inserted_count}")
            logging.info(f"Duplicate records skipped: {skipped_count}")
            logging.info(f"Process date: {process_date}")
            
            success = inserted_count > 0 or len(maintenance_records) == skipped_count
            
            # Update script status
            script_id = 13  # Use script_id 13 for maintenance
            if success:
                self.update_script_status_by_id(script_id, True)
                logging.info("Maintenance records import completed successfully")
            else:
                self.update_script_status_by_id(script_id, False, "No valid data to insert")
                logging.warning("Maintenance records import completed with warnings")
            
            return success
            
        except Exception as e:
            error_msg = f"Error storing maintenance records: {str(e)}"
            logging.error(error_msg)
            script_id = 13
            self.update_script_status_by_id(script_id, False, error_msg)
            if self.connection:
                try:
                    self.connection.rollback()
                except:
                    pass
            return False
        finally:
            if cursor:
                cursor.close()
    
    def check_maintenance_record_exists(self, vehicle_id: str, maintenance_type: str, due_date: str) -> bool:
        """
        Check if maintenance record with given vehicle ID, type, and due date already exists.
        
        Args:
            vehicle_id: Vehicle ID
            maintenance_type: Type of maintenance
            due_date: Due date
        
        Returns:
            Boolean indicating if record exists
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM VTUtility.dbo.Maintenance_Records 
                WHERE Vehicle_ID = ? AND Maintenance_Type = ? AND Due_Date = ?
            """, (vehicle_id, maintenance_type, due_date))
            count = cursor.fetchone()[0]
            cursor.close()
            return count > 0
        except Exception as e:
            logging.warning(f"Error checking maintenance record duplicate: {e}")
            return False
    
    def update_script_status_by_id(self, script_id: int, success: bool = True, error_message: Optional[str] = None) -> bool:
        """
        Updates the script status table for a specific script ID.
        
        Args:
            script_id: ID of the script to update
            success: Boolean indicating successful execution
            error_message: Error message in case of failure
        
        Returns:
            Boolean indicating if the status update was successful
        """
        cursor = None
        try:
            if self.connection is None:
                logging.error("Cannot update status: No database connection")
                return False
                
            cursor = self.connection.cursor()
            status_table = "[VTUtility].[dbo].[Script_status]"
            
            if success:
                query = f"UPDATE {status_table} SET result=1, Last_execution=GETDATE() WHERE ID = {script_id}"
            else:
                error_msg = error_message if error_message else "Unknown error"
                error_msg = error_msg.replace("'", "''")  # Escape single quotes for SQL
                query = f"UPDATE {status_table} SET result=0, comments='{error_msg}', Last_execution=GETDATE() WHERE ID = {script_id}"
            
            cursor.execute(query)
            self.connection.commit()
            logging.info(f"Successfully updated script status for ID {script_id} to {'SUCCESS' if success else 'FAILED'}")
            return True
        except Exception as e:
            logging.error(f"Failed to update script status for ID {script_id}: {e}")
            if self.connection:
                try:
                    self.connection.rollback()
                except:
                    pass
            return False
        finally:
            if cursor:
                cursor.close()
    
    def test_connection(self) -> bool:
        """
        Test database connectivity.
        
        Returns:
            Boolean indicating if connection is successful
        """
        try:
            if self.connection is None:
                self.create_connection()
            
            if self.connection:
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return True
            return False
        except Exception as e:
            logging.error(f"Database connection test failed: {e}")
            return False

    def __enter__(self):
        """Context manager entry."""
        self.create_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_connection() 
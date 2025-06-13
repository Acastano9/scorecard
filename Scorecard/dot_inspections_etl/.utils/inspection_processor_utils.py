"""
Inspection processing utilities for DOT inspections ETL.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple


class InspectionProcessor:
    """Handles business logic for processing DOT inspection data."""
    
    def __init__(self):
        """Initialize inspection processor."""
        pass
    
    def process_violations(self, violations_data: Any) -> str:
        """
        Process violations data from inspection record.
        
        Args:
            violations_data: Violations data from XML (can be dict or list).
        
        Returns:
            Formatted violations string.
        """
        if not violations_data:
            return ''
        
        try:
            if isinstance(violations_data, dict):
                # Single violation
                fed_code = violations_data.get('FedVioCode', '')
                viol_cat = violations_data.get('ViolationCategory', '')
                viol_desc = violations_data.get('SectionDesc', '')
                return f"{fed_code} {viol_cat} {viol_desc}".strip()
            else:
                # Multiple violations
                violations = []
                for violation in violations_data:
                    if isinstance(violation, dict):
                        fed_code = violation.get('FedVioCode', '')
                        viol_cat = violation.get('ViolationCategory', '')
                        viol_desc = violation.get('SectionDesc', '')
                        violation_str = f"{fed_code} {viol_cat} {viol_desc}".strip()
                        if violation_str:
                            violations.append(violation_str)
                
                # Join with separator and clean up quotes
                violations_str = ' | '.join(violations)
                return violations_str.replace("'", "")
                
        except Exception as e:
            logging.error(f"Error processing violations: {e}")
            return ''
    
    def process_vehicle_data(self, vehicle_data: Any) -> Tuple[str, str, str, str]:
        """
        Process vehicle data from inspection record.
        
        Args:
            vehicle_data: Vehicle data from XML (can be dict or list).
        
        Returns:
            Tuple of (tractor_id, tractor_license, trailer_id, trailer_license).
        """
        tractor_id = ''
        tractor_license = ''
        trailer_id = ''
        trailer_license = ''
        
        try:
            if isinstance(vehicle_data, dict):
                # Single vehicle
                unit_type = vehicle_data.get('VehicleUnitTypeCode', '').upper()
                vehicle_id = vehicle_data.get('VehicleCompanyID', '')
                vehicle_license = vehicle_data.get('VehicleLicenseID', '')
                
                if 'TRACTOR' in unit_type:
                    tractor_id = vehicle_id
                    tractor_license = vehicle_license
                elif 'TRAILER' in unit_type:
                    trailer_id = vehicle_id
                    trailer_license = vehicle_license
            else:
                # Multiple vehicles
                for vehicle in vehicle_data:
                    if isinstance(vehicle, dict):
                        unit_type = vehicle.get('VehicleUnitTypeCode', '').upper()
                        vehicle_id = vehicle.get('VehicleCompanyID', '')
                        vehicle_license = vehicle.get('VehicleLicenseID', '')
                        
                        if 'TRACTOR' in unit_type:
                            tractor_id = vehicle_id
                            tractor_license = vehicle_license
                        elif 'TRAILER' in unit_type:
                            trailer_id = vehicle_id
                            trailer_license = vehicle_license
                            
        except Exception as e:
            logging.error(f"Error processing vehicle data: {e}")
        
        return tractor_id, tractor_license, trailer_id, trailer_license
    
    def extract_inspection_data(self, inspection: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract and process data from a single inspection record.
        
        Args:
            inspection: Single inspection data from XML.
        
        Returns:
            Processed inspection data dictionary or None if invalid.
        """
        try:
            # Extract basic inspection information
            insp_main = inspection.get('InspMain', {})
            inspection_id = insp_main.get('inspectionId')
            post_date = insp_main.get('InspectionPostDate')
            
            if not inspection_id or not post_date:
                logging.warning("Missing inspection ID or post date")
                return None
            
            # Convert inspection_id to integer
            try:
                inspection_id = int(inspection_id)
            except (ValueError, TypeError):
                logging.warning(f"Invalid inspection ID: {inspection_id}")
                return None
            
            # Extract driver information
            drivers_data = inspection.get('Drivers', {})
            driver_data = drivers_data.get('Driver', {})
            driver_name = driver_data.get('DriverLastName', '')
            license_number = driver_data.get('DriverLicenseID', '')
            
            if not driver_name or not license_number:
                logging.warning(f"Missing driver information for inspection {inspection_id}")
                return None
            
            # Process vehicle data
            vehicles_data = inspection.get('Vehicles', {})
            vehicle_data = vehicles_data.get('Vehicle', {})
            tractor_id, tractor_license, trailer_id, trailer_license = self.process_vehicle_data(vehicle_data)
            
            # Process violations
            violations = ''
            if 'Violations' in inspection and inspection['Violations']:
                violations_data = inspection['Violations'].get('Violation', {})
                violations = self.process_violations(violations_data)
            
            # Create processed inspection data
            processed_data = {
                'inspection_id': inspection_id,
                'post_date': post_date,
                'driver_name': driver_name,
                'license_number': license_number,
                'tractor_id': tractor_id,
                'tractor_license': tractor_license,
                'trailer_id': trailer_id,
                'trailer_license': trailer_license,
                'violations': violations,
                'driver_id': None  # To be filled in later
            }
            
            logging.debug(f"Successfully processed inspection {inspection_id}")
            return processed_data
            
        except Exception as e:
            logging.error(f"Error extracting inspection data: {e}")
            return None
    
    def process_inspections_batch(self, inspections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of inspections.
        
        Args:
            inspections: List of inspection data from XML.
        
        Returns:
            List of processed inspection data dictionaries.
        """
        processed_inspections = []
        
        for i, inspection in enumerate(inspections):
            logging.debug(f"Processing inspection {i + 1} of {len(inspections)}")
            
            processed_data = self.extract_inspection_data(inspection)
            if processed_data:
                processed_inspections.append(processed_data)
            else:
                logging.warning(f"Skipped invalid inspection at index {i}")
        
        logging.info(f"Successfully processed {len(processed_inspections)} out of {len(inspections)} inspections")
        return processed_inspections
    
    def validate_processed_inspection(self, inspection_data: Dict[str, Any]) -> bool:
        """
        Validate processed inspection data before database insertion.
        
        Args:
            inspection_data: Processed inspection data dictionary.
        
        Returns:
            Boolean indicating if data is valid for insertion.
        """
        required_fields = ['inspection_id', 'post_date', 'driver_name', 'license_number']
        
        for field in required_fields:
            if field not in inspection_data or not inspection_data[field]:
                logging.warning(f"Missing required field '{field}' in inspection data")
                return False
        
        # Validate inspection_id is integer
        if not isinstance(inspection_data['inspection_id'], int):
            logging.warning(f"Invalid inspection_id type: {type(inspection_data['inspection_id'])}")
            return False
        
        # Validate post_date format (basic check)
        post_date = inspection_data['post_date']
        if not isinstance(post_date, str) or len(post_date) < 8:
            logging.warning(f"Invalid post_date format: {post_date}")
            return False
        
        return True
    
    def get_processing_summary(self, processed_inspections: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Get summary statistics for processed inspections.
        
        Args:
            processed_inspections: List of processed inspection data.
        
        Returns:
            Dictionary with processing summary.
        """
        summary = {
            'total_processed': len(processed_inspections),
            'with_violations': 0,
            'with_tractor': 0,
            'with_trailer': 0,
            'valid_for_insertion': 0
        }
        
        for inspection in processed_inspections:
            if inspection.get('violations'):
                summary['with_violations'] += 1
            
            if inspection.get('tractor_id'):
                summary['with_tractor'] += 1
            
            if inspection.get('trailer_id'):
                summary['with_trailer'] += 1
            
            if self.validate_processed_inspection(inspection):
                summary['valid_for_insertion'] += 1
        
        logging.info(f"Processing Summary: {summary}")
        return summary 
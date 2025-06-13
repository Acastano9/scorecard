"""
XML processing utilities for DOT inspections ETL.
"""

import logging
import xmltodict
from typing import Dict, Any, Optional, List
from pathlib import Path


class XMLProcessor:
    """Handles XML file parsing and data extraction for DOT inspections."""
    
    def __init__(self):
        """Initialize XML processor."""
        pass
    
    def parse_xml_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Parse XML file and return structured data.
        
        Args:
            file_path: Path to XML file.
        
        Returns:
            Parsed XML data as dictionary or None if failed.
        """
        try:
            file_path_obj = Path(file_path)
            
            if not file_path_obj.exists():
                logging.error(f"XML file not found: {file_path}")
                return None
            
            if file_path_obj.suffix.lower() != '.xml':
                logging.error(f"File is not an XML file: {file_path}")
                return None
            
            logging.info(f"Parsing XML file: {file_path}")
            
            with open(file_path, 'rb') as file:
                data = xmltodict.parse(file)
                logging.info(f"Successfully parsed XML file: {file_path}")
                return data
                
        except Exception as e:
            logging.error(f"Error parsing XML file {file_path}: {e}")
            return None
    
    def extract_inspections(self, xml_data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """
        Extract inspections data from parsed XML.
        
        Args:
            xml_data: Parsed XML data.
        
        Returns:
            List of inspection dictionaries or None if failed.
        """
        try:
            if 'Inspections' not in xml_data:
                logging.error("No 'Inspections' root element found in XML data")
                return None
            
            inspections_data = xml_data['Inspections']
            
            if 'Inspection' not in inspections_data:
                logging.error("No 'Inspection' elements found in XML data")
                return None
            
            inspections = inspections_data['Inspection']
            
            # Handle single inspection case (convert to list)
            if not isinstance(inspections, list):
                inspections = [inspections]
            
            logging.info(f"Extracted {len(inspections)} inspections from XML data")
            return inspections
            
        except Exception as e:
            logging.error(f"Error extracting inspections from XML data: {e}")
            return None
    
    def validate_inspection_structure(self, inspection: Dict[str, Any]) -> bool:
        """
        Validate that an inspection has required structure.
        
        Args:
            inspection: Single inspection data dictionary.
        
        Returns:
            Boolean indicating if structure is valid.
        """
        required_sections = ['InspMain', 'Drivers', 'Vehicles']
        
        for section in required_sections:
            if section not in inspection:
                logging.warning(f"Missing required section '{section}' in inspection")
                return False
        
        # Check InspMain structure
        if 'inspectionId' not in inspection['InspMain']:
            logging.warning("Missing 'inspectionId' in InspMain section")
            return False
        
        if 'InspectionPostDate' not in inspection['InspMain']:
            logging.warning("Missing 'InspectionPostDate' in InspMain section")
            return False
        
        # Check Drivers structure
        if 'Driver' not in inspection['Drivers']:
            logging.warning("Missing 'Driver' in Drivers section")
            return False
        
        driver = inspection['Drivers']['Driver']
        if 'DriverLastName' not in driver or 'DriverLicenseID' not in driver:
            logging.warning("Missing driver information (DriverLastName or DriverLicenseID)")
            return False
        
        # Check Vehicles structure
        if 'Vehicle' not in inspection['Vehicles']:
            logging.warning("Missing 'Vehicle' in Vehicles section")
            return False
        
        return True
    
    def get_inspection_summary(self, xml_data: Dict[str, Any]) -> Dict[str, int]:
        """
        Get summary statistics from XML data.
        
        Args:
            xml_data: Parsed XML data.
        
        Returns:
            Dictionary with summary statistics.
        """
        summary = {
            'total_inspections': 0,
            'valid_inspections': 0,
            'invalid_inspections': 0,
            'inspections_with_violations': 0
        }
        
        try:
            inspections = self.extract_inspections(xml_data)
            if not inspections:
                return summary
            
            summary['total_inspections'] = len(inspections)
            
            for inspection in inspections:
                if self.validate_inspection_structure(inspection):
                    summary['valid_inspections'] += 1
                    
                    # Check for violations
                    if 'Violations' in inspection and inspection['Violations']:
                        summary['inspections_with_violations'] += 1
                else:
                    summary['invalid_inspections'] += 1
            
            logging.info(f"XML Summary: {summary}")
            return summary
            
        except Exception as e:
            logging.error(f"Error generating XML summary: {e}")
            return summary 
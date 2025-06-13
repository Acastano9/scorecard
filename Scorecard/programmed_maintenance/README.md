# Programmed Maintenance - Excel File Processing

This folder is for **manually placing Excel files** containing programmed maintenance data. The system will automatically process any Excel files placed in this directory.

## File Placement Instructions

### 1. Download Excel Files
- Download maintenance Excel files from your management system
- Save them to your local computer first

### 2. Place Files in This Folder
- Copy/move the Excel files into this `programmed_maintenance` folder
- Multiple files can be processed in a single run
- Files are **not deleted** after processing

### 3. Supported File Formats
- `.xlsx` (Excel 2007+)
- `.xls` (Excel 97-2003)

## Expected Excel File Structure

### Required Columns
The Excel files must contain these columns (flexible naming):

| Column Purpose | Accepted Names |
|----------------|----------------|
| **Vehicle ID** | Vehicle ID, vehicle_id, VehicleID, Unit_ID, Truck_ID |
| **Maintenance Type** | Maintenance Type, maintenance_type, MaintenanceType, Service_Type, Work_Type |
| **Due Date** | Due Date, due_date, DueDate, Service_Due, Scheduled_Date |

### Optional Columns
Additional columns that will be processed if present:

| Column Purpose | Accepted Names |
|----------------|----------------|
| Vehicle Number | Vehicle Number, vehicle_number, VehicleNumber, Unit_Number, Truck_Number |
| Last Service | Last Service, last_service, LastService, Previous_Service, Last_Completed |
| Mileage | Mileage, mileage, Miles, Odometer, Current_Miles |
| Service Miles | Service Miles, service_miles, ServiceMiles, Next_Service_Miles |
| Status | Status, status, Service_Status, Maintenance_Status |
| Priority | Priority, priority, Urgency, Level |
| Location | Location, location, Terminal, Base, Yard |

### Example Excel Structure
```
Vehicle ID | Maintenance Type | Due Date   | Status    | Priority | Location
-----------|------------------|------------|-----------|----------|----------
T001       | Oil Change       | 2024-01-15 | Scheduled | High     | Terminal A
T002       | Brake Service    | 2024-01-20 | Overdue   | Critical | Terminal B
T003       | Tire Rotation    | 2024-01-25 | Completed | Medium   | Terminal A
```

## Processing Options

### 1. Automatic Processing (Recommended)
```bash
# Process all Excel files in this folder
python3 programmed_maintenance_etl.py
```

### 2. Interactive Mode
```bash
# Run with interactive menu
python3 programmed_maintenance_etl.py --interactive
```

### 3. Process with Analysis
```bash
# Process files and show detailed analysis
python3 programmed_maintenance_etl.py --analyze
```

### 4. Process Specific File
```bash
# Process only one specific file
python3 programmed_maintenance_etl.py --file "maintenance_report.xlsx"
```

### 5. Via Unified Processor
```bash
# Using the main ETL processor
python3 unified_etl_processor.py maintenance --directory programmed_maintenance
```

## Output and Results

### Database Storage
Processed data is stored in:
- **Table**: `VTUtility.dbo.Maintenance_Records`
- **Duplicate Prevention**: Based on Vehicle ID and Maintenance Type
- **Transaction Safety**: Rollback on errors

### Analysis Reports
When using `--analyze` flag, you'll see:
- Total maintenance items
- Overdue items count and percentage
- Status breakdown (Scheduled, Overdue, Completed, etc.)
- Priority breakdown (High, Medium, Low, Critical)
- Maintenance type distribution

### Logging
- **Log File**: `logs/programmed_maintenance_etl.log`
- **Console Output**: Real-time processing status
- **Error Tracking**: Detailed error messages for troubleshooting

## File Management

### Best Practices
1. **Use descriptive filenames** (e.g., `maintenance_2024_01.xlsx`)
2. **Check file format** before placing (must be Excel)
3. **Verify column headers** match expected names
4. **Remove empty rows** from Excel files
5. **Keep backups** of original files

### File Status
- Files remain in the folder after processing
- No automatic file deletion or moving
- Safe to process the same file multiple times (duplicates are prevented)

### Troubleshooting
If processing fails:
1. Check the log file: `logs/programmed_maintenance_etl.log`
2. Verify Excel file format and column names
3. Run in debug mode: `python3 programmed_maintenance_etl.py --debug`
4. Use interactive mode for step-by-step processing

## Integration with Platform

This module integrates with the main ETL platform:
- Uses shared utilities from `.utils/` folder
- Follows platform logging and error handling standards
- Compatible with unified processor interface
- Database integration with existing tables

## Support

For issues or questions:
1. Check log files for detailed error messages
2. Run debug mode to test components
3. Verify database connectivity
4. Ensure Excel file format compliance

---

**Note**: This replaces the previous win32com automation. Users now manually download and place Excel files instead of automated downloading. 
# DOT Inspections ETL

A modular Python ETL application for processing FMCSA DOT inspection XML files and importing them into a SQL Server database.

## Project Structure

```
dot_inspections_etl/
├── .utils/                          # Utility modules
│   ├── __init__.py                  # Package initialization
│   ├── config_utils.py              # Configuration management
│   ├── database_utils.py            # Database operations
│   ├── xml_processor_utils.py       # XML parsing utilities
│   ├── inspection_processor_utils.py # Inspection data processing
│   └── file_utils.py                # File management utilities
├── data/                            # Data directories
│   ├── xml_files/                   # Input XML files
│   ├── processed/                   # Successfully processed files
│   └── errors/                      # Files that caused errors
├── logs/                            # Log files
├── dot_inspections_etl.py           # Main ETL script
├── requirements.txt                 # Project dependencies
└── README.md                        # This file
```

## Features

### Modular Architecture

- **DOTConfigManager**: Centralized configuration management
- **DOTDatabaseManager**: Database connections and operations with context management
- **XMLProcessor**: XML file parsing and validation
- **InspectionProcessor**: Business logic for processing inspection data
- **FileManager**: File operations and directory management

### Processing Capabilities

- **Single File Processing**: Process individual XML files
- **Batch Directory Processing**: Process all XML files in a directory
- **Interactive Mode**: User-friendly menu-driven interface
- **Debug Mode**: Component testing and troubleshooting

### Data Processing Features

- XML parsing and validation
- Vehicle data processing (tractors and trailers)
- Violations data extraction and formatting
- Driver lookup by license number
- Duplicate inspection detection
- Comprehensive error handling and logging

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables in your `.env` file:
```env
ServerName=your_sql_server_name
dbUser=your_database_username
dbpassword=your_database_password
datab=your_database_name
```

## Database Schema

The application expects the following database tables:

### DOT_Inspections Table
```sql
CREATE TABLE [VTUtility].[dbo].[DOT_Inspections] (
    [inspection_id] INT PRIMARY KEY,
    [post_date] DATE,
    [driver_name] VARCHAR(255),
    [tractor_id] VARCHAR(50),
    [tractor_license] VARCHAR(50),
    [trailer_id] VARCHAR(50),
    [trailer_license] VARCHAR(50),
    [violations] TEXT,
    [driver_id] VARCHAR(50)
);
```

### Driver Table
```sql
-- Expected in lme_prod.dbo.driver
-- Used for driver ID lookup by license number
```

### Script_status Table
```sql
CREATE TABLE [VTUtility].[dbo].[Script_status] (
    [ID] INT,
    [result] BIT,
    [comments] VARCHAR(MAX),
    [Last_execution] DATETIME
);
```

## Usage

### Command Line Usage

```bash
# Process single XML file
python dot_inspections_etl.py -f inspections.xml

# Process all XML files in a directory
python dot_inspections_etl.py -d /path/to/xml/files

# Process files in default directory
python dot_inspections_etl.py -d dot_inspections_etl/data/xml_files

# Interactive mode
python dot_inspections_etl.py --interactive

# Debug mode
python dot_inspections_etl.py --debug

# Help
python dot_inspections_etl.py --help
```

### Interactive Mode

Run without arguments or with `--interactive` flag:

```bash
python dot_inspections_etl.py --interactive
```

This provides a menu-driven interface with options for:
- Processing single files
- Processing directories
- Viewing file information
- Debug mode testing

### File Processing Workflow

1. **File Validation**: Checks file existence and XML format
2. **XML Parsing**: Parses XML and extracts inspection data
3. **Data Processing**: Processes vehicles, violations, and driver information
4. **Database Operations**: 
   - Checks for existing inspections (prevents duplicates)
   - Looks up driver IDs by license number
   - Inserts new inspection records
5. **File Management**: Moves processed files to appropriate directories
6. **Status Updates**: Updates script execution status in database

## XML File Format

The application expects XML files with the following structure:

```xml
<Inspections>
    <Inspection>
        <InspMain>
            <inspectionId>12345</inspectionId>
            <InspectionPostDate>2024-01-15</InspectionPostDate>
        </InspMain>
        <Drivers>
            <Driver>
                <DriverLastName>Smith</DriverLastName>
                <DriverLicenseID>D123456789</DriverLicenseID>
            </Driver>
        </Drivers>
        <Vehicles>
            <Vehicle>
                <VehicleUnitTypeCode>TRACTOR</VehicleUnitTypeCode>
                <VehicleCompanyID>T001</VehicleCompanyID>
                <VehicleLicenseID>ABC123</VehicleLicenseID>
            </Vehicle>
        </Vehicles>
        <Violations>
            <Violation>
                <FedVioCode>392.2</FedVioCode>
                <ViolationCategory>Driver</ViolationCategory>
                <SectionDesc>Ill or fatigued driver</SectionDesc>
            </Violation>
        </Violations>
    </Inspection>
</Inspections>
```

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `ServerName` | SQL Server instance | `localhost` |
| `dbUser` | Database username | `sa` |
| `dbpassword` | Database password | `password123` |
| `datab` | Database name | `VTUtility` |

### Processing Configuration

The application includes configurable settings for:
- Company ID for driver lookup (default: 'TMS')
- Batch processing size
- Maximum retry attempts
- File backup options

## Error Handling

### File Processing Errors

- **Invalid XML files**: Moved to `data/errors/` directory
- **Missing required data**: Logged and skipped
- **Driver not found**: Logged but processing continues
- **Database errors**: Rolled back with detailed logging

### Logging

- **Console output**: Real-time processing information
- **Log files**: Stored in `logs/` directory
- **Error logs**: Created for failed files with error details

## Monitoring and Status

### Processing Results

Each processing operation returns detailed results:

```python
{
    'total_inspections': 150,
    'processed_inspections': 145,
    'skipped_existing': 20,
    'driver_not_found': 5,
    'inserted_successfully': 120,
    'errors': 0
}
```

### Database Status Updates

- Script execution status tracked in `Script_status` table
- Success/failure status with timestamps
- Error messages for troubleshooting

## Development

### Adding New Features

1. Create utility modules in `.utils/`
2. Implement business logic in processor classes
3. Update main ETL class to use new functionality
4. Add command line options if needed

### Testing

Use debug mode to test components without processing data:

```bash
python dot_inspections_etl.py --debug
```

Debug mode tests:
- Database connectivity
- File discovery
- XML parsing (if files available)
- Component initialization

## Best Practices

- **File Management**: Keep XML files organized in the `data/xml_files/` directory
- **Error Resolution**: Check error logs in `data/errors/` for failed files
- **Regular Cleanup**: Use file cleanup utilities to manage processed files
- **Database Monitoring**: Monitor script status table for execution history
- **Backup Strategy**: Processed files are automatically backed up

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Check environment variables
   - Verify database server connectivity
   - Confirm database permissions

2. **XML Parsing Errors**
   - Validate XML file format
   - Check file encoding
   - Verify XML structure matches expected format

3. **Driver Not Found**
   - Verify license numbers in source data
   - Check driver table for matching records
   - Confirm company_id configuration

4. **File Access Errors**
   - Check file permissions
   - Verify directory access
   - Ensure sufficient disk space

### Debug Mode

Use debug mode for detailed component testing:

```bash
python dot_inspections_etl.py --debug
```

This provides:
- Database connection testing
- File discovery verification
- XML parsing validation
- Component initialization checks

## Dependencies

- `pyodbc`: SQL Server database connectivity
- `python-dotenv`: Environment variable management
- `xmltodict`: XML parsing and conversion

## Security Considerations

- Environment variables for sensitive configuration
- SQL injection prevention in database operations
- File path validation and sanitization
- Error message sanitization for logs 
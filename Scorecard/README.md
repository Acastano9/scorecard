# Multi-Source Data Processing Platform

A comprehensive ETL platform for transportation management data processing, supporting multiple data sources with shared utilities for maximum code reuse and maintainability.

## Overview

This platform processes five key data sources for transportation operations:

1. **Netradyne Driver Scores** - Monthly driver safety scores from API/web/files
2. **DOT Inspections** - Monthly DOT inspection reports from XML files
3. **HOS Violations** - Hours of Service violations from JSON/Excel reports (NEW!)
4. **Score Card** - Driver performance evaluation metrics
5. **Programmed Maintenance** - Daily maintenance data with Excel file support and manual file placement workflow

## Architecture

```
scorecard/
├── .utils/                          # Shared utilities (90% code reuse)
│   ├── config_utils.py             # Configuration management
│   ├── database_utils.py           # Database operations
│   ├── file_processing_utils.py    # File processing
│   ├── netradyne_api_utils.py      # Netradyne API client
│   ├── netradyne_scraper_utils.py  # Web scraping
│   ├── hos_violations_utils.py     # HOS violations processing (JSON/Excel)
│   ├── scorecard_utils.py          # Scorecard processing
│   └── maintenance_utils.py        # Maintenance processing
├── data/                           # Netradyne data files
├── dot_inspections_etl/            # DOT inspections module
├── hos_violations_data/            # HOS violations data (NEW!)
├── scorecard_data/                 # Scorecard data files
├── maintenance_data/               # Maintenance data files
├── programmed_maintenance/         # Programmed maintenance Excel files (NEW!)
├── logs/                           # Application logs
├── netradyne_api_gz.py            # Netradyne API script
├── netradyne_green_zone.py        # Netradyne web scraping script
├── hos_violations_etl.py          # HOS violations ETL script (NEW!)
├── programmed_maintenance_etl.py  # Programmed maintenance ETL script (NEW!)
├── unified_etl_processor.py       # Unified interface for all data sources
└── README.md
```

## Quick Start

### Prerequisites

```bash
pip install pandas pyodbc python-dotenv requests beautifulsoup4 lxml openpyxl
```

### Environment Configuration

Create `.env` file with database credentials:
```bash
ServerName=your_server
dbUser=your_username
dbpassword=your_password
```

### Basic Usage

```bash
# Process all data sources interactively
python3 unified_etl_processor.py --interactive

# Process specific data source
python3 unified_etl_processor.py netradyne --method api
python3 unified_etl_processor.py hos_violations --directory hos_violations_data
python3 unified_etl_processor.py --debug
```

## Data Sources

### 1. Netradyne Driver Scores

Monthly driver safety score processing with multiple input methods.

**Processing Methods:**
- **API**: Direct API integration
- **Web Scraping**: Automated web portal download
- **File Processing**: Local/network file processing

**Usage:**
```bash
# API method
python3 netradyne_api_gz.py
python3 unified_etl_processor.py netradyne --method api

# Web scraping method  
python3 netradyne_green_zone.py
python3 unified_etl_processor.py netradyne --method scraper

# File processing method
python3 unified_etl_processor.py netradyne --method files --directory data/
```

**Features:**
- Token-based authentication
- Previous month data retrieval
- Duplicate score prevention
- Comprehensive error handling
- Status tracking in database

### 2. DOT Inspections

Monthly DOT inspection report processing from XML files.

**Usage:**
```bash
# Interactive mode
python3 dot_inspections_etl/dot_inspections_etl.py --interactive

# Process specific file
python3 dot_inspections_etl/dot_inspections_etl.py --file inspections.xml

# Process directory
python3 dot_inspections_etl/dot_inspections_etl.py --directory xml_files/

# Debug mode
python3 dot_inspections_etl/dot_inspections_etl.py --debug
```

**Features:**
- XML validation and parsing
- Schema compliance checking
- Error file management
- Batch processing
- Comprehensive logging

### 3. HOS Violations (NEW!)

Hours of Service violations processing supporting both JSON and Excel formats.

**Supported Formats:**
- **JSON**: Direct API exports with detailed violation data
- **Excel**: Monthly violation reports with flexible column mapping

**Usage:**
```bash
# Process all files in directory
python3 hos_violations_etl.py

# Process specific JSON file
python3 hos_violations_etl.py --file violations.json

# Process specific Excel file
python3 hos_violations_etl.py --file violations.xlsx

# Process with analysis
python3 hos_violations_etl.py --analyze

# Custom directory
python3 hos_violations_etl.py --directory custom_hos_data/

# Interactive mode
python3 hos_violations_etl.py --interactive

# Debug mode
python3 hos_violations_etl.py --debug

# Via unified processor
python3 unified_etl_processor.py hos_violations --file violations.json
python3 unified_etl_processor.py hos_violations --directory hos_violations_data --analyze
```

**JSON Format Support:**
- Direct parsing from API exports
- Handles ISO format timestamps
- Supports nested violation data
- Flexible field mapping

**Excel Format Support:**
- Intelligent column detection
- Multiple naming conventions
- Date format handling
- Missing data management

**Analysis Features:**
- Driver violation summaries
- Violation type distribution
- Terminal-based statistics
- Date range analysis
- Top violators identification

**Interactive Options:**
1. Process all files in default directory
2. Process files in custom directory
3. Process specific file (JSON/Excel)
4. Process with analysis
5. View current database summary

### 4. Score Card

Driver performance evaluation with calculated metrics.

**Usage:**
```bash
python3 unified_etl_processor.py scorecard --directory scorecard_data/
```

**Features:**
- Performance metrics calculation
- Driver ranking and scoring
- Multiple evaluation criteria
- Trend analysis support

### 5. Programmed Maintenance

Daily maintenance data processing with Excel file support and manual file placement workflow.

**Processing Methods:**
- **Manual Excel Files**: Users manually place Excel files in `programmed_maintenance/` folder
- **Legacy Directory**: Process files from `maintenance_data/` folder  
- **Email Attachments**: Process maintenance data from email attachments (if supported)

**Usage:**
```bash
# Process Excel files from programmed_maintenance folder (recommended)
python3 programmed_maintenance_etl.py
python3 unified_etl_processor.py maintenance --directory programmed_maintenance

# Process files from legacy maintenance_data folder
python3 unified_etl_processor.py maintenance --directory maintenance_data

# Interactive mode with sub-options
python3 programmed_maintenance_etl.py --interactive

# Process specific Excel file
python3 programmed_maintenance_etl.py --file maintenance_report.xlsx

# Process with analysis
python3 programmed_maintenance_etl.py --analyze

# Debug mode
python3 programmed_maintenance_etl.py --debug

# Via unified processor
python3 unified_etl_processor.py maintenance --directory programmed_maintenance
```

**Excel File Requirements:**
- **Required Columns**: Vehicle ID, Maintenance Type, Due Date
- **Optional Columns**: Vehicle Number, Last Service, Mileage, Status, Priority, Location
- **Supported Formats**: .xlsx, .xls
- **Flexible Naming**: Intelligent column detection with multiple naming conventions

**Workflow:**
1. **Download Excel files** from your maintenance management system
2. **Place files** in the `programmed_maintenance/` folder
3. **Run processing script** or use interactive mode
4. **View results** and analysis (optional)

**Features:**
- Manual file placement (replaces win32com automation)
- Excel format validation and processing
- Maintenance schedule tracking
- Overdue items identification
- Priority and status analysis
- Batch processing with duplicate prevention
- Comprehensive error handling and logging

**Analysis Features:**
- Total maintenance items count
- Overdue items tracking with percentages
- Status breakdown (Scheduled, Overdue, Completed, etc.)
- Priority distribution (High, Medium, Low, Critical)
- Maintenance type categorization

## Unified Interface

The `unified_etl_processor.py` provides a single entry point for all data sources:

```bash
# Interactive mode with menu
python3 unified_etl_processor.py --interactive

# Command line processing
python3 unified_etl_processor.py <data_source> [options]

# Debug mode for all components
python3 unified_etl_processor.py --debug
```

**Interactive Menu Features:**
- Source selection with sub-options
- Real-time processing feedback
- Error handling and reporting
- Database status checking
- Analysis and reporting

## Database Integration

### Tables

- `VTUtility.dbo.Driver_Scores` - Netradyne driver scores
- `VTUtility.dbo.DOT_Inspections` - DOT inspection records
- `VTUtility.dbo.HOS_Violations` - HOS violation records (NEW!)
- `VTUtility.dbo.Scorecards` - Performance scorecards
- `VTUtility.dbo.Maintenance_Records` - Maintenance data
- `VTUtility.dbo.Script_status` - Processing status tracking

### HOS Violations Schema (NEW!)

```sql
CREATE TABLE VTUtility.dbo.HOS_Violations (
    ID NVARCHAR(255) PRIMARY KEY,
    Start_Time_and_Driver NVARCHAR(500),
    Driver_ID NVARCHAR(50),
    Driver_Name NVARCHAR(255),
    Violation_Start_Time DATETIME,
    Violation_End_Time DATETIME,
    Driver_Status NVARCHAR(100),
    Terminal NVARCHAR(100),
    Ruleset NVARCHAR(100),
    Violation_Type NVARCHAR(200),
    Violation_Duration NVARCHAR(50)
);
```

### Features

- **Duplicate Prevention**: ID-based uniqueness checks
- **Batch Processing**: Optimized bulk inserts (1000 records/batch)
- **Transaction Safety**: Rollback on errors
- **Status Tracking**: Success/failure logging
- **Performance Optimization**: Minimal database connections

## Error Handling & Logging

### Logging Strategy
- **File Logging**: Separate log files per data source
- **Console Output**: Real-time processing feedback
- **Debug Mode**: Detailed component testing
- **Error Isolation**: Failed records don't stop batch processing

### Error Management
- **Input Validation**: File existence and format checks
- **Data Validation**: Required field verification
- **Database Errors**: Connection and constraint handling
- **Recovery**: Partial success scenarios

## Key Features

### Code Reuse
- **90% Shared Code**: Core utilities used across all sources
- **Consistent Patterns**: Standardized error handling and logging
- **Modular Design**: Easy addition of new data sources
- **Single Maintenance Point**: Core functionality updates once

### Performance
- **Batch Processing**: 1000-record database batches
- **Parallel Processing**: Multiple file support
- **Memory Efficiency**: Streaming large datasets
- **Connection Pooling**: Optimized database usage

### Reliability  
- **Duplicate Prevention**: ID-based uniqueness
- **Transaction Safety**: Rollback on failures
- **Status Tracking**: Database status logging
- **Comprehensive Testing**: Debug mode for all components

### Security
- **Environment Variables**: Secure credential storage
- **SQL Injection Prevention**: Parameterized queries
- **Input Sanitization**: Safe data handling
- **Access Control**: Database permission based

## Development Guide

### Adding New Data Sources

1. **Create Processor**: Add new utility in `.utils/`
2. **Database Support**: Add methods to `DatabaseManager`
3. **Update Unified**: Add to `unified_etl_processor.py`
4. **Create Script**: Optional standalone script
5. **Update Documentation**: Add to README

### Testing

```bash
# Test individual components
python3 netradyne_api_gz.py --debug
python3 hos_violations_etl.py --debug
python3 dot_inspections_etl/dot_inspections_etl.py --debug

# Test unified processor
python3 unified_etl_processor.py --debug

# Test specific functionality
python3 unified_etl_processor.py hos_violations --file test_violations.json --analyze
```

### Code Quality

- **Type Hints**: Full type annotation support
- **Documentation**: Comprehensive docstrings
- **Error Handling**: Graceful failure modes
- **Logging**: Detailed operation tracking
- **Testing**: Debug modes for all components

## Recent Updates

### HOS Violations Enhancement (Latest)

**New Capabilities:**
- **Dual Format Support**: Both JSON and Excel processing
- **Enhanced Analysis**: Comprehensive violation statistics
- **Improved Database Operations**: Batch processing with duplicate prevention
- **Flexible Column Mapping**: Handles various Excel formats
- **Rich Interactive Mode**: Multiple processing options
- **Standalone Script**: Full-featured `hos_violations_etl.py`

**Data Model:**
- Structured `HOSViolation` dataclass
- ISO datetime support
- Flexible field mapping
- Database-ready tuple conversion

**Performance Improvements:**
- 1000-record batch processing
- Duplicate detection optimization
- Memory-efficient processing
- Transaction safety

## Support

For issues or questions:
1. Check debug mode output: `--debug`
2. Review log files in `logs/` directory
3. Verify database connectivity
4. Test with sample data files

## License

Internal use only - Transportation management system. 
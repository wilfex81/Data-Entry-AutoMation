# Medical Provider Data Entry Automation

A comprehensive system for validating, processing, and analyzing medical provider data. This system automates the data entry process by validating provider information, submitting it to an API, and providing analytical insights into the processed data.

## Features

- **Data Validation**: Validates medical provider data against a defined schema and business rules
- **API Integration**: Submits validated provider data to an external API in batches
- **SQLite Database**: Stores validated provider data in a SQLite database located in the data folder
- **Data Analysis**: Analyzes processed data for duplicate entries, trends, and inconsistencies
- **Reporting**: Generates reports on validation failures and data analysis results
- **Structured Storage**: Maintains database files in the data folder and logs in the logs folder
- **Demo Mode**: Provides a dedicated demo environment that doesn't affect production data

## Requirements

- Python 3.7+
- Required packages listed in `requirements.txt`

## Getting Started

### Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/Data-Entry-AutoMation.git
cd Data-Entry-AutoMation
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Create a `.env` file for configuration:

```bash
# API Configuration
API_BASE_URL=https://api.medicaldashboard.example.com/v1
API_KEY=your_api_key_here

# Logging Configuration - Logs will be stored in the logs directory
LOG_LEVEL=INFO
```

### Application Structure

The application uses a structured organization:
- `/data`: Contains database files and other data resources
  - `medical_providers.db`: Production database
  - `demo_medical_providers.db`: Demo database (created when running in demo mode)
  - `providers.json`: Sample provider data 
- `/logs`: Contains all application logs with daily rotation
- `/src`: Source code modules
- `/tests`: Test files and sample data

### Running the Demo

The easiest way to see the system in action is to run the demo script:

```bash
python demo_db.py
```

This will:
- Create a demo database file (`demo_medical_providers.db`) in the `/data` directory
- Load and process sample data from the data directory
- Store validated provider records in the demo database
- Demonstrate database operations like searching, updating, and retrieving records

The demo database is completely separate from the production database, allowing you to experiment without affecting production data.

## Usage

### Processing Data Files

Process a data file with validation, API submission, and analysis:

```bash
python -m src.main --input tests/sample_data.json
```

This will:
- Validate all provider records
- Submit valid records to the API
- Generate an analysis report and list of validation failures
- Store logs in the logs directory

### Storing Data in SQLite Database

To enable database storage while processing data:

```bash
python -m src.main --input tests/sample_data.json --use-db
```

By default, the production database will be created in the data directory at `data/medical_providers.db`.

### Demo Mode

To run the application in demo mode with an isolated database:

```bash
python -m src.main --input tests/sample_data.json --use-db --demo
```

This will use a separate database file (`data/demo_medical_providers.db`) that won't affect your production data.

You can also specify a custom database path:

```bash
python -m src.main --input tests/sample_data.json --use-db --db-path /custom/path/to/database.db
```

Options:
- `--use-db`: Enable database storage
- `--demo`: Run in demo mode with isolated database
- `--db-path`: Specify custom path for SQLite database file (optional)

### Database Operations

Once you have data in the database, you can perform these operations:

#### List All Providers

```bash
python -m src.main --use-db --list-all-providers
```

In demo mode:
```bash
python -m src.main --use-db --demo --list-all-providers
```

#### Search Providers

Search by provider type:
```bash
python -m src.main --use-db --search-db '{"provider_type":"hospital"}'
```

Search by state:
```bash
python -m src.main --use-db --search-db '{"state":"NY"}'
```

#### Get Provider Details

```bash
python -m src.main --use-db --get-provider <provider-id>
```

#### List Validation Failures

```bash
python -m src.main --use-db --list-validation-failures
```

#### Delete a Provider

```bash
python -m src.main --use-db --delete-provider <provider-id>
```

### Additional Command Line Options

```
--input                Path to input file (CSV, JSON, or Excel)
--batch-size           Batch size for API submissions (default: 50)
--failures-output      Path to export validation failures (default: validation_failures.json)
--analysis-output      Path to export data analysis report (default: data_analysis_report.json)
--help                 Show all available options
```

## Data Format

The system accepts data in the following formats:
- JSON (recommended)
- CSV
- Excel (.xlsx, .xls)

### Expected JSON Format

```json
[
  {
    "provider_name": "Metro Hospital",
    "provider_type": "hospital",
    "address": {
      "street1": "123 Main St",
      "street2": "Suite 100",
      "city": "New York",
      "state": "NY",
      "zip_code": "10001",
      "country": "USA"
    },
    "contact_info": {
      "phone_number": "+1-212-555-0123",
      "fax": "+1-212-555-0124",
      "email": "info@metrohospital.example.com",
      "website": "https://www.metrohospital.example.com"
    },
    "services": ["primary_care", "emergency", "surgery"],
    "accreditations": [
      {
        "organization": "Joint Commission",
        "license_number": "JC-12345",
        "issue_date": "2020-01-01",
        "expiration_date": "2025-01-01"
      }
    ],
    "specialties": ["Cardiology", "Neurology", "Orthopedics"],
    "languages": ["English", "Spanish", "Mandarin"],
    "insurance_accepted": ["Medicare", "Medicaid", "Blue Cross", "Aetna"]
  }
]
```

## SQLite Database Schema

The system uses a normalized SQLite database schema:

- **providers**: Core provider information
- **addresses**: Provider address details
- **contact_info**: Provider contact methods
- **accreditations**: Provider licenses and credentials
- **services**: Available service categories
- **provider_services**: Many-to-many relationship between providers and services
- **specialties**, **languages**, **insurance_plans**: Reference tables for these attributes
- **validation_failures**: Records of validation failures with error details

## Project Structure

- `data/`: Database files and data resources
  - Production and demo databases
  - Provider data files
- `logs/`: Application log files with daily rotation
- `src/`: Source code
  - `api_client.py`: API communication layer
  - `data_analyzer.py`: Data analysis functionality
  - `data_models.py`: Pydantic data models
  - `data_validator.py`: Validation logic
  - `db_client.py`: SQLite database client
  - `main.py`: Application entry point with command-line interface
  - `utils/`: Utility modules
    - `logger.py`: Logging configuration
- `tests/`: Test files and sample data
- `demo_db.py`: Demo script to showcase database functionality

## Troubleshooting

**Database Connection Issues**:
- Production database is stored in `data/medical_providers.db`
- Demo database is stored in `data/demo_medical_providers.db` 
- If specifying a custom database path, ensure the directory exists
- Check file permissions if you get write errors

**Logging Issues**:
- All logs are stored in the `logs` directory with daily rotation
- Check permissions if logs aren't being written properly

**Data Validation Failures**:
- Examine validation_failures.json for detailed error information
- Common issues include missing required fields or incorrect data formats

**API Submission Errors**:
- Check your API credentials in the .env file
- Verify that the API endpoint is accessible

**Separating Demo and Production Data**:
- Use the `--demo` flag to run in demo mode with an isolated database
- Without this flag, the system uses the production database

## License

This project is licensed under the MIT License.
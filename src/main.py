#!/usr/bin/env python3
import argparse
import json
import os
import sys
import pandas as pd
from typing import List, Dict, Any, Optional

from src.data_models import MedicalProvider, ServiceCategory, Address, ContactInfo, Accreditation
from src.data_validator import DataValidator
from src.api_client import APIClient
from src.db_client import DatabaseClient
from src.data_analyzer import DataAnalyzer
from src.utils.logger import app_logger

class DataEntryAutomation:
    def __init__(self, use_db=True, sqlite_path=None, is_demo=False):
        self.validator = DataValidator()
        self.api_client = APIClient()
        self.analyzer = DataAnalyzer()
        self.processed_providers = []
        self.validation_failures = []
        self.use_db = use_db
        self.is_demo = is_demo
        
        app_logger.info("Data Entry Automation initialized")
        
        if use_db:
            # If no path specified, use default path in data directory
            if sqlite_path is None:
                # Set up appropriate paths for demo or production
                data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
                os.makedirs(data_dir, exist_ok=True)
                
                if is_demo:
                    sqlite_path = os.path.join(data_dir, "demo_medical_providers.db")
                    app_logger.info(f"Using demo database at: {sqlite_path}")
                else:
                    sqlite_path = os.path.join(data_dir, "medical_providers.db")
                    app_logger.info(f"Using production database at: {sqlite_path}")
                    
            self.db_client = DatabaseClient(sqlite_path=sqlite_path)
            self.db_client.create_tables()
            app_logger.info(f"Connected to database for data storage at: {sqlite_path}")
    
    def load_data(self, input_file: str) -> List[Dict[str, Any]]:
        app_logger.info(f"Loading data from: {input_file}")
        
        if not os.path.exists(input_file):
            app_logger.error(f"File not found: {input_file}")
            raise FileNotFoundError(f"File not found: {input_file}")
        
        # Determine file type by extension
        _, ext = os.path.splitext(input_file.lower())
        
        if ext == '.json':
            with open(input_file, 'r') as f:
                data = json.load(f)
        elif ext == '.csv':
            data = pd.read_csv(input_file).to_dict(orient='records')
        elif ext in ['.xlsx', '.xls']:
            data = pd.read_excel(input_file).to_dict(orient='records')
        else:
            app_logger.error(f"Unsupported file format: {ext}")
            raise ValueError(f"Unsupported file format: {ext}")
        
        app_logger.info(f"Loaded {len(data)} records from {input_file}")
        return data
    
    def process_providers(self, providers_data: List[Dict[str, Any]], batch_size: int = 50) -> Dict[str, Any]:
        app_logger.info(f"Processing {len(providers_data)} provider records")
        
        valid_providers = []
        
        for idx, provider_data in enumerate(providers_data):
            app_logger.debug(f"Validating provider {idx+1}/{len(providers_data)}")
            
            # Map service strings to ServiceCategory enum values
            if "services" in provider_data and isinstance(provider_data["services"], list):
                try:
                    provider_data["services"] = [
                        ServiceCategory(s.lower().replace(" ", "_")) if isinstance(s, str) else s 
                        for s in provider_data["services"]
                    ]
                except ValueError as e:
                    app_logger.warning(f"Invalid service category in provider {provider_data.get('provider_name', 'Unknown')}: {str(e)}")
            
            # Validate provider data
            is_valid, errors, provider = self.validator.validate_provider(provider_data)
            
            if is_valid and provider:
                valid_providers.append(provider)
                app_logger.info(f"Validated provider: {provider.provider_name}")
            else:
                self.validation_failures.append({
                    "provider_name": provider_data.get("provider_name", "Unknown"),
                    "errors": errors
                })
                app_logger.warning(f"Validation failed for provider: {provider_data.get('provider_name', 'Unknown')}")
                
                # Also log the validation failure to the database if DB is enabled
                if self.use_db:
                    self.db_client.log_validation_failure(
                        provider_data.get("provider_name", "Unknown"),
                        errors,
                        provider_data
                    )
        
        results = {
            "total_providers": len(providers_data),
            "valid_providers": len(valid_providers),
            "validation_failures": len(self.validation_failures),
            "batches": [],
            "db_results": None
        }
        
        # Store in database if enabled
        if self.use_db and valid_providers:
            try:
                db_ids = self.db_client.add_providers_batch(valid_providers)
                results["db_results"] = {
                    "providers_stored": len(db_ids),
                    "provider_ids": db_ids
                }
                app_logger.info(f"Stored {len(db_ids)} providers in database")
            except Exception as e:
                app_logger.error(f"Error storing providers in database: {str(e)}")
                results["db_results"] = {
                    "error": str(e),
                    "providers_stored": 0
                }
        
        # Submit providers to API in batches
        if valid_providers:
            for i in range(0, len(valid_providers), batch_size):
                batch = valid_providers[i:i+batch_size]
                try:
                    response = self.api_client.submit_provider_batch(batch)
                    results["batches"].append({
                        "batch_id": response.get("batch_id"),
                        "count": len(batch),
                        "status": "submitted"
                    })
                    self.processed_providers.extend(batch)
                except Exception as e:
                    app_logger.error(f"Error submitting batch {i//batch_size + 1}: {str(e)}")
                    results["batches"].append({
                        "batch_index": i//batch_size + 1,
                        "count": len(batch),
                        "status": "failed",
                        "error": str(e)
                    })
        
        return results
    
    def analyze_processed_data(self) -> Dict[str, Any]:
        app_logger.info(f"Analyzing {len(self.processed_providers)} processed providers")
        
        if not self.processed_providers:
            app_logger.warning("No processed providers to analyze")
            return {}
        
        # Detect duplicates
        duplicates = self.analyzer.detect_duplicates(self.processed_providers)
        
        # Identify trends
        trends = self.analyzer.identify_trends(self.processed_providers)
        
        # Identify inconsistencies
        inconsistencies = self.analyzer.identify_inconsistencies(self.processed_providers)
        
        # Generate comprehensive report
        analysis_report = self.analyzer.generate_report()
        
        return analysis_report
    
    def export_validation_failures(self, output_file: str) -> None:
        app_logger.info(f"Exporting {len(self.validation_failures)} validation failures to {output_file}")
        
        if not self.validation_failures:
            app_logger.info("No validation failures to export")
            return
        
        # Determine file type by extension
        _, ext = os.path.splitext(output_file.lower())
        
        try:
            if ext == '.json':
                with open(output_file, 'w') as f:
                    json.dump(self.validation_failures, f, indent=2)
            elif ext == '.csv':
                # Flatten errors list for CSV format
                flat_failures = []
                for failure in self.validation_failures:
                    flat_failure = {
                        "provider_name": failure["provider_name"],
                        "errors": "; ".join(failure["errors"])
                    }
                    flat_failures.append(flat_failure)
                
                pd.DataFrame(flat_failures).to_csv(output_file, index=False)
            else:
                app_logger.error(f"Unsupported file format: {ext}")
                raise ValueError(f"Unsupported file format: {ext}")
            
            app_logger.info(f"Successfully exported validation failures to {output_file}")
        
        except Exception as e:
            app_logger.error(f"Error exporting validation failures: {str(e)}")
            raise
    
    # Database operations
    def get_all_providers_from_db(self) -> List[MedicalProvider]:
        """Get all providers from the database."""
        if not self.use_db:
            app_logger.warning("Database usage is disabled. Enable with use_db=True")
            return []
            
        return self.db_client.get_all_providers()
    
    def search_providers_in_db(self, criteria: Dict[str, Any]) -> List[MedicalProvider]:
        """Search for providers in the database based on criteria."""
        if not self.use_db:
            app_logger.warning("Database usage is disabled. Enable with use_db=True")
            return []
            
        return self.db_client.search_providers(criteria)
    
    def get_provider_from_db(self, provider_id: str) -> Optional[MedicalProvider]:
        """Get a provider from the database by ID."""
        if not self.use_db:
            app_logger.warning("Database usage is disabled. Enable with use_db=True")
            return None
            
        return self.db_client.get_provider(provider_id)
    
    def update_provider_in_db(self, provider_id: str, provider: MedicalProvider) -> bool:
        """Update a provider in the database."""
        if not self.use_db:
            app_logger.warning("Database usage is disabled. Enable with use_db=True")
            return False
            
        return self.db_client.update_provider(provider_id, provider)
    
    def delete_provider_from_db(self, provider_id: str) -> bool:
        """Delete a provider from the database."""
        if not self.use_db:
            app_logger.warning("Database usage is disabled. Enable with use_db=True")
            return False
            
        return self.db_client.delete_provider(provider_id)
    
    def get_validation_failures_from_db(self, limit: int = None) -> List[Dict]:
        """Get validation failures from the database."""
        if not self.use_db:
            app_logger.warning("Database usage is disabled. Enable with use_db=True")
            return []
            
        return self.db_client.get_validation_failures(limit)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Medical Provider Data Entry Automation')
    parser.add_argument('--input', help='Path to input file (CSV, JSON, or Excel)')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for API submissions')
    parser.add_argument('--failures-output', default='validation_failures.json', help='Path to export validation failures')
    parser.add_argument('--analysis-output', default='data_analysis_report.json', help='Path to export data analysis report')
    parser.add_argument('--use-db', action='store_true', help='Enable database storage')
    parser.add_argument('--db-path', help='Path to SQLite database file')
    parser.add_argument('--demo', action='store_true', help='Run in demo mode with isolated database')
    parser.add_argument('--search-db', help='Search providers in database (JSON criteria)')
    parser.add_argument('--get-provider', help='Get provider by ID from database')
    parser.add_argument('--delete-provider', help='Delete provider by ID from database')
    parser.add_argument('--list-all-providers', action='store_true', help='List all providers from database')
    parser.add_argument('--list-validation-failures', action='store_true', help='List validation failures from database')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # Initialize automation with appropriate settings
    automation = DataEntryAutomation(
        use_db=args.use_db,
        sqlite_path=args.db_path,
        is_demo=args.demo
    )
    
    try:
        # Database operations
        if args.use_db:
            if args.demo:
                print("Running in DEMO mode with isolated database")
                
            if args.search_db:
                try:
                    criteria = json.loads(args.search_db)
                    providers = automation.search_providers_in_db(criteria)
                    print(f"\nFound {len(providers)} providers matching search criteria:")
                    for p in providers:
                        print(f"- {p.provider_name} ({p.provider_type}) - {p.provider_id}")
                    return
                except json.JSONDecodeError:
                    print(f"Error: Search criteria must be valid JSON")
                    sys.exit(1)
            
            if args.get_provider:
                provider = automation.get_provider_from_db(args.get_provider)
                if provider:
                    print(f"\nProvider details for {provider.provider_name}:")
                    print(f"ID: {provider.provider_id}")
                    print(f"Type: {provider.provider_type}")
                    print(f"Address: {provider.address.street1}, {provider.address.city}, {provider.address.state}")
                    print(f"Phone: {provider.contact_info.phone_number}")
                    print(f"Services: {', '.join([s.value for s in provider.services])}")
                    return
                else:
                    print(f"Provider not found with ID {args.get_provider}")
                    sys.exit(1)
            
            if args.delete_provider:
                success = automation.delete_provider_from_db(args.delete_provider)
                if success:
                    print(f"Provider {args.delete_provider} deleted successfully")
                    return
                else:
                    print(f"Failed to delete provider {args.delete_provider}")
                    sys.exit(1)
            
            if args.list_all_providers:
                providers = automation.get_all_providers_from_db()
                print(f"\nAll providers in database ({len(providers)}):")
                for p in providers:
                    print(f"- {p.provider_name} ({p.provider_type}) - {p.provider_id}")
                return
                
            if args.list_validation_failures:
                failures = automation.get_validation_failures_from_db(limit=10)
                print(f"\nRecent validation failures ({len(failures)}):")
                for f in failures:
                    print(f"- {f['provider_name']}: {', '.join(f['errors'])}")
                return
        
        # Process data file if provided
        if args.input:
            # Load data from input file
            providers_data = automation.load_data(args.input)
            
            # Process providers
            results = automation.process_providers(providers_data, args.batch_size)
            
            # Export validation failures
            automation.export_validation_failures(args.failures_output)
            
            # Analyze processed data
            if automation.processed_providers:
                analysis = automation.analyze_processed_data()
                
                # Export analysis report
                with open(args.analysis_output, 'w') as f:
                    json.dump(analysis, f, indent=2)
                
                app_logger.info(f"Analysis report exported to {args.analysis_output}")
            
            # Print summary
            print(f"\nData Entry Automation Summary:")
            print(f"Total providers: {results['total_providers']}")
            print(f"Successfully validated: {results['valid_providers']}")
            print(f"Validation failures: {results['validation_failures']}")
            
            if args.use_db and results.get('db_results'):
                print(f"Providers stored in database: {results['db_results'].get('providers_stored', 0)}")
            
            print(f"Batches submitted to API: {len([b for b in results['batches'] if b.get('status') == 'submitted'])}")
            print(f"Failed batches: {len([b for b in results['batches'] if b.get('status') == 'failed'])}")
            print(f"\nValidation failures exported to: {args.failures_output}")
            
            if automation.processed_providers:
                print(f"Analysis report exported to: {args.analysis_output}")
        elif not any([args.search_db, args.get_provider, args.delete_provider, args.list_all_providers, args.list_validation_failures]):
            print("Error: No action specified. Please provide --input or a database operation.")
            args = parse_arguments()
            args.print_help()
            sys.exit(1)
        
    except Exception as e:
        app_logger.error(f"Error in main process: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
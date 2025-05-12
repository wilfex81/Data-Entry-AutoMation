#!/usr/bin/env python3
"""
This script demonstrates how to use the database functionality in the Data Entry Automation system.
It loads data from the data directory, stores it in the database, and performs various database operations.
"""

import json
import os
from src.main import DataEntryAutomation
import time
import shutil

def main():
    print("\n=== Medical Provider Database Demo ===\n")
    
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    automation = DataEntryAutomation(
        use_db=True,
        is_demo=True
    )
    print(f"Demo database initialized in the data directory")
    
    data_file = os.path.join(data_dir, "providers.json")
    
    if not os.path.exists(data_file):
        print(f"\nNo data file found at {data_file}.")
        print("Copying sample data to the data directory as a starting point...")
        
        sample_file = "tests/sample_data.json"
        shutil.copy(sample_file, data_file)
        print(f"Sample data copied to {data_file}")
    
    print(f"\nLoading data from {data_file}...")
    providers_data = automation.load_data(data_file)
    
    print(f"Processing {len(providers_data)} provider records...")
    results = automation.process_providers(providers_data)
    
    print(f"\nData Processing Results:")
    print(f"  Total providers: {results['total_providers']}")
    print(f"  Successfully validated: {results['valid_providers']}")
    print(f"  Validation failures: {results['validation_failures']}")
    
    if results.get('db_results'):
        print(f"  Providers stored in demo database: {results['db_results'].get('providers_stored', 0)}")
        provider_ids = results['db_results'].get('provider_ids', [])
        if provider_ids:
            print(f"  Provider IDs: {', '.join(provider_ids[:3])}{'...' if len(provider_ids) > 3 else ''}")
    
    print("\n--- Database Operations Demo ---\n")
    
    providers = automation.get_all_providers_from_db()
    print(f"All providers in demo database ({len(providers)}):")
    for p in providers:
        print(f"- {p.provider_name} ({p.provider_type}) - ID: {p.provider_id}")
    
    if not providers:
        print("No providers found in demo database.")
        return
    
    print("\n--- Get Provider By ID ---")
    sample_id = providers[0].provider_id
    provider = automation.get_provider_from_db(sample_id)
    if provider:
        print(f"\nProvider details for {provider.provider_name}:")
        print(f"  ID: {provider.provider_id}")
        print(f"  Type: {provider.provider_type}")
        print(f"  Address: {provider.address.street1}, {provider.address.city}, {provider.address.state}")
        print(f"  Phone: {provider.contact_info.phone_number}")
        print(f"  Services: {', '.join([s.value for s in provider.services])}")
    
    print("\n--- Search Providers ---")
    criteria = {"provider_type": "hospital"}
    results = automation.search_providers_in_db(criteria)
    print(f"\nSearch results for hospitals: {len(results)} found")
    for p in results:
        print(f"- {p.provider_name} ({p.provider_type})")
    
    criteria = {"state": "NY"}
    results = automation.search_providers_in_db(criteria)
    print(f"\nSearch results for NY providers: {len(results)} found")
    for p in results:
        print(f"- {p.provider_name} ({p.address.city}, {p.address.state})")
    
    print("\n--- Update Provider ---")
    if provider:
        if not provider.specialties:
            provider.specialties = ["Telemedicine"]
        else:
            provider.specialties.append("Telemedicine")
        
        success = automation.update_provider_in_db(provider.provider_id, provider)
        if success:
            print(f"Provider {provider.provider_name} updated with new specialty: Telemedicine")
            
            updated = automation.get_provider_from_db(provider.provider_id)
            if updated and updated.specialties:
                print(f"  Updated specialties: {', '.join(updated.specialties)}")
    
    if len(providers) > 1:
        print("\n--- Delete Provider ---")
        provider_to_delete = providers[-1]
        print(f"Deleting provider: {provider_to_delete.provider_name}")
        
        success = automation.delete_provider_from_db(provider_to_delete.provider_id)
        if success:
            print(f"Provider {provider_to_delete.provider_name} deleted successfully")
            
            remaining = automation.get_all_providers_from_db()
            print(f"Remaining providers: {len(remaining)}")
    
    print(f"\nDatabase demo completed! The demo database is stored in the data directory.")
    print("You can run this demo again to work with the same persistent demo database.")
    print(f"To add or modify provider data, edit the file at {data_file}")

if __name__ == "__main__":
    main()
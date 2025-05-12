"""
Database module for Medical Provider Data Entry Automation.
Handles storage of provider data, validation results, and historical records.
"""

import sqlite3
import json
import os
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path

from src.utils.logger import app_logger
from src.data_models import MedicalProvider, Address, ContactInfo, Accreditation, ServiceCategory

class DatabaseClient:
    """Database for storing medical provider information."""
    
    def __init__(self, sqlite_path=None):
        """Initialize the database connection."""
        # Use specified SQLite database file
        if sqlite_path:
            db_path = os.path.abspath(sqlite_path)
            app_logger.info(f"Using SQLite database at: {db_path}")
            # Ensure directory exists
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            self.conn = sqlite3.connect(db_path)
        else:
            # Use environment variable or default to SQLite in data folder
            db_path = os.getenv("DATABASE_URL")
            if not db_path or not db_path.startswith("sqlite:///"):
                # Default to SQLite in the data folder
                data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
                os.makedirs(data_dir, exist_ok=True)
                default_db_path = os.path.join(data_dir, "medical_providers.db")
                app_logger.info(f"DATABASE_URL not specified, using SQLite at: {default_db_path}")
                db_path = default_db_path
            else:
                # Extract file path from sqlite:/// URL
                db_path = db_path.replace("sqlite:///", "")
                
            app_logger.info(f"Connecting to SQLite database: {db_path}")
            self.conn = sqlite3.connect(db_path)
        
        # Enable foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON")
        
        # Enable column names in query results
        self.conn.row_factory = sqlite3.Row
        
    def create_tables(self):
        """Create necessary database tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Providers table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS providers (
            id TEXT PRIMARY KEY,
            provider_name TEXT NOT NULL,
            provider_type TEXT NOT NULL, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Addresses table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_id TEXT NOT NULL,
            street1 TEXT NOT NULL,
            street2 TEXT,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            zip_code TEXT NOT NULL,
            country TEXT DEFAULT 'USA',
            FOREIGN KEY (provider_id) REFERENCES providers(id) ON DELETE CASCADE
        )
        ''')
        
        # Contact info table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS contact_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_id TEXT NOT NULL,
            phone_number TEXT NOT NULL,
            fax TEXT,
            email TEXT,
            website TEXT,
            FOREIGN KEY (provider_id) REFERENCES providers(id) ON DELETE CASCADE
        )
        ''')
        
        # Accreditations table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS accreditations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_id TEXT NOT NULL,
            organization TEXT NOT NULL,
            license_number TEXT NOT NULL,
            issue_date TEXT NOT NULL,
            expiration_date TEXT NOT NULL,
            FOREIGN KEY (provider_id) REFERENCES providers(id) ON DELETE CASCADE
        )
        ''')
        
        # Services table - used for lookup/reference
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS services (
            name TEXT PRIMARY KEY,
            description TEXT
        )
        ''')
        
        # Provider-Services association table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS provider_services (
            provider_id TEXT NOT NULL,
            service_name TEXT NOT NULL,
            PRIMARY KEY (provider_id, service_name),
            FOREIGN KEY (provider_id) REFERENCES providers(id) ON DELETE CASCADE,
            FOREIGN KEY (service_name) REFERENCES services(name) ON DELETE CASCADE
        )
        ''')
        
        # Specialties table - used for lookup/reference
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS specialties (
            name TEXT PRIMARY KEY,
            description TEXT
        )
        ''')
        
        # Provider-Specialties association table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS provider_specialties (
            provider_id TEXT NOT NULL,
            specialty_name TEXT NOT NULL,
            PRIMARY KEY (provider_id, specialty_name),
            FOREIGN KEY (provider_id) REFERENCES providers(id) ON DELETE CASCADE,
            FOREIGN KEY (specialty_name) REFERENCES specialties(name) ON DELETE CASCADE
        )
        ''')
        
        # Languages table - used for lookup/reference
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS languages (
            name TEXT PRIMARY KEY
        )
        ''')
        
        # Provider-Languages association table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS provider_languages (
            provider_id TEXT NOT NULL,
            language_name TEXT NOT NULL,
            PRIMARY KEY (provider_id, language_name),
            FOREIGN KEY (provider_id) REFERENCES providers(id) ON DELETE CASCADE,
            FOREIGN KEY (language_name) REFERENCES languages(name) ON DELETE CASCADE
        )
        ''')
        
        # Insurance plans table - used for lookup/reference
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS insurance_plans (
            name TEXT PRIMARY KEY,
            description TEXT
        )
        ''')
        
        # Provider-Insurance association table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS provider_insurance (
            provider_id TEXT NOT NULL,
            insurance_name TEXT NOT NULL,
            PRIMARY KEY (provider_id, insurance_name),
            FOREIGN KEY (provider_id) REFERENCES providers(id) ON DELETE CASCADE,
            FOREIGN KEY (insurance_name) REFERENCES insurance_plans(name) ON DELETE CASCADE
        )
        ''')
        
        # Validation failures tracking table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS validation_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_name TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            errors TEXT NOT NULL,
            raw_data TEXT
        )
        ''')
        
        self.conn.commit()
        app_logger.info("Database tables created")
    
    def add_provider(self, provider: MedicalProvider) -> str:
        """Add a provider to the database."""
        cursor = self.conn.cursor()
        try:
            # Check if provider already exists
            cursor.execute(
                "SELECT id FROM providers WHERE provider_name = ?", 
                (provider.provider_name,)
            )
            existing = cursor.fetchone()
            if existing:
                app_logger.warning(f"Provider {provider.provider_name} already exists in database")
                return existing[0]
            
            # Generate provider ID if not provided
            provider_id = provider.provider_id or str(uuid.uuid4())
            
            # 1. Insert provider
            cursor.execute(
                "INSERT INTO providers (id, provider_name, provider_type) VALUES (?, ?, ?)",
                (provider_id, provider.provider_name, provider.provider_type)
            )
            
            # 2. Insert address
            cursor.execute(
                """
                INSERT INTO addresses 
                (provider_id, street1, street2, city, state, zip_code, country)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    provider_id, 
                    provider.address.street1, 
                    provider.address.street2, 
                    provider.address.city, 
                    provider.address.state, 
                    provider.address.zip_code, 
                    provider.address.country
                )
            )
            
            # 3. Insert contact info
            cursor.execute(
                """
                INSERT INTO contact_info
                (provider_id, phone_number, fax, email, website)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    provider_id, 
                    provider.contact_info.phone_number, 
                    provider.contact_info.fax, 
                    provider.contact_info.email, 
                    provider.contact_info.website
                )
            )
            
            # 4. Insert accreditations
            for accred in provider.accreditations:
                cursor.execute(
                    """
                    INSERT INTO accreditations
                    (provider_id, organization, license_number, issue_date, expiration_date)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        provider_id, 
                        accred.organization, 
                        accred.license_number, 
                        accred.issue_date, 
                        accred.expiration_date
                    )
                )
            
            # 5. Insert services
            for service in provider.services:
                service_name = service.value
                # Make sure the service exists in the services table
                cursor.execute(
                    "INSERT OR IGNORE INTO services (name) VALUES (?)",
                    (service_name,)
                )
                # Link the service to the provider
                cursor.execute(
                    "INSERT INTO provider_services (provider_id, service_name) VALUES (?, ?)",
                    (provider_id, service_name)
                )
            
            # 6. Insert specialties
            if provider.specialties:
                for specialty in provider.specialties:
                    # Make sure the specialty exists
                    cursor.execute(
                        "INSERT OR IGNORE INTO specialties (name) VALUES (?)",
                        (specialty,)
                    )
                    # Link to provider
                    cursor.execute(
                        "INSERT INTO provider_specialties (provider_id, specialty_name) VALUES (?, ?)",
                        (provider_id, specialty)
                    )
            
            # 7. Insert languages
            if provider.languages:
                for language in provider.languages:
                    # Make sure the language exists
                    cursor.execute(
                        "INSERT OR IGNORE INTO languages (name) VALUES (?)",
                        (language,)
                    )
                    # Link to provider
                    cursor.execute(
                        "INSERT INTO provider_languages (provider_id, language_name) VALUES (?, ?)",
                        (provider_id, language)
                    )
            
            # 8. Insert insurance plans
            if provider.insurance_accepted:
                for insurance in provider.insurance_accepted:
                    # Make sure the insurance exists
                    cursor.execute(
                        "INSERT OR IGNORE INTO insurance_plans (name) VALUES (?)",
                        (insurance,)
                    )
                    # Link to provider
                    cursor.execute(
                        "INSERT INTO provider_insurance (provider_id, insurance_name) VALUES (?, ?)",
                        (provider_id, insurance)
                    )
            
            self.conn.commit()
            app_logger.info(f"Provider {provider.provider_name} added to database with ID {provider_id}")
            return provider_id
            
        except Exception as e:
            self.conn.rollback()
            app_logger.error(f"Error adding provider {provider.provider_name} to database: {str(e)}")
            raise
    
    def add_providers_batch(self, providers: List[MedicalProvider]) -> List[str]:
        """Add multiple providers to the database in a batch."""
        provider_ids = []
        
        for provider in providers:
            try:
                provider_id = self.add_provider(provider)
                provider_ids.append(provider_id)
            except Exception as e:
                app_logger.error(f"Error adding provider {provider.provider_name} in batch: {str(e)}")
        
        app_logger.info(f"Added {len(provider_ids)} providers to database")
        return provider_ids
    
    def get_provider(self, provider_id: str) -> Optional[MedicalProvider]:
        """Get a provider from the database by ID."""
        cursor = self.conn.cursor()
        
        try:
            # 1. Get provider basic info
            cursor.execute(
                "SELECT * FROM providers WHERE id = ?",
                (provider_id,)
            )
            provider_row = cursor.fetchone()
            
            if not provider_row:
                app_logger.warning(f"Provider with ID {provider_id} not found in database")
                return None
                
            # 2. Get address
            cursor.execute(
                "SELECT * FROM addresses WHERE provider_id = ?",
                (provider_id,)
            )
            address_row = cursor.fetchone()
            
            # 3. Get contact info
            cursor.execute(
                "SELECT * FROM contact_info WHERE provider_id = ?",
                (provider_id,)
            )
            contact_row = cursor.fetchone()
            
            # 4. Get accreditations
            cursor.execute(
                "SELECT * FROM accreditations WHERE provider_id = ?",
                (provider_id,)
            )
            accreditation_rows = cursor.fetchall()
            
            # 5. Get services
            cursor.execute(
                "SELECT service_name FROM provider_services WHERE provider_id = ?",
                (provider_id,)
            )
            service_rows = cursor.fetchall()
            
            # 6. Get specialties
            cursor.execute(
                "SELECT specialty_name FROM provider_specialties WHERE provider_id = ?",
                (provider_id,)
            )
            specialty_rows = cursor.fetchall()
            
            # 7. Get languages
            cursor.execute(
                "SELECT language_name FROM provider_languages WHERE provider_id = ?",
                (provider_id,)
            )
            language_rows = cursor.fetchall()
            
            # 8. Get insurance plans
            cursor.execute(
                "SELECT insurance_name FROM provider_insurance WHERE provider_id = ?",
                (provider_id,)
            )
            insurance_rows = cursor.fetchall()
            
            # Construct address model
            address = Address(
                street1=address_row['street1'],
                street2=address_row['street2'],
                city=address_row['city'],
                state=address_row['state'],
                zip_code=address_row['zip_code'],
                country=address_row['country']
            )
            
            # Construct contact info model
            contact_info = ContactInfo(
                phone_number=contact_row['phone_number'],
                fax=contact_row['fax'],
                email=contact_row['email'],
                website=contact_row['website']
            )
            
            # Construct accreditations
            accreditations = []
            for row in accreditation_rows:
                accreditations.append(Accreditation(
                    organization=row['organization'],
                    license_number=row['license_number'],
                    issue_date=row['issue_date'],
                    expiration_date=row['expiration_date']
                ))
            
            # Construct services
            services = [ServiceCategory(row['service_name']) for row in service_rows]
            
            # Construct specialties, languages and insurance
            specialties = [row['specialty_name'] for row in specialty_rows] if specialty_rows else None
            languages = [row['language_name'] for row in language_rows] if language_rows else None
            insurance_accepted = [row['insurance_name'] for row in insurance_rows] if insurance_rows else None
            
            # Build and return the complete provider model
            provider = MedicalProvider(
                provider_id=provider_row['id'],
                provider_name=provider_row['provider_name'],
                provider_type=provider_row['provider_type'],
                address=address,
                contact_info=contact_info,
                services=services,
                accreditations=accreditations,
                specialties=specialties,
                languages=languages,
                insurance_accepted=insurance_accepted,
                created_at=provider_row['created_at'],
                updated_at=provider_row['updated_at']
            )
            
            return provider
            
        except Exception as e:
            app_logger.error(f"Error retrieving provider {provider_id}: {str(e)}")
            raise
    
    def search_providers(self, criteria: Dict[str, Any]) -> List[MedicalProvider]:
        """Search providers based on criteria."""
        try:
            query = """
            SELECT p.id 
            FROM providers p
            """
            
            params = []
            conditions = []
            
            # Join tables as needed based on criteria
            if 'state' in criteria or 'city' in criteria:
                query += " JOIN addresses a ON p.id = a.provider_id"
                
            # Build conditions
            if 'provider_name' in criteria:
                conditions.append("p.provider_name LIKE ?")
                params.append(f"%{criteria['provider_name']}%")
            
            if 'provider_type' in criteria:
                conditions.append("p.provider_type = ?")
                params.append(criteria['provider_type'])
            
            if 'state' in criteria:
                conditions.append("a.state = ?")
                params.append(criteria['state'])
            
            if 'city' in criteria:
                conditions.append("a.city LIKE ?")
                params.append(f"%{criteria['city']}%")
                
            # Add conditions to query
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
                
            # Execute the query
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            
            # Fetch all matching provider IDs
            provider_ids = [row['id'] for row in cursor.fetchall()]
            
            # Get full provider objects for each ID
            providers = [self.get_provider(pid) for pid in provider_ids]
            
            # Filter out any None results (in case a provider was deleted)
            providers = [p for p in providers if p is not None]
            
            app_logger.info(f"Found {len(providers)} providers matching search criteria")
            return providers
            
        except Exception as e:
            app_logger.error(f"Error searching providers: {str(e)}")
            raise
    
    def update_provider(self, provider_id: str, provider: MedicalProvider) -> bool:
        """Update an existing provider in the database."""
        cursor = self.conn.cursor()
        
        try:
            # Check if provider exists
            cursor.execute("SELECT id FROM providers WHERE id = ?", (provider_id,))
            if not cursor.fetchone():
                app_logger.warning(f"Provider with ID {provider_id} not found for update")
                return False
            
            # 1. Update provider basic info
            cursor.execute(
                """
                UPDATE providers 
                SET provider_name = ?, provider_type = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (provider.provider_name, provider.provider_type, provider_id)
            )
            
            # 2. Update or insert address
            cursor.execute("SELECT id FROM addresses WHERE provider_id = ?", (provider_id,))
            address_exists = cursor.fetchone()
            
            if address_exists:
                cursor.execute(
                    """
                    UPDATE addresses
                    SET street1 = ?, street2 = ?, city = ?, state = ?, zip_code = ?, country = ?
                    WHERE provider_id = ?
                    """,
                    (
                        provider.address.street1, 
                        provider.address.street2, 
                        provider.address.city, 
                        provider.address.state, 
                        provider.address.zip_code, 
                        provider.address.country, 
                        provider_id
                    )
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO addresses
                    (provider_id, street1, street2, city, state, zip_code, country)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        provider_id, 
                        provider.address.street1, 
                        provider.address.street2, 
                        provider.address.city, 
                        provider.address.state, 
                        provider.address.zip_code, 
                        provider.address.country
                    )
                )
            
            # 3. Update or insert contact info
            cursor.execute("SELECT id FROM contact_info WHERE provider_id = ?", (provider_id,))
            contact_exists = cursor.fetchone()
            
            if contact_exists:
                cursor.execute(
                    """
                    UPDATE contact_info
                    SET phone_number = ?, fax = ?, email = ?, website = ?
                    WHERE provider_id = ?
                    """,
                    (
                        provider.contact_info.phone_number,
                        provider.contact_info.fax,
                        provider.contact_info.email,
                        provider.contact_info.website,
                        provider_id
                    )
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO contact_info
                    (provider_id, phone_number, fax, email, website)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        provider_id, 
                        provider.contact_info.phone_number, 
                        provider.contact_info.fax, 
                        provider.contact_info.email, 
                        provider.contact_info.website
                    )
                )
            
            # 4. Delete existing accreditations and insert new ones
            cursor.execute("DELETE FROM accreditations WHERE provider_id = ?", (provider_id,))
            for accred in provider.accreditations:
                cursor.execute(
                    """
                    INSERT INTO accreditations
                    (provider_id, organization, license_number, issue_date, expiration_date)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        provider_id, 
                        accred.organization, 
                        accred.license_number, 
                        accred.issue_date, 
                        accred.expiration_date
                    )
                )
            
            # 5. Delete existing services and insert new ones
            cursor.execute("DELETE FROM provider_services WHERE provider_id = ?", (provider_id,))
            for service in provider.services:
                service_name = service.value
                cursor.execute(
                    "INSERT OR IGNORE INTO services (name) VALUES (?)",
                    (service_name,)
                )
                cursor.execute(
                    "INSERT INTO provider_services (provider_id, service_name) VALUES (?, ?)",
                    (provider_id, service_name)
                )
            
            # 6. Update specialties
            cursor.execute("DELETE FROM provider_specialties WHERE provider_id = ?", (provider_id,))
            if provider.specialties:
                for specialty in provider.specialties:
                    cursor.execute(
                        "INSERT OR IGNORE INTO specialties (name) VALUES (?)",
                        (specialty,)
                    )
                    cursor.execute(
                        "INSERT INTO provider_specialties (provider_id, specialty_name) VALUES (?, ?)",
                        (provider_id, specialty)
                    )
            
            # 7. Update languages
            cursor.execute("DELETE FROM provider_languages WHERE provider_id = ?", (provider_id,))
            if provider.languages:
                for language in provider.languages:
                    cursor.execute(
                        "INSERT OR IGNORE INTO languages (name) VALUES (?)",
                        (language,)
                    )
                    cursor.execute(
                        "INSERT INTO provider_languages (provider_id, language_name) VALUES (?, ?)",
                        (provider_id, language)
                    )
            
            # 8. Update insurance plans
            cursor.execute("DELETE FROM provider_insurance WHERE provider_id = ?", (provider_id,))
            if provider.insurance_accepted:
                for insurance in provider.insurance_accepted:
                    cursor.execute(
                        "INSERT OR IGNORE INTO insurance_plans (name) VALUES (?)",
                        (insurance,)
                    )
                    cursor.execute(
                        "INSERT INTO provider_insurance (provider_id, insurance_name) VALUES (?, ?)",
                        (provider_id, insurance)
                    )
            
            self.conn.commit()
            app_logger.info(f"Provider {provider_id} updated in database")
            return True
            
        except Exception as e:
            self.conn.rollback()
            app_logger.error(f"Error updating provider {provider_id} in database: {str(e)}")
            raise
    
    def delete_provider(self, provider_id: str) -> bool:
        """Delete a provider from the database."""
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT id FROM providers WHERE id = ?", (provider_id,))
            if not cursor.fetchone():
                app_logger.warning(f"Provider with ID {provider_id} not found for deletion")
                return False
            
            # With CASCADE enabled, deleting from providers table will delete all related records
            cursor.execute("DELETE FROM providers WHERE id = ?", (provider_id,))
            self.conn.commit()
            app_logger.info(f"Provider {provider_id} deleted from database")
            return True
            
        except Exception as e:
            self.conn.rollback()
            app_logger.error(f"Error deleting provider {provider_id} from database: {str(e)}")
            raise
    
    def get_all_providers(self) -> List[MedicalProvider]:
        """Get all providers from the database."""
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT id FROM providers")
            provider_ids = [row['id'] for row in cursor.fetchall()]
            
            providers = [self.get_provider(pid) for pid in provider_ids]
            providers = [p for p in providers if p is not None]
            
            app_logger.info(f"Retrieved {len(providers)} providers from database")
            return providers
            
        except Exception as e:
            app_logger.error(f"Error retrieving all providers from database: {str(e)}")
            raise
    
    def log_validation_failure(self, provider_name: str, errors: List[str], raw_data: Dict = None):
        """Log a validation failure to the database."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO validation_failures 
                (provider_name, errors, raw_data)
                VALUES (?, ?, ?)
                """,
                (
                    provider_name,
                    json.dumps(errors),
                    json.dumps(raw_data) if raw_data else None
                )
            )
            self.conn.commit()
            app_logger.info(f"Logged validation failure for provider {provider_name}")
            
        except Exception as e:
            self.conn.rollback()
            app_logger.error(f"Error logging validation failure: {str(e)}")
    
    def get_validation_failures(self, limit: int = None) -> List[Dict]:
        """Get validation failures from the database."""
        cursor = self.conn.cursor()
        try:
            query = "SELECT * FROM validation_failures ORDER BY timestamp DESC"
            if limit:
                query += f" LIMIT {int(limit)}"
                
            cursor.execute(query)
            failures = []
            
            for row in cursor.fetchall():
                failures.append({
                    'id': row['id'],
                    'provider_name': row['provider_name'],
                    'timestamp': row['timestamp'],
                    'errors': json.loads(row['errors']),
                    'raw_data': json.loads(row['raw_data']) if row['raw_data'] else None
                })
                
            return failures
            
        except Exception as e:
            app_logger.error(f"Error retrieving validation failures: {str(e)}")
            raise
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            app_logger.info("Database connection closed")
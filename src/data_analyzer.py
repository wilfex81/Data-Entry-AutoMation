import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Set, Tuple
import re
from collections import Counter
from difflib import SequenceMatcher
from src.data_models import MedicalProvider
from src.utils.logger import app_logger

class DataAnalyzer:
    def __init__(self):
        self.analysis_results = {}
    
    def detect_duplicates(self, providers: List[MedicalProvider], threshold: float = 0.85) -> List[Dict[str, Any]]:
        potential_duplicates = []
        
        # Convert providers to simpler dict for comparison
        providers_data = [self._convert_provider_to_dict(p) for p in providers]
        
        # Compare each provider with every other provider
        for i, provider1 in enumerate(providers):
            for j, provider2 in enumerate(providers):
                if i >= j:  # Skip self-comparison and already compared pairs
                    continue
                
                similarity_score = self._calculate_similarity(
                    providers_data[i], 
                    providers_data[j]
                )
                
                if similarity_score >= threshold:
                    potential_duplicates.append({
                        "provider1_id": provider1.provider_id,
                        "provider1_name": provider1.provider_name,
                        "provider2_id": provider2.provider_id,
                        "provider2_name": provider2.provider_name,
                        "similarity_score": similarity_score
                    })
        
        app_logger.info(f"Detected {len(potential_duplicates)} potential duplicate provider entries")
        return potential_duplicates
    
    def identify_trends(self, providers: List[MedicalProvider]) -> Dict[str, Any]:
        # Create a DataFrame for easier analysis
        providers_df = self._create_dataframe(providers)
        
        # Analyze provider distribution by type
        provider_type_counts = providers_df["provider_type"].value_counts().to_dict()
        
        # Analyze service distribution
        all_services = []
        for services in providers_df["services"]:
            all_services.extend(services)
        service_counts = Counter(all_services)
        
        # Analyze geographic distribution
        geo_distribution = providers_df["state"].value_counts().to_dict()
        
        # Find most common specialties
        all_specialties = []
        for specialties in providers_df["specialties"].dropna():
            if specialties:
                all_specialties.extend(specialties)
        specialty_counts = Counter(all_specialties)
        
        # Find data completeness metrics
        missing_data = {}
        for column in providers_df.columns:
            missing_count = providers_df[column].isna().sum()
            if missing_count > 0:
                missing_data[column] = missing_count
        
        trends = {
            "provider_type_distribution": provider_type_counts,
            "service_distribution": service_counts,
            "geographic_distribution": geo_distribution,
            "specialty_distribution": dict(specialty_counts.most_common(10)),
            "data_completeness": {
                "fields_with_missing_data": missing_data,
                "completeness_score": 1 - (sum(missing_data.values()) / (len(providers_df) * len(providers_df.columns)))
            }
        }
        
        self.analysis_results["trends"] = trends
        app_logger.info("Completed trend analysis on provider data")
        return trends
    
    def identify_inconsistencies(self, providers: List[MedicalProvider]) -> List[Dict[str, Any]]:
        inconsistencies = []
        
        # Create a DataFrame for easier analysis
        providers_df = self._create_dataframe(providers)
        
        # Check for inconsistent provider naming patterns
        for provider_type in providers_df["provider_type"].unique():
            type_providers = providers_df[providers_df["provider_type"] == provider_type]
            if len(type_providers) > 5:  # Only check if we have enough data
                name_patterns = self._extract_name_patterns(type_providers["provider_name"].tolist())
                if len(name_patterns) > 2:
                    inconsistencies.append({
                        "type": "naming_inconsistency",
                        "provider_type": provider_type,
                        "patterns": name_patterns,
                        "recommendation": "Standardize naming conventions for consistency"
                    })
        
        # Check for inconsistent phone number formats
        phone_patterns = set()
        for provider in providers:
            phone = provider.contact_info.phone_number
            if phone:
                # Extract just the pattern of formatting
                pattern = re.sub(r'\d', '#', phone)
                phone_patterns.add(pattern)
        
        if len(phone_patterns) > 1:
            inconsistencies.append({
                "type": "phone_format_inconsistency",
                "patterns": list(phone_patterns),
                "recommendation": "Standardize phone number formats"
            })
        
        # Check for inconsistent service categorization
        hospital_providers = [p for p in providers if p.provider_type.lower() == "hospital"]
        if hospital_providers:
            service_sets = [set(p.services) for p in hospital_providers]
            common_services = set.intersection(*service_sets) if service_sets else set()
            if len(common_services) < 2 and len(hospital_providers) > 3:
                inconsistencies.append({
                    "type": "service_categorization_inconsistency",
                    "provider_type": "hospital",
                    "recommendation": "Standardize core services for hospitals"
                })
        
        app_logger.info(f"Identified {len(inconsistencies)} inconsistencies in provider data")
        return inconsistencies
    
    def generate_report(self) -> Dict[str, Any]:
        # Combine all analysis results into a comprehensive report
        report = {
            "timestamp": pd.Timestamp.now().isoformat(),
            **self.analysis_results
        }
        
        # Convert NumPy int64/float64 to Python int/float for JSON serialization
        return self._convert_numpy_types(report)
    
    def _convert_provider_to_dict(self, provider: MedicalProvider) -> Dict[str, Any]:
        return {
            "name": provider.provider_name,
            "type": provider.provider_type,
            "address": f"{provider.address.street1}, {provider.address.city}, {provider.address.state}",
            "phone": provider.contact_info.phone_number,
            "services": [s.value for s in provider.services]
        }
    
    def _calculate_similarity(self, provider1: Dict[str, Any], provider2: Dict[str, Any]) -> float:
        # Name similarity (higher weight)
        name_sim = SequenceMatcher(None, provider1["name"], provider2["name"]).ratio() * 0.4
        
        # Address similarity
        addr_sim = SequenceMatcher(None, provider1["address"], provider2["address"]).ratio() * 0.3
        
        # Phone similarity
        phone_sim = SequenceMatcher(None, provider1["phone"], provider2["phone"]).ratio() * 0.2
        
        # Service similarity
        services1 = set(provider1["services"])
        services2 = set(provider2["services"])
        
        if services1 and services2:
            service_sim = len(services1.intersection(services2)) / len(services1.union(services2)) * 0.1
        else:
            service_sim = 0
        
        return name_sim + addr_sim + phone_sim + service_sim
    
    def _create_dataframe(self, providers: List[MedicalProvider]) -> pd.DataFrame:
        data = []
        
        for provider in providers:
            provider_dict = {
                "provider_id": provider.provider_id,
                "provider_name": provider.provider_name,
                "provider_type": provider.provider_type,
                "services": [s.value for s in provider.services],
                "street": provider.address.street1,
                "city": provider.address.city,
                "state": provider.address.state,
                "zip_code": provider.address.zip_code,
                "phone": provider.contact_info.phone_number,
                "email": provider.contact_info.email,
                "website": provider.contact_info.website,
                "specialties": provider.specialties,
                "num_accreditations": len(provider.accreditations) if provider.accreditations else 0
            }
            data.append(provider_dict)
        
        return pd.DataFrame(data)
    
    def _extract_name_patterns(self, names: List[str]) -> List[str]:
        patterns = []
        for name in names:
            # Extract pattern (e.g., "First Last Medical Center" → "[Word] [Word] Medical Center")
            pattern = re.sub(r'\b[A-Z][a-z]+\b', '[Word]', name)
            if pattern not in patterns:
                patterns.append(pattern)
        
        return patterns[:5]  # Return at most 5 patterns
    
    def _convert_numpy_types(self, obj):
        # Handle NumPy types for JSON serialization
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {key: self._convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_numpy_types(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(self._convert_numpy_types(item) for item in obj)
        else:
            return obj
from src.data_models import MedicalProvider, ServiceCategory
from src.utils.logger import app_logger
import re
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime

class DataValidator:
    def __init__(self):
        self.error_log = []

    def validate_provider(self, provider_data: Dict[str, Any]) -> Tuple[bool, List[str], Optional[MedicalProvider]]:
        errors = []
        
        try:
            # Basic validation using Pydantic model
            provider = MedicalProvider(**provider_data)
            
            # Additional business rule validations
            validation_errors = self._apply_business_rules(provider)
            errors.extend(validation_errors)
            
            if errors:
                self._log_errors(provider_data.get("provider_name", "Unknown provider"), errors)
                return False, errors, None
                
            return True, [], provider
            
        except Exception as e:
            error_msg = f"Validation error: {str(e)}"
            errors.append(error_msg)
            self._log_errors(provider_data.get("provider_name", "Unknown provider"), errors)
            return False, errors, None
    
    def _apply_business_rules(self, provider: MedicalProvider) -> List[str]:
        errors = []
        
        # Check for required services based on provider type
        if provider.provider_type.lower() == "hospital" and ServiceCategory.EMERGENCY not in provider.services:
            errors.append("Hospitals should offer emergency services")
        
        # Validate accreditation expiration
        for accreditation in provider.accreditations:
            try:
                exp_date = datetime.strptime(accreditation.expiration_date, "%Y-%m-%d")
                if exp_date < datetime.now():
                    errors.append(f"Accreditation from {accreditation.organization} has expired")
            except ValueError:
                pass  # Date format already validated by Pydantic
        
        # Check phone numbers follow consistent format
        if provider.contact_info.phone_number and not provider.contact_info.phone_number.startswith("+1"):
            errors.append("Phone numbers should use international format starting with +1 for US numbers")
        
        # Ensure hospitals have multiple services
        if provider.provider_type.lower() == "hospital" and len(provider.services) < 3:
            errors.append("Hospitals should offer at least 3 different service categories")
            
        return errors
    
    def cross_reference_validation(self, provider: MedicalProvider, external_data_source=None) -> List[str]:
        # This would integrate with external APIs to validate information
        # For now, we'll return an empty list as a placeholder
        app_logger.info(f"Cross-reference validation for {provider.provider_name}")
        return []
    
    def _log_errors(self, provider_name: str, errors: List[str]) -> None:
        for error in errors:
            app_logger.error(f"Validation error for {provider_name}: {error}")
            self.error_log.append({"provider": provider_name, "error": error, "timestamp": datetime.now().isoformat()})
        
    def get_error_log(self) -> List[Dict[str, Any]]:
        return self.error_log
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
import phonenumbers
import validators
import re
from enum import Enum

class ServiceCategory(str, Enum):
    PRIMARY_CARE = "primary_care"
    SPECIALTY_CARE = "specialty_care"
    EMERGENCY = "emergency"
    SURGERY = "surgery"
    DIAGNOSTIC = "diagnostic"
    MENTAL_HEALTH = "mental_health"
    REHABILITATION = "rehabilitation"
    PEDIATRICS = "pediatrics"
    GERIATRICS = "geriatrics"
    OBSTETRICS = "obstetrics"
    GYNECOLOGY = "gynecology"
    OTHER = "other"

class Address(BaseModel):
    street1: str
    street2: Optional[str] = None
    city: str
    state: str
    zip_code: str
    country: str = "USA"
    
    @field_validator('zip_code')
    @classmethod
    def validate_zip_code(cls, v):
        if not re.match(r'^\d{5}(-\d{4})?$', v):
            raise ValueError('Invalid ZIP code format')
        return v

class ContactInfo(BaseModel):
    phone_number: str
    fax: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    
    @field_validator('phone_number', 'fax')
    @classmethod
    def validate_phone(cls, v, info):
        if v is None and info.field_name == 'fax':
            return v
        try:
            parsed = phonenumbers.parse(v, "US")
            if not phonenumbers.is_valid_number(parsed):
                raise ValueError(f"Invalid {info.field_name}")
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except Exception:
            raise ValueError(f"Invalid {info.field_name}")
    
    @field_validator('email')
    @classmethod
    def validate_email(cls, v):
        if v is None:
            return v
        if not validators.email(v):
            raise ValueError('Invalid email address')
        return v
    
    @field_validator('website')
    @classmethod
    def validate_website(cls, v):
        if v is None:
            return v
        if not validators.url(v):
            raise ValueError('Invalid website URL')
        return v

class Accreditation(BaseModel):
    organization: str
    license_number: str
    issue_date: str
    expiration_date: str
    
    @field_validator('issue_date', 'expiration_date')
    @classmethod
    def validate_date(cls, v):
        # Simple date validation - can be enhanced
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', v):
            raise ValueError('Date must be in format YYYY-MM-DD')
        return v

class MedicalProvider(BaseModel):
    provider_id: Optional[str] = None
    provider_name: str
    provider_type: str  # hospital, clinic, individual provider
    address: Address
    contact_info: ContactInfo
    services: List[ServiceCategory]
    accreditations: List[Accreditation]
    specialties: Optional[List[str]] = None
    languages: Optional[List[str]] = None
    insurance_accepted: Optional[List[str]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
import sys
import os
import json
import unittest

# Add parent directory to path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_validator import DataValidator
from src.data_models import ServiceCategory

class TestDataValidator(unittest.TestCase):
    def setUp(self):
        self.validator = DataValidator()
        
        # Load sample test data
        with open(os.path.join(os.path.dirname(__file__), 'sample_data.json'), 'r') as f:
            self.test_data = json.load(f)
    
    def test_valid_provider(self):
        # Test with a valid provider record
        provider_data = self.test_data[0]
        
        # Convert service strings to ServiceCategory enum
        provider_data["services"] = [
            ServiceCategory(s) for s in provider_data["services"]
        ]
        
        is_valid, errors, provider = self.validator.validate_provider(provider_data)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
        self.assertIsNotNone(provider)
        self.assertEqual(provider.provider_name, "Metropolis General Hospital")
    
    def test_invalid_phone_format(self):
        # Test with invalid phone number format
        provider_data = self.test_data[3]  # Eastside Community Hospital with invalid phone format
        
        # Convert service strings to ServiceCategory enum
        provider_data["services"] = [
            ServiceCategory(s) for s in provider_data["services"]
        ]
        
        is_valid, errors, provider = self.validator.validate_provider(provider_data)
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
        self.assertIn("phone", " ".join(errors).lower())
    
    def test_duplicate_detection(self):
        # Test if the validator can apply business rules to find potential duplicates
        # First provider - full record
        provider1 = self.test_data[0]
        provider1["services"] = [ServiceCategory(s) for s in provider1["services"]]
        
        # Last provider - duplicate with slight variations
        provider2 = self.test_data[4]
        provider2["services"] = [ServiceCategory(s) for s in provider2["services"]]
        
        _, _, valid_provider1 = self.validator.validate_provider(provider1)
        _, _, valid_provider2 = self.validator.validate_provider(provider2)
        
        # Business rules for hospital requiring 3+ services should fail for the duplicate
        self.assertEqual(valid_provider1.provider_name, valid_provider2.provider_name)
        
        # Apply business rules manually
        errors = self.validator._apply_business_rules(valid_provider2)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("3 different service categories" in error for error in errors))

if __name__ == '__main__':
    unittest.main()
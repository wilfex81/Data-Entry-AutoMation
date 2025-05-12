import requests
import json
import os
from typing import Dict, Any, List, Optional, Union
from dotenv import load_dotenv
from src.utils.logger import app_logger
from src.data_models import MedicalProvider
import uuid

load_dotenv()

class APIClient:
    def __init__(self):
        self.base_url = os.getenv("API_BASE_URL", "https://api.medicaldashboard.example.com/v1")
        self.api_key = os.getenv("API_KEY")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        if not self.api_key:
            app_logger.warning("API key not found in environment variables")

    def submit_provider(self, provider: MedicalProvider) -> Dict[str, Any]:
        endpoint = f"{self.base_url}/providers"
        
        try:
            response = requests.post(
                endpoint,
                headers=self.headers,
                data=provider.json()
            )
            
            response.raise_for_status()
            app_logger.info(f"Successfully submitted provider: {provider.provider_name}")
            return response.json()
            
        except requests.exceptions.RequestException as e:
            app_logger.error(f"Error submitting provider {provider.provider_name}: {str(e)}")
            if hasattr(e, 'response') and e.response:
                try:
                    error_detail = e.response.json()
                    app_logger.error(f"API error details: {json.dumps(error_detail)}")
                except:
                    app_logger.error(f"API error status code: {e.response.status_code}")
            raise
    
    def update_provider(self, provider_id: str, provider: MedicalProvider) -> Dict[str, Any]:
        endpoint = f"{self.base_url}/providers/{provider_id}"
        
        try:
            response = requests.put(
                endpoint,
                headers=self.headers,
                data=provider.json()
            )
            
            response.raise_for_status()
            app_logger.info(f"Successfully updated provider: {provider.provider_name}")
            return response.json()
            
        except requests.exceptions.RequestException as e:
            app_logger.error(f"Error updating provider {provider.provider_name}: {str(e)}")
            raise
    
    def get_provider(self, provider_id: str) -> Dict[str, Any]:
        endpoint = f"{self.base_url}/providers/{provider_id}"
        
        try:
            response = requests.get(
                endpoint,
                headers=self.headers
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            app_logger.error(f"Error retrieving provider {provider_id}: {str(e)}")
            raise
    
    def search_providers(self, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        endpoint = f"{self.base_url}/providers/search"
        
        try:
            response = requests.get(
                endpoint,
                headers=self.headers,
                params=query_params
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            app_logger.error(f"Error searching providers: {str(e)}")
            raise
    
    def get_batch_status(self, batch_id: str) -> Dict[str, Any]:
        endpoint = f"{self.base_url}/batches/{batch_id}"
        
        try:
            response = requests.get(
                endpoint,
                headers=self.headers
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            app_logger.error(f"Error retrieving batch status for {batch_id}: {str(e)}")
            raise
    
    def submit_provider_batch(self, providers: List[MedicalProvider]) -> Dict[str, Any]:
        endpoint = f"{self.base_url}/providers/batch"
        
        try:
            provider_data = [provider.dict() for provider in providers]
            
            response = requests.post(
                endpoint,
                headers=self.headers,
                json={"providers": provider_data}
            )
            
            response.raise_for_status()
            batch_id = response.json().get("batch_id")
            app_logger.info(f"Successfully submitted batch with {len(providers)} providers. Batch ID: {batch_id}")
            return response.json()
            
        except requests.exceptions.RequestException as e:
            app_logger.error(f"Error submitting provider batch: {str(e)}")
            raise
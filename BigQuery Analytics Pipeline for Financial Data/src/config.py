"""
BigQuery configuration and initialization utilities.
"""
import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class BigQueryConfig:
    """Configuration for BigQuery connections and dataset management."""
    
    def __init__(self):
        self.project_id: str = os.getenv("GCP_PROJECT_ID", "")
        self.dataset_id: str = os.getenv("GCP_DATASET_ID", "financial_data")
        self.credentials_path: Optional[str] = os.getenv("GCP_CREDENTIALS_PATH")
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    def validate(self) -> bool:
        """Validate configuration is complete."""
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID not set in environment")
        return True
    
    def get_dataset_ref(self) -> str:
        """Get fully qualified dataset reference."""
        return f"{self.project_id}.{self.dataset_id}"

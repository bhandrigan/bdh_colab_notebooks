import yaml
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from simple_salesforce import Salesforce


class GCPClientManager:
    def __init__(self, config_path, service_account_key):
        """
        Initialize the GCPClientManager with configuration and service account key.

        Args:
            config_path (str): Path to the YAML configuration file.
            service_account_key (str): Key in the YAML file for the service account path.
        """
        self.config = self.load_config(config_path)
        self.service_account_key = self.config.get(service_account_secret_name)
        self.credentials = self.load_service_account_credentials(self.config.get(service_account_key))
        self.clients = {}

    def load_config(self, file_path):
        """Load the configuration file."""
        try:
            with open(file_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at: {file_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration file: {e}")

    def load_service_account_credentials(self, service_account_path):
        """Load service account credentials from a file."""
        if not service_account_path:
            raise ValueError("Service account path is not defined in the configuration.")
        try:
            return service_account.Credentials.from_service_account_file(service_account_path)
        except Exception as e:
            raise ValueError(f"Error loading service account credentials: {e}")

    def get_bigquery_client(self):
        """Initialize and return a BigQuery client."""
        if "bigquery" not in self.clients:
            self.clients["bigquery"] = bigquery.Client(credentials=self.credentials)
        return self.clients["bigquery"]

    def get_storage_client(self):
        """Initialize and return a Storage client."""
        if "storage" not in self.clients:
            self.clients["storage"] = storage.Client(credentials=self.credentials)
        return self.clients["storage"]

    def get_salesforce_client(self):
        """Initialize and return a Salesforce client."""
        if "salesforce" not in self.clients:
            sf_config = self.config.get("veil_billing", {})
            sf_username = sf_config.get("SF_USERNAME")
            sf_password = sf_config.get("SF_PASSWORD")
            sf_token = sf_config.get("SF_TOKEN")
            if not (sf_username and sf_password and sf_token):
                raise ValueError("Salesforce credentials are incomplete in the configuration.")
            self.clients["salesforce"] = Salesforce(username=sf_username, password=sf_password, security_token=sf_token)
        return self.clients["salesforce"]


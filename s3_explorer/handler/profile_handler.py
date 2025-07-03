import os
import json
import boto3
from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtWidgets import QMessageBox # For potential error messages if not handled by main app
from dotenv import load_dotenv, find_dotenv

class ProfileManager(QObject):
    s3_client_initialized = pyqtSignal(object, str) # client, profile_name
    s3_client_init_failed = pyqtSignal(str, str)    # profile_name, error_message
    profiles_loaded = pyqtSignal(dict, str)         # all_profiles, active_profile_name
    active_profile_switched = pyqtSignal(str, object) # new_active_profile_name, new_s3_client (or None if failed)


    def __init__(self, app_data_dir, parent=None):
        super().__init__(parent)
        self.app_data_dir = app_data_dir
        self.profiles_file = os.path.join(self.app_data_dir, "profiles.json")
        
        self.aws_profiles = {}
        self.active_profile_name = None
        self.s3_client = None

    def _ensure_app_data_dir_exists(self): # Keep for self-sufficiency if needed
        if not os.path.exists(self.app_data_dir):
            try:
                os.makedirs(self.app_data_dir, exist_ok=True)
            except OSError as e:
                print(f"Error creating application data directory {self.app_data_dir}: {e}")
                # Let the main application handle critical error popups
                return False
        return True

    def load_aws_profiles(self):
        try:
            if os.path.exists(self.profiles_file):
                with open(self.profiles_file, 'r') as f:
                    data = json.load(f)
                    self.aws_profiles = data.get("profiles", {})
                    self.active_profile_name = data.get("active_profile_name", None)
            else: # File doesn't exist, initialize
                self.aws_profiles = {}
                self.active_profile_name = None
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading profiles from {self.profiles_file}: {e}. Initializing empty profiles.")
            self.aws_profiles = {}
            self.active_profile_name = None

        if not isinstance(self.aws_profiles, dict): self.aws_profiles = {}

        if not self.aws_profiles: # No profiles exist at all
            dotenv_path = find_dotenv(usecwd=True, raise_error_if_not_found=False)
            if dotenv_path and os.path.exists(dotenv_path):
                print(f"Found .env file at {dotenv_path}, attempting migration to 'Default' profile.")
                load_dotenv(dotenv_path)
                env_access_key = os.getenv("AWS_ACCESS_KEY_ID")
                env_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
                env_region = os.getenv("AWS_DEFAULT_REGION")
                env_endpoint_url = os.getenv("AWS_ENDPOINT_URL", "") 
                if env_access_key and env_secret_key and env_region:
                    self.aws_profiles["Default"] = {
                        "aws_access_key_id": env_access_key,
                        "aws_secret_access_key": env_secret_key,
                        "aws_default_region": env_region,
                        "endpoint_url": env_endpoint_url,
                        "default_s3_bucket": os.getenv("DEFAULT_S3_BUCKET", "")
                    }
                    self.active_profile_name = "Default"
                    self.save_aws_profiles() # Save the migrated profile
                    # Main app should show the QMessageBox for migration info
            
            if not self.aws_profiles: # Still no profiles, create an empty "Default"
                self.aws_profiles["Default"] = { "aws_access_key_id": "", "aws_secret_access_key": "", "aws_default_region": "us-east-1", "endpoint_url": "", "default_s3_bucket": ""}
                self.active_profile_name = "Default"
                # Don't save empty default automatically, let user confirm via dialog if it's first run.
        
        self.profiles_loaded.emit(self.aws_profiles.copy(), self.active_profile_name)
        return self.aws_profiles, self.active_profile_name


    def save_aws_profiles(self):
        if not self._ensure_app_data_dir_exists(): return False
        data_to_save = {
            "profiles": self.aws_profiles,
            "active_profile_name": self.active_profile_name
        }
        try:
            with open(self.profiles_file, 'w') as f:
                json.dump(data_to_save, f, indent=4)
            print(f"AWS profiles saved to {self.profiles_file}")
            return True
        except IOError as e:
            print(f"Error saving AWS profiles to {self.profiles_file}: {e}")
            # QMessageBox.warning(None, "Save Error", f"Could not save AWS profiles: {e}")
            return False

    def get_s3_client(self):
        return self.s3_client

    def get_active_profile_name(self):
        return self.active_profile_name
    
    def get_all_profiles(self):
        return self.aws_profiles.copy()

    def get_profile_data(self, profile_name):
        return self.aws_profiles.get(profile_name)

    def get_active_profile_data(self):
        if self.active_profile_name and self.active_profile_name in self.aws_profiles:
            return self.aws_profiles[self.active_profile_name]
        return None

    def update_profiles_data(self, new_profiles_data, new_active_profile_name):
        self.aws_profiles = new_profiles_data
        # Active profile switch will be handled by switch_profile or init_s3_client_with_config
        # This method is primarily for when the ProfileManagerDialog returns updated data.
        # The S3Explorer will then decide if a re-initialization is needed based on new_active_profile_name.


    def attempt_s3_client_initialization(self, profile_name_to_init=None):
        """Attempts to initialize S3 client for the given profile, or the current active one."""
        target_profile_name = profile_name_to_init if profile_name_to_init else self.active_profile_name
        
        if target_profile_name and target_profile_name in self.aws_profiles:
            profile_data = self.aws_profiles[target_profile_name]
            if profile_data.get("aws_access_key_id") and \
               profile_data.get("aws_secret_access_key") and \
               profile_data.get("aws_default_region"):
                print(f"PROFILE_MANAGER: Calling init_s3_client_with_config for profile '{target_profile_name}'")
                return self.init_s3_client_with_config(profile_data, target_profile_name)
            else:
                err_msg = f"Profile '{target_profile_name}' is incomplete. Cannot initialize S3 client."
                print(f"PROFILE_MANAGER: {err_msg}")
                self.s3_client = None # Ensure client is None
                self.s3_client_init_failed.emit(target_profile_name, err_msg)
                return False
        else:
            err_msg = f"Profile '{target_profile_name}' not found or no active profile. Cannot initialize S3 client."
            print(f"PROFILE_MANAGER: {err_msg}")
            self.s3_client = None # Ensure client is None
            self.s3_client_init_failed.emit(target_profile_name or "None", err_msg)
            return False


    def init_s3_client_with_config(self, profile_config, profile_name_being_initialized):
        print(f"PROFILE_MANAGER: init_s3_client_with_config for profile: '{profile_name_being_initialized}'")
        
        access_key = profile_config.get("aws_access_key_id")
        secret_key = profile_config.get("aws_secret_access_key")
        region = profile_config.get("aws_default_region")
        endpoint_url = profile_config.get("endpoint_url") # Keep None if not present or empty string
        if isinstance(endpoint_url, str) and not endpoint_url.strip(): # Treat empty string as None
            endpoint_url = None
            
        default_s3_bucket = profile_config.get("default_s3_bucket")
        if isinstance(default_s3_bucket, str) and not default_s3_bucket.strip():
            default_s3_bucket = None

        if not all([access_key, secret_key, region]):
            self.s3_client = None
            error_msg = f"Profile '{profile_name_being_initialized}' is incomplete (missing Key ID, Secret Key, or Region)."
            print(f"  {error_msg}")
            self.s3_client_init_failed.emit(profile_name_being_initialized, error_msg)
            return False
        try:
            session_params = {
                "aws_access_key_id": access_key,
                "aws_secret_access_key": secret_key,
                "region_name": region
            }
            session = boto3.Session(**session_params)
            
            client_params = {}
            if endpoint_url:
                client_params['endpoint_url'] = endpoint_url
                # For some S3-compatible services, signature version might be important
                # client_params['config'] = boto3.session.Config(signature_version='s3v4') # Example
            
            print(f"  Attempting S3 client creation. Endpoint URL: {endpoint_url}, Region: {region}")
            new_s3_client = session.client('s3', **client_params)
            
            # Perform a test call
            test_call_description = ""
            if endpoint_url and default_s3_bucket:
                test_call_description = f"s3.head_bucket(Bucket='{default_s3_bucket}') on endpoint {endpoint_url}"
                print(f"  Attempting {test_call_description} for profile '{profile_name_being_initialized}'")
                new_s3_client.head_bucket(Bucket=default_s3_bucket)
            else: # No endpoint OR (endpoint present but no default bucket specified)
                test_call_description = f"s3.list_buckets() (Endpoint: {endpoint_url or 'AWS S3 Default'})"
                print(f"  Attempting {test_call_description} for profile '{profile_name_being_initialized}'") 
                new_s3_client.list_buckets() 
            
            print(f"  Test call ({test_call_description}) successful.") 
            
            old_active_profile_name = self.active_profile_name
            self.s3_client = new_s3_client 
            self.active_profile_name = profile_name_being_initialized 
            
            self.s3_client_initialized.emit(self.s3_client, self.active_profile_name)
            if old_active_profile_name != self.active_profile_name or old_active_profile_name is None:
                self.active_profile_switched.emit(self.active_profile_name, self.s3_client)

            print(f"PROFILE_MANAGER: init_s3_client_with_config for '{profile_name_being_initialized}': End. Success: True")
            return True 

        except Exception as e:
            self.s3_client = None 
            error_msg = f"Error connecting with profile '{profile_name_being_initialized}' using {test_call_description or 'S3 client'}: {e}"
            print(f"  EXCEPTION during S3 client init for '{profile_name_being_initialized}': {e}") 
            self.s3_client_init_failed.emit(profile_name_being_initialized, error_msg)
            return False

    def set_active_profile_name_only(self, profile_name):
        """Only sets the active profile name, does not attempt to initialize. Caller saves."""
        if profile_name in self.aws_profiles or profile_name is None:
            self.active_profile_name = profile_name
            print(f"PROFILE_MANAGER: Active profile name set to '{profile_name}' (no client init).")
            
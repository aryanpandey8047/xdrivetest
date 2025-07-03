from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QListWidget, QListWidgetItem,
    QPushButton, QFormLayout, QLineEdit, QComboBox,
    QDialogButtonBox, QMessageBox, QLabel, QAbstractItemView, QInputDialog
)
from PyQt6.QtCore import Qt

from PyQt6.QtCore import QUrl, QTimer
from PyQt6.QtGui import QDesktopServices
import webbrowser
import threading
import time
from callback_server import run_callback_server
import os,sys, json
import platform
from pathlib import Path


# Reuse AWS_REGIONS from credentials_dialog.py or define here
AWS_REGIONS = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2", "af-south-1", "ap-east-1",
    "ap-south-1", "ap-northeast-3", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
    "ap-northeast-1", "ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2",
    "eu-south-1", "eu-west-3", "eu-north-1", "me-south-1", "sa-east-1"
]

# --- Application Data Paths ---
def get_application_base_path():
    """ Get the base path for the application, accounting for PyInstaller. """
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        # Running in a PyInstaller bundle (one-file or one-folder)
        # For one-file, _MEIPASS is the temp dir. For one-folder, it's the app dir.
        # We want the directory of the executable itself for persistent data.
        return os.path.dirname(sys.executable)
    else:
        # Running as a normal script
        return os.path.dirname(os.path.abspath(__file__))

class ProfileManagerDialog(QDialog):
    def __init__(self, parent=None, profiles=None, active_profile_name=None):
        super().__init__(parent)
        self.setWindowTitle("AWS Connection Profiles")
        self.setMinimumSize(700, 500)

        self.profiles_data = profiles if profiles is not None else {}  # dict: {profile_name: {details}}
        self.active_profile_name = active_profile_name
        self.profile_path = get_application_base_path()


        main_layout = QVBoxLayout(self)

        # Left: List of profiles | Right: Details of selected profile
        content_layout = QHBoxLayout()

        # Left Panel: Profile List
        left_panel_layout = QVBoxLayout()
        left_panel_layout.addWidget(QLabel("Profiles:"))
        self.profiles_list_widget = QListWidget()
        self.profiles_list_widget.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        # self.profiles_list_widget.itemSelectionChanged.connect(self.on_profile_selected)
        left_panel_layout.addWidget(self.profiles_list_widget)

        list_buttons_layout = QHBoxLayout()
        add_profile_button = QPushButton("Add")
        add_profile_button.clicked.connect(self.add_profile)
        remove_profile_button = QPushButton("Remove")
        remove_profile_button.clicked.connect(self.remove_profile)
        set_active_button = QPushButton("Set Active")
        set_active_button.clicked.connect(self.set_selected_as_active)
        
        select_button = QPushButton("Sign in")
        select_button.clicked.connect(self.start_callback_server_and_open_login)

        list_buttons_layout.addWidget(add_profile_button)
        list_buttons_layout.addWidget(remove_profile_button)
        list_buttons_layout.addWidget(set_active_button)
        left_panel_layout.addLayout(list_buttons_layout)
        content_layout.addLayout(left_panel_layout, 1) # Stretch factor 1
        #list_buttons_layout.addWidget(select_button) #<-- Un comment line to enable sign in button


        # Right Panel: Profile Details
        right_panel_layout = QVBoxLayout()
        right_panel_layout.addWidget(QLabel("Profile Details:"))
        self.details_form_layout = QFormLayout()

        self.profile_name_edit = QLineEdit()
        self.profile_name_edit.setReadOnly(True) # Name is key, edit via rename action (future)
        self.access_key_edit = QLineEdit()
        self.secret_key_edit = QLineEdit()
        self.secret_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.region_combo = QComboBox()
        self.region_combo.addItems(AWS_REGIONS)
        self.region_combo.setEditable(True)
        self.endpoint_url_edit = QLineEdit() # DEFINED HERE
        self.endpoint_url_edit.setPlaceholderText("(Optional) e.g., http://localhost:9000")
        self.default_bucket_edit = QLineEdit()
        self.default_bucket_edit.setPlaceholderText("(Optional) e.g., my-startup-bucket")

        self.details_form_layout.addRow("Profile Name:", self.profile_name_edit)
        self.details_form_layout.addRow("Access Key ID:", self.access_key_edit)
        self.details_form_layout.addRow("Secret Access Key:", self.secret_key_edit)
        self.details_form_layout.addRow("Default Region:", self.region_combo)
        self.details_form_layout.addRow("Endpoint URL:", self.endpoint_url_edit) 
        self.details_form_layout.addRow("Default S3 Bucket:", self.default_bucket_edit)

        save_changes_button = QPushButton("Save Changes to Selected Profile")
        save_changes_button.clicked.connect(self.save_current_profile_details)

        right_panel_layout.addLayout(self.details_form_layout)
        right_panel_layout.addWidget(save_changes_button)
        right_panel_layout.addStretch()
        content_layout.addLayout(right_panel_layout, 2) # Stretch factor 2

        main_layout.addLayout(content_layout)

        # Dialog Buttons (OK/Cancel)
        self.dialog_button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.dialog_button_box.accepted.connect(self.accept) # Returns modified profiles_data & active_profile
        self.dialog_button_box.rejected.connect(self.reject)
        main_layout.addWidget(self.dialog_button_box)

        # Connect selection signal AFTER all form elements are defined
        self.profiles_list_widget.itemSelectionChanged.connect(self.on_profile_selected)

        self.populate_profiles_list() # This might trigger on_profile_selected if items exist

        # Explicitly clear or load details based on initial state AFTER form elements are ready
        if self.active_profile_name and self.active_profile_name in self.profiles_data:
            selected_item_found = False
            for i in range(self.profiles_list_widget.count()):
                item = self.profiles_list_widget.item(i)
                if item.data(Qt.ItemDataRole.UserRole) == self.active_profile_name:
                    item.setSelected(True) # This should trigger on_profile_selected
                    selected_item_found = True
                    break
            if not selected_item_found: # Active profile name exists but not in list widget (should not happen)
                 self.clear_details_form()
        elif self.profiles_list_widget.count() > 0: 
            self.profiles_list_widget.setCurrentRow(0) # Triggers on_profile_selected for first item
        else: # No profiles and no active profile
            self.clear_details_form()
    
    def start_callback_server_and_open_login(self):
        def run_and_wait_for_file():
            run_callback_server(timeout=15)

            time.sleep(1)  # Give the file system time to write

            if os.path.exists(self.profile_path):
                with open(self.profile_path, "r") as f:
                    profile_data = json.load(f)
                    self.profiles_data = profile_data.get("profiles", {})
                    self.active_profile_name = profile_data.get("active_profile_name", None)

                self.populate_profiles_list()

        threading.Thread(target=run_and_wait_for_file, daemon=True).start()
        webbrowser.open("http://localhost/accounts.oaks.pro/login.php?redirect_url=http://localhost/accounts.oaks.pro/xdrivelogin.php")

    def populate_profiles_list(self):
        current_selection_name = None
        selected_items_now = self.profiles_list_widget.selectedItems() # Get current selection before clearing
        if selected_items_now:
            current_selection_name = selected_items_now[0].data(Qt.ItemDataRole.UserRole)

        self.profiles_list_widget.clear()
        new_selection_index = -1
        for i, name in enumerate(sorted(self.profiles_data.keys())):
            item_text = name
            if name == self.active_profile_name:
                item_text += " (Active)"
            list_item = QListWidgetItem(item_text)
            list_item.setData(Qt.ItemDataRole.UserRole, name) 
            self.profiles_list_widget.addItem(list_item)
            if name == current_selection_name: 
                new_selection_index = i
        
        if new_selection_index != -1:
            self.profiles_list_widget.setCurrentRow(new_selection_index) # Re-select row if it still exists
        elif self.profiles_list_widget.count() > 0 and not current_selection_name: # No prior selection, select first
            self.profiles_list_widget.setCurrentRow(0)


    def on_profile_selected(self):
        selected_items = self.profiles_list_widget.selectedItems()
        if selected_items:
            profile_name = selected_items[0].data(Qt.ItemDataRole.UserRole)
            self.load_profile_details(profile_name)
        else:
            self.clear_details_form()

    def load_profile_details(self, profile_name):
        profile = self.profiles_data.get(profile_name)
        if profile:
            self.profile_name_edit.setText(profile_name)
            self.access_key_edit.setText(profile.get("aws_access_key_id", ""))
            self.secret_key_edit.setText(profile.get("aws_secret_access_key", ""))
            
            current_region = profile.get("aws_default_region", "us-east-1")
            if current_region not in AWS_REGIONS and self.region_combo.findText(current_region) == -1:
                self.region_combo.insertItem(0, current_region) # Add if custom and not present
            self.region_combo.setCurrentText(current_region)
            
            self.endpoint_url_edit.setText(profile.get("endpoint_url", ""))
            self.default_bucket_edit.setText(profile.get("default_s3_bucket", ""))
        else:
            self.clear_details_form()

    def clear_details_form(self):
        self.profile_name_edit.clear()
        self.access_key_edit.clear()
        self.secret_key_edit.clear()
        # Ensure region_combo and endpoint_url_edit exist before clearing
        if hasattr(self, 'region_combo'):
            self.region_combo.setCurrentIndex(0) if self.region_combo.count() > 0 else self.region_combo.clearEditText()
        if hasattr(self, 'endpoint_url_edit'):
            self.endpoint_url_edit.clear() 
        if hasattr(self, 'default_bucket_edit'):
            self.default_bucket_edit.clear()


    def add_profile(self):
        profile_name, ok = QInputDialog.getText(self, "New Profile", "Enter a unique profile name:")
        if ok and profile_name:
            profile_name = profile_name.strip()
            if not profile_name:
                QMessageBox.warning(self, "Input Error", "Profile name cannot be empty.")
                return
            if profile_name in self.profiles_data:
                QMessageBox.warning(self, "Input Error", f"Profile name '{profile_name}' already exists.")
                return

            self.profiles_data[profile_name] = { # Initialize with empty but valid structure
                "aws_access_key_id": "",
                "aws_secret_access_key": "",
                "aws_default_region": "us-east-1",
                "endpoint_url": "",
                "default_s3_bucket": ""
            }
            self.populate_profiles_list()
            
            for i in range(self.profiles_list_widget.count()):
                if self.profiles_list_widget.item(i).data(Qt.ItemDataRole.UserRole) == profile_name:
                    self.profiles_list_widget.setCurrentRow(i) # Triggers selection and loads empty details
                    break
            # self.clear_details_form() # on_profile_selected should handle this
            # self.profile_name_edit.setText(profile_name) # on_profile_selected handles this
            QMessageBox.information(self, "New Profile", f"Profile '{profile_name}' added. Please fill in its details and click 'Save Changes'.")


    def remove_profile(self):
        selected_items = self.profiles_list_widget.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Selection Error", "Please select a profile to remove.")
            return

        profile_name_to_remove = selected_items[0].data(Qt.ItemDataRole.UserRole)
        
        reply = QMessageBox.question(self, "Confirm Remove",
                                     f"Are you sure you want to remove profile '{profile_name_to_remove}'?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            if profile_name_to_remove in self.profiles_data:
                del self.profiles_data[profile_name_to_remove]
            
            was_active = (self.active_profile_name == profile_name_to_remove)
            if was_active:
                self.active_profile_name = None 
                if self.profiles_data: # try to set a new active one
                    self.active_profile_name = sorted(self.profiles_data.keys())[0]
            
            self.populate_profiles_list() # Re-populate list

            # If the removed profile was active or no profile is selected, clear form.
            # Otherwise, the selection will shift and on_profile_selected will load new details.
            if not self.profiles_list_widget.selectedItems():
                 self.clear_details_form()

    def set_selected_as_active(self):
        selected_items = self.profiles_list_widget.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Selection Error", "Please select a profile to set as active.")
            return
        profile_name = selected_items[0].data(Qt.ItemDataRole.UserRole)
        
        # Ensure the profile has minimal necessary data before allowing it to be set active
        profile_to_activate = self.profiles_data.get(profile_name, {})
        if not profile_to_activate.get("aws_access_key_id") or \
           not profile_to_activate.get("aws_secret_access_key") or \
           not profile_to_activate.get("aws_default_region"):
            QMessageBox.warning(self, "Incomplete Profile", 
                                f"Profile '{profile_name}' is incomplete (missing Key ID, Secret Key, or Region). "
                                "Please fill in the details and save before setting it active.")
            return

        self.active_profile_name = profile_name
        self.populate_profiles_list() 
        QMessageBox.information(self, "Profile Activated", f"Profile '{profile_name}' will be active when you click 'OK'.")


    def save_current_profile_details(self):
        profile_name = self.profile_name_edit.text()
        if not profile_name or profile_name not in self.profiles_data:
            QMessageBox.warning(self, "Save Error", "No profile selected or profile name is invalid. Select or add a profile first.")
            return

        access_key = self.access_key_edit.text().strip()
        secret_key = self.secret_key_edit.text() # Don't strip secret key
        region = self.region_combo.currentText().strip()
        endpoint_url = self.endpoint_url_edit.text().strip()

        if not access_key:
            QMessageBox.warning(self, "Input Error", "Access Key ID is required.")
            return
        if not secret_key:
            QMessageBox.warning(self, "Input Error", "Secret Access Key is required.")
            return
        if not region:
            QMessageBox.warning(self, "Input Error", "Default Region is required.")
            return

        self.profiles_data[profile_name] = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "aws_default_region": region,
            "endpoint_url": endpoint_url,
            "default_s3_bucket": self.default_bucket_edit.text().strip()
        }
        QMessageBox.information(self, "Profile Saved", f"Details for profile '{profile_name}' saved locally. Click OK to apply changes to the application.")
        self.populate_profiles_list() 

    def get_profiles_data(self):
        # Called when OK is clicked on the main dialog
        # Ensure the currently displayed (and potentially unsaved) details are saved if a profile is selected
        if self.profile_name_edit.text() and self.profile_name_edit.text() in self.profiles_data:
            # Check if form data is different from stored data for the selected profile
            # This is a bit manual; a "dirty" flag would be better.
            # For now, just prompt to save if a profile is loaded in the form.
            current_profile_name_in_form = self.profile_name_edit.text()
            # Check if the profile exists; if it was deleted and form not cleared, stored_data would be {}
            if current_profile_name_in_form not in self.profiles_data:
                 # This case implies the profile was deleted but form wasn't fully cleared, or a new profile was typed into a readonly field
                 # This shouldn't happen with readonly profile_name_edit.
                 pass
            else:
                stored_data = self.profiles_data.get(current_profile_name_in_form, {})
                form_data = {
                    "aws_access_key_id": self.access_key_edit.text().strip(),
                    "aws_secret_access_key": self.secret_key_edit.text(),
                    "aws_default_region": self.region_combo.currentText().strip(),
                    "endpoint_url": self.endpoint_url_edit.text().strip(), 
                    "default_s3_bucket": self.default_bucket_edit.text().strip()
                }
                
                fields_to_compare = ["aws_access_key_id", "aws_secret_access_key", "aws_default_region", "endpoint_url", "default_s3_bucket"]
                if any(stored_data.get(k) != form_data.get(k) for k in fields_to_compare):
                    reply = QMessageBox.question(self, "Unsaved Changes",
                                                 f"You have unsaved changes for profile '{current_profile_name_in_form}'. Save them now?",
                                                 QMessageBox.StandardButton.Save | QMessageBox.StandardButton.Discard | QMessageBox.StandardButton.Cancel,
                                                 QMessageBox.StandardButton.Save)
                    if reply == QMessageBox.StandardButton.Save:
                        self.save_current_profile_details() 
                    elif reply == QMessageBox.StandardButton.Cancel:
                        return None 
        return self.profiles_data, self.active_profile_name

    def accept(self):
        # Override accept to handle the potential None from get_profiles_data
        result = self.get_profiles_data()
        if result is not None:
            super().accept()
            
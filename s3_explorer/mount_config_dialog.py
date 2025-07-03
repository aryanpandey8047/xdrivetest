from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, QLineEdit,
    QPushButton, QListWidget, QListWidgetItem, QFileDialog, QMessageBox,
    QDialogButtonBox, QLabel, QAbstractItemView, QInputDialog, QComboBox # Added QComboBox
)
from PyQt6.QtCore import Qt

class MountConfigDialog(QDialog):
    def __init__(self, parent=None, existing_mounts=None, active_profile_default_bucket=None): # Added active_profile_default_bucket
        super().__init__(parent)
        self.setWindowTitle("Configure S3 Mounted Paths")
        self.setMinimumSize(650, 450) # Slightly wider for filter

        # Master list of all mount configurations
        self.all_mounts_config = list(existing_mounts) if existing_mounts else [] 
        self.active_profile_default_bucket = active_profile_default_bucket
        self.current_filter_bucket_text = "All Buckets" # Initial filter, will be updated

        main_layout = QVBoxLayout(self)

        # Filter layout
        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("Filter by S3 Bucket:"))
        self.bucket_filter_combo = QComboBox()
        self.bucket_filter_combo.setMinimumWidth(200)
        filter_layout.addWidget(self.bucket_filter_combo)
        filter_layout.addStretch(1)
        main_layout.addLayout(filter_layout)

        self.mounts_list_widget = QListWidget()
        self.mounts_list_widget.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        main_layout.addWidget(QLabel("Configured Mounts (Local Path  <=>  s3://Bucket/Prefix):"))
        main_layout.addWidget(self.mounts_list_widget)

        list_button_layout = QHBoxLayout()
        add_button = QPushButton("Add Mount")
        add_button.clicked.connect(self.add_mount_entry)
        remove_button = QPushButton("Remove Selected Mount")
        remove_button.clicked.connect(self.remove_selected_mount_entry)
        list_button_layout.addWidget(add_button)
        list_button_layout.addWidget(remove_button)
        main_layout.addLayout(list_button_layout)

        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        main_layout.addWidget(self.button_box)

        self._update_bucket_filter_options() # Populate and set initial filter
        self.bucket_filter_combo.currentTextChanged.connect(self._on_filter_bucket_changed)
        
        # Initial population based on the default filter set by _update_bucket_filter_options
        # self.current_filter_bucket_text is already set by _update_bucket_filter_options
        self.populate_mounts_list()


    def _update_bucket_filter_options(self):
        """Populates the bucket filter combobox and sets its initial/current value."""
        self.bucket_filter_combo.blockSignals(True)
        
        # Preserve user's current filter choice if possible, else default
        preferred_filter = self.current_filter_bucket_text 
        if self.active_profile_default_bucket and preferred_filter == "All Buckets": # If initial or no specific user choice yet
            preferred_filter = self.active_profile_default_bucket
        
        self.bucket_filter_combo.clear()
        
        all_options = ["All Buckets"]
        unique_buckets_from_mounts = sorted(list(set(m['s3_bucket'] for m in self.all_mounts_config)))
        all_options.extend(unique_buckets_from_mounts)

        if self.active_profile_default_bucket and self.active_profile_default_bucket not in all_options:
            all_options.append(self.active_profile_default_bucket)
            all_options.sort() # Re-sort if "All Buckets" isn't first or if new bucket added
            if "All Buckets" in all_options: # Ensure "All Buckets" is first after sort
                all_options.remove("All Buckets")
                all_options.insert(0, "All Buckets")
        
        self.bucket_filter_combo.addItems(all_options)

        # Try to set the preferred filter
        if self.bucket_filter_combo.findText(preferred_filter) != -1:
            self.bucket_filter_combo.setCurrentText(preferred_filter)
        else: # Fallback if preferred_filter (e.g. from a removed mount) is no longer valid
            self.bucket_filter_combo.setCurrentText("All Buckets")
            
        self.current_filter_bucket_text = self.bucket_filter_combo.currentText() # Update internal state
            
        self.bucket_filter_combo.blockSignals(False)


    def _on_filter_bucket_changed(self, selected_text):
        if selected_text: # Ensure it's not empty during intermediate clear
            self.current_filter_bucket_text = selected_text
            print(f"MountConfigDialog: Filter changed to: {self.current_filter_bucket_text}")
            self.populate_mounts_list()

    def populate_mounts_list(self):
        self.mounts_list_widget.clear()
        
        filter_bucket_is_specific = self.current_filter_bucket_text != "All Buckets"

        for mount_data in self.all_mounts_config:
            if not filter_bucket_is_specific or mount_data['s3_bucket'] == self.current_filter_bucket_text:
                s3_path_display = f"s3://{mount_data['s3_bucket']}"
                if mount_data['s3_prefix']:
                    s3_path_display += f"/{mount_data['s3_prefix'].strip('/')}"
                
                item_text = f"{mount_data['local_path']}  <=>  {s3_path_display}"
                list_item = QListWidgetItem(item_text)
                list_item.setData(Qt.ItemDataRole.UserRole, mount_data) 
                self.mounts_list_widget.addItem(list_item)

    def add_mount_entry(self):
        add_dialog = QDialog(self)
        add_dialog.setWindowTitle("Add New S3 Mount")
        add_dialog.setMinimumWidth(400)
        layout = QVBoxLayout(add_dialog)
        form = QFormLayout()

        local_edit = QLineEdit()
        browse_btn = QPushButton("Browse...")
        s3_bucket_edit_add = QLineEdit()
        s3_prefix_edit_add = QLineEdit()
        s3_prefix_edit_add.setPlaceholderText("e.g., path/to/folder (optional)")

        # Pre-fill bucket if a specific filter is active
        if self.current_filter_bucket_text != "All Buckets":
            s3_bucket_edit_add.setText(self.current_filter_bucket_text)
            # Optionally make it read-only if you want to enforce the filter during add
            # s3_bucket_edit_add.setReadOnly(True) 

        local_path_h_layout = QHBoxLayout()
        local_path_h_layout.addWidget(local_edit)
        local_path_h_layout.addWidget(browse_btn)
        browse_btn.clicked.connect(lambda: local_edit.setText(QFileDialog.getExistingDirectory(add_dialog, "Select Local Directory")))

        form.addRow("Local Path:", local_path_h_layout)
        form.addRow("S3 Bucket Name:", s3_bucket_edit_add)
        form.addRow("S3 Prefix (in bucket):", s3_prefix_edit_add)
        layout.addLayout(form)

        add_buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        add_buttons.accepted.connect(add_dialog.accept)
        add_buttons.rejected.connect(add_dialog.reject)
        layout.addWidget(add_buttons)

        if add_dialog.exec():
            local_path = local_edit.text().strip()
            s3_bucket = s3_bucket_edit_add.text().strip()
            s3_prefix = s3_prefix_edit_add.text().strip().strip('/') # Ensure no leading/trailing slash for consistency

            if not local_path or not s3_bucket:
                QMessageBox.warning(self, "Input Error", "Local Path and S3 Bucket Name are required.")
                return

            # Check for duplicate local_path in the master list
            if any(m['local_path'] == local_path for m in self.all_mounts_config):
                QMessageBox.warning(self, "Duplicate Mount", f"Local path '{local_path}' is already configured for a mount.")
                return

            new_mount = {'local_path': local_path, 's3_bucket': s3_bucket, 's3_prefix': s3_prefix}
            self.all_mounts_config.append(new_mount) # Add to master list
            
            self._update_bucket_filter_options() # Re-populate filter combo in case of new bucket
            self.populate_mounts_list() # Refresh the displayed list (respects current filter)

    def remove_selected_mount_entry(self):
        selected_items = self.mounts_list_widget.selectedItems()
        if not selected_items:
            QMessageBox.information(self, "Remove Mount", "Please select a mount to remove.")
            return
        
        mount_to_remove_text = selected_items[0].text() 
        mount_to_remove_data = selected_items[0].data(Qt.ItemDataRole.UserRole) # This is the dict from all_mounts_config

        reply = QMessageBox.question(self, "Confirm Remove",
                                     f"Are you sure you want to remove this mount?\n{mount_to_remove_text}",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            # Remove from the master list
            self.all_mounts_config = [m for m in self.all_mounts_config if m != mount_to_remove_data]
            
            # Store current filter to try and restore it
            previous_filter = self.current_filter_bucket_text
            self._update_bucket_filter_options() 
            # If previous filter is still valid, _update_bucket_filter_options should restore it.
            # If not, it will default. Then populate list based on new filter.
            self.populate_mounts_list()

    def get_configured_mounts(self):
        # Always return the full master list, not the filtered view
        return self.all_mounts_config
    
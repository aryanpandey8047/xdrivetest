from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QLineEdit, QTextEdit,
    QDialogButtonBox, QLabel, QTabWidget, QWidget, QScrollArea,
    QApplication
)
from PyQt6.QtCore import Qt
from datetime import datetime

def format_datetime_for_display(dt_obj):
    if isinstance(dt_obj, datetime):
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S %Z%z")
    return str(dt_obj)

class PropertiesDialog(QDialog):
    def __init__(self, s3_client, bucket_name, s3_key, is_folder, item_name, parent=None):
        super().__init__(parent)
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.s3_key = s3_key
        self.is_folder = is_folder
        self.item_name = item_name

        self.setWindowTitle(f"Properties: {self.item_name}")
        self.setMinimumSize(500, 400)

        main_layout = QVBoxLayout(self)
        self.tab_widget = QTabWidget()

        # --- General Tab ---
        general_tab = QWidget()
        general_layout = QFormLayout(general_tab)

        self.name_label = QLineEdit(self.item_name)
        self.name_label.setReadOnly(True)
        self.type_label = QLineEdit("Folder" if self.is_folder else "File") # More specific type later
        self.type_label.setReadOnly(True)
        self.s3_path_label = QLineEdit(f"s3://{self.bucket_name}/{self.s3_key}")
        self.s3_path_label.setReadOnly(True)

        general_layout.addRow("Name:", self.name_label)
        general_layout.addRow("Type:", self.type_label)
        general_layout.addRow("S3 Path:", self.s3_path_label)

        if not self.is_folder:
            self.size_label = QLineEdit("Loading...")
            self.size_label.setReadOnly(True)
            self.last_modified_label = QLineEdit("Loading...")
            self.last_modified_label.setReadOnly(True)
            self.etag_label = QLineEdit("Loading...")
            self.etag_label.setReadOnly(True)
            self.storage_class_label = QLineEdit("Loading...")
            self.storage_class_label.setReadOnly(True)
            self.encryption_label = QLineEdit("Loading...")
            self.encryption_label.setReadOnly(True)

            general_layout.addRow("Size:", self.size_label)
            general_layout.addRow("Last Modified:", self.last_modified_label)
            general_layout.addRow("ETag:", self.etag_label)
            general_layout.addRow("Storage Class:", self.storage_class_label)
            general_layout.addRow("Server-Side Encryption:", self.encryption_label)
        
        self.tab_widget.addTab(general_tab, "General")

        # --- Permissions Tab (Read-only ACLs for now) ---
        if not self.is_folder: # ACLs are per-object
            permissions_tab_scroll_area = QScrollArea()
            permissions_tab_scroll_area.setWidgetResizable(True)
            permissions_tab_widget = QWidget()
            permissions_layout = QVBoxLayout(permissions_tab_widget)
            
            self.acl_text_edit = QTextEdit("Loading ACLs...")
            self.acl_text_edit.setReadOnly(True)
            self.acl_text_edit.setFontFamily("Courier New") # Monospace for better formatting
            
            permissions_layout.addWidget(QLabel("Object ACL (Access Control List):"))
            permissions_layout.addWidget(self.acl_text_edit)
            permissions_tab_scroll_area.setWidget(permissions_tab_widget)
            self.tab_widget.addTab(permissions_tab_scroll_area, "Permissions (ACL)")


        main_layout.addWidget(self.tab_widget)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        button_box.accepted.connect(self.accept)
        main_layout.addWidget(button_box)

        self.load_properties()

    def load_properties(self):
        if self.is_folder:
            # For folders, most detailed properties aren't directly available from a single call
            # We might show number of items or total size if we decide to calculate it (can be slow)
            return

        if not self.s3_client:
            self.size_label.setText("Error: S3 client not available")
            return

        try:
            head = self.s3_client.head_object(Bucket=self.bucket_name, Key=self.s3_key)
            
            # Update General Tab
            size_bytes = head.get('ContentLength', 0)
            # Re-use format_size from main app if possible, or define locally
            self.size_label.setText(f"{size_bytes} bytes ({self.format_bytes(size_bytes)})")
            self.last_modified_label.setText(format_datetime_for_display(head.get('LastModified')))
            self.etag_label.setText(head.get('ETag', '').strip('"'))
            self.storage_class_label.setText(head.get('StorageClass', 'STANDARD'))
            self.encryption_label.setText(head.get('ServerSideEncryption', 'None'))
            
            file_type_ext = self.s3_key.split('.')[-1] if '.' in self.s3_key else "Unknown"
            mime_type = head.get('ContentType', 'application/octet-stream')
            self.type_label.setText(f"File ({mime_type})")


            # Load ACLs for Permissions Tab
            if hasattr(self, 'acl_text_edit'):
                try:
                    acl = self.s3_client.get_object_acl(Bucket=self.bucket_name, Key=self.s3_key)
                    acl_str = f"Owner: {acl['Owner']['DisplayName']} (ID: {acl['Owner']['ID']})\n\nGrants:\n"
                    for grant in acl['Grants']:
                        grantee = grant['Grantee']
                        grantee_type = grantee['Type']
                        grantee_id = grantee.get('ID', 'N/A')
                        grantee_display = grantee.get('DisplayName') or grantee.get('URI', grantee_id)
                        permission = grant['Permission']
                        acl_str += f"  - Grantee: {grantee_display} ({grantee_type})\n"
                        acl_str += f"    Permission: {permission}\n"
                    self.acl_text_edit.setText(acl_str)
                except Exception as e_acl:
                    self.acl_text_edit.setText(f"Error loading ACLs: {e_acl}\n\nThis might be due to permissions (s3:GetObjectAcl required).")

        except Exception as e:
            error_text = f"Error loading properties: {e}"
            if hasattr(self, 'size_label'): self.size_label.setText(error_text)
            if hasattr(self, 'last_modified_label'): self.last_modified_label.setText("")
            # ... and for other fields
            QApplication.instance().main_window.status_bar.showMessage(f"Error fetching properties: {e}", 5000)


    def format_bytes(self, size_bytes): # Local copy for dialog independence
        if size_bytes is None: return ""
        if size_bytes == 0: return "0 B"
        size_name = ("B", "KB", "MB", "GB", "TB")
        i = 0
        power = 1024
        while size_bytes >= power and i < len(size_name) - 1:
            size_bytes /= power
            i += 1
        return f"{size_bytes:.2f} {size_name[i]}"
    
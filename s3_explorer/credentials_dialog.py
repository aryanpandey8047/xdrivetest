from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QLineEdit,
    QDialogButtonBox, QLabel, QMessageBox, QComboBox
)
from PyQt6.QtCore import Qt

# Common AWS Regions - can be expanded
AWS_REGIONS = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "af-south-1", "ap-east-1", "ap-south-1", "ap-northeast-3",
    "ap-northeast-2", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1",
    "ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2",
    "eu-south-1", "eu-west-3", "eu-north-1", "me-south-1", "sa-east-1"
]

class CredentialsDialog(QDialog):
    def __init__(self, parent=None, current_config=None):
        super().__init__(parent)
        self.setWindowTitle("AWS Credentials Setup")
        self.setModal(True)
        self.current_config = current_config if current_config else {}

        layout = QVBoxLayout(self)

        form_layout = QFormLayout()
        self.access_key_edit = QLineEdit(self.current_config.get("AWS_ACCESS_KEY_ID", ""))
        self.secret_key_edit = QLineEdit(self.current_config.get("AWS_SECRET_ACCESS_KEY", ""))
        self.secret_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.region_combo = QComboBox()
        self.region_combo.addItems(AWS_REGIONS)
        self.region_combo.setEditable(True) # Allow custom region input
        current_region = self.current_config.get("AWS_DEFAULT_REGION", "")
        if current_region and current_region in AWS_REGIONS:
            self.region_combo.setCurrentText(current_region)
        elif current_region: # If it's a custom region not in the list
             self.region_combo.insertItem(0, current_region)
             self.region_combo.setCurrentIndex(0)
        else:
            self.region_combo.setCurrentText("us-east-1") # Default fallback

        self.default_bucket_edit = QLineEdit(self.current_config.get("DEFAULT_S3_BUCKET", ""))

        form_layout.addRow(QLabel("AWS Access Key ID:"), self.access_key_edit)
        form_layout.addRow(QLabel("AWS Secret Access Key:"), self.secret_key_edit)
        form_layout.addRow(QLabel("AWS Default Region:"), self.region_combo)
        form_layout.addRow(QLabel("Default S3 Bucket (Optional):"), self.default_bucket_edit)

        layout.addLayout(form_layout)

        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.button_box.accepted.connect(self.validate_and_accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

    def validate_and_accept(self):
        if not self.access_key_edit.text().strip():
            QMessageBox.warning(self, "Input Error", "AWS Access Key ID cannot be empty.")
            return
        if not self.secret_key_edit.text().strip():
            # Secret key can technically be empty if using session tokens (not supported here yet)
            # For access/secret key pair, it's required.
            QMessageBox.warning(self, "Input Error", "AWS Secret Access Key cannot be empty for key-based auth.")
            return
        if not self.region_combo.currentText().strip():
            QMessageBox.warning(self, "Input Error", "AWS Default Region cannot be empty.")
            return
        self.accept()

    def get_credentials(self):
        return {
            "AWS_ACCESS_KEY_ID": self.access_key_edit.text().strip(),
            "AWS_SECRET_ACCESS_KEY": self.secret_key_edit.text(), # Don't strip secret
            "AWS_DEFAULT_REGION": self.region_combo.currentText().strip(),
            "DEFAULT_S3_BUCKET": self.default_bucket_edit.text().strip()
        }
    
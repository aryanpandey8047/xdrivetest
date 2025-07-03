
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QLineEdit,
    QHBoxLayout,QWidget,
    QToolBar, QStatusBar, QMessageBox, QDialog,
    QLabel, QFileDialog, QTabWidget,
    QProgressDialog, QInputDialog, QComboBox, QStyle, QSizePolicy
)
from PyQt6.QtGui import QClipboard

def generate_shareable_s3_link(s3_client, s3_key: str, bucket_name: str, expiration_seconds: int, item_name: str, parent_widget):
    """
    Generate a pre-signed URL for an S3 object and copy it to the clipboard.
    
    Args:
        s3_client: The boto3 S3 client instance.
        s3_key (str): The S3 object key.
        bucket_name (str): The S3 bucket name.
        expiration_seconds (int): URL expiration time in seconds.
        item_name (str): Display name of the item for user feedback.
        parent_widget: The parent widget for QMessageBox (typically the main window).
    """
    if not s3_client:
        QMessageBox.warning(parent_widget, "S3 Error", "S3 client not connected. Cannot generate shareable link.")
        parent_widget.update_status_bar_message_slot("Cannot generate link: No S3 connection.", 5000)
        return

    try:
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': bucket_name, 'Key': s3_key},
            ExpiresIn=expiration_seconds
        )
        
        clipboard = QApplication.clipboard()
        clipboard.setText(presigned_url, QClipboard.Mode.Clipboard)
        
        # Provide user feedback
        time_str = {
            60: "1 minute",
            300: "5 minutes",
            1800: "30 minutes",
            3600: "1 hour"
        }.get(expiration_seconds, f"{expiration_seconds} seconds")
        parent_widget.update_status_bar_message_slot(f"Shareable link for '{item_name}' ({time_str}) copied to clipboard.", 5000)
        QMessageBox.information(
            parent_widget,
            "Link Generated",
            f"Pre-signed URL for 's3://{bucket_name}/{s3_key}' (expires in {time_str}) has been copied to the clipboard."
        )
        
    except Exception as e:
        error_msg = f"Failed to generate shareable link for '{item_name}': {str(e)}"
        parent_widget.update_status_bar_message_slot(error_msg, 7000)
        QMessageBox.critical(parent_widget, "Link Generation Error", error_msg)
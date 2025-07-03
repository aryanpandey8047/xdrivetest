import os
from PyQt6.QtCore import QThread, pyqtSignal


class DownloadFolderWorker(QThread):
    progress_updated = pyqtSignal(int, int, str)  # current, total, key
    finished = pyqtSignal(str)  # local_folder_path
    error = pyqtSignal(str)
    canceled = pyqtSignal()

    def __init__(self, s3_client, bucket, s3_key, local_folder):
        super().__init__()
        self.s3_client = s3_client
        self.bucket = bucket
        self.s3_key = s3_key
        self.local_folder = local_folder
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def run(self):
        try:
            # List all keys
            paginator = self.s3_client.get_paginator('list_objects_v2')
            all_keys = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=self.s3_key):
                for obj in page.get("Contents", []):
                    all_keys.append(obj['Key'])

            total = len(all_keys)
            if total == 0:
                self.error.emit("This folder contains no files.")
                return

            for i, key in enumerate(all_keys):
                if self._cancel:
                    self.canceled.emit()
                    return

                if key.endswith('/'):
                    rel_dir = key[len(self.s3_key):].lstrip('/').rstrip('/')
                    if rel_dir:
                        os.makedirs(os.path.join(self.local_folder, rel_dir), exist_ok=True)
                    self.progress_updated.emit(i + 1, total, key)
                    continue

                rel_path = key[len(self.s3_key):].lstrip('/')
                local_path = os.path.join(self.local_folder, rel_path)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                self.s3_client.download_file(self.bucket, key, local_path)

                self.progress_updated.emit(i + 1, total, key)

            self.finished.emit(self.local_folder)

        except Exception as e:
            self.error.emit(str(e))

# zip_worker.py
import os
import time
import zipfile
from PyQt6.QtCore import QThread, pyqtSignal


class ZipFolderWorker(QThread):
    progress_updated = pyqtSignal(int, int, int, str)  # current, total, percent, eta_str
    finished = pyqtSignal(str)  # zip path
    error = pyqtSignal(str)
    canceled = pyqtSignal()

    def __init__(self, source_folder: str, zip_path: str):
        super().__init__()
        self.source_folder = source_folder
        self.zip_path = zip_path
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def run(self):
        try:
            # Step 1: Collect entries
            entries = []
            for root, dirs, files in os.walk(self.source_folder):
                rel_dir = os.path.relpath(root, os.path.dirname(self.source_folder))
                if not files and not dirs:
                    entries.append((None, rel_dir))
                for file in files:
                    full_path = os.path.join(root, file)
                    arcname = os.path.relpath(full_path, os.path.dirname(self.source_folder))
                    entries.append((full_path, arcname))

            total = len(entries)
            if total == 0:
                self.error.emit("No files or folders to zip.")
                return

            # Step 2: Begin zipping
            start_time = time.time()
            with zipfile.ZipFile(self.zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for i, (path, arcname) in enumerate(entries):
                    if self._cancel:
                        zipf.close()
                        if os.path.exists(self.zip_path):
                            os.remove(self.zip_path)
                        self.canceled.emit()
                        return

                    if path is None:
                        zip_info = zipfile.ZipInfo(arcname + '/')
                        zipf.writestr(zip_info, '')
                    else:
                        zipf.write(path, arcname)

                    elapsed = time.time() - start_time
                    processed = i + 1
                    percent = int((processed / total) * 100)
                    eta = (elapsed / processed) * (total - processed)
                    eta_str = time.strftime('%M:%S', time.gmtime(eta))
                    self.progress_updated.emit(processed, total, percent, eta_str)

            self.finished.emit(self.zip_path)

        except Exception as e:
            self.error.emit(str(e))

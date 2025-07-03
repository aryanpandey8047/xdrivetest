import os
from datetime import datetime
from PyQt6.QtCore import QObject, pyqtSignal
# s3_client is needed for checking S3 mtime, operation_manager for re-upload

class TempFileManager(QObject):
    temp_file_modified_status_changed = pyqtSignal(str, bool) # s3_key, is_modified
    # This signal can be used by S3Explorer to update "Save" action state

    def __init__(self, parent=None):
        super().__init__(parent)
        self.opened_temp_files = {} # s3_key: {temp_path, original_s3_mtime, local_mtime_on_open, s3_bucket}

    def track_opened_temp_file(self, s3_key, temp_path, s3_bucket, original_s3_mtime, local_mtime_on_open):
        self.opened_temp_files[s3_key] = {
            'temp_path': temp_path,
            'original_s3_mtime': original_s3_mtime,
            'local_mtime_on_open': local_mtime_on_open,
            's3_bucket': s3_bucket
        }
        print(f"TEMP_FILE_HANDLER: Tracking {s3_key} at {temp_path}")
        self.temp_file_modified_status_changed.emit(s3_key, False) # Initially not modified

    def handle_temp_file_upload_success(self, s3_key, s3_bucket, uploaded_local_path, s3_client_ref):
        """
        Handles post-successful-upload tasks for a tracked temporary file.
        This means the S3 object has been updated with the contents of 'uploaded_local_path'.
        We need to update our internal tracking to reflect this synced state.

        Args:
            s3_key (str): The S3 object key (which is the key in self.opened_temp_files).
            s3_bucket (str): The S3 bucket.
            uploaded_local_path (str): The path of the local temporary file whose contents were uploaded.
            s3_client_ref (boto3.S3.Client): Reference to the S3 client for fetching new S3 mtime.
        """
        if s3_key not in self.opened_temp_files:
            print(f"TEMP_FILE_HANDLER: S3 Key '{s3_key}' (from local path '{uploaded_local_path}') "
                  f"not found in tracked temp files during upload success handling. Ignoring mtime update.")
            # Potentially, the file was untracked between the upload queue and completion.
            # We should not emit temp_file_modified_status_changed if we can't find the tracking entry.
            return

        tracked_file_data = self.opened_temp_files[s3_key]
        updated_successfully = False

        try:
            # 1. Update local_mtime_on_open to the current mtime of the uploaded local file.
            # This makes the local file appear "not modified" relative to its last synced state.
            if uploaded_local_path and os.path.exists(uploaded_local_path):
                new_local_mtime = os.path.getmtime(uploaded_local_path)
                tracked_file_data['local_mtime_on_open'] = new_local_mtime
                print(f"TEMP_FILE_HANDLER: Updated 'local_mtime_on_open' for '{s3_key}' to: "
                      f"{new_local_mtime} ({datetime.fromtimestamp(new_local_mtime).strftime('%H:%M:%S.%f')})")
                updated_successfully = True # At least local mtime was updated
            else:
                print(f"TEMP_FILE_HANDLER: Warning - Local path '{uploaded_local_path}' for '{s3_key}' "
                      f"not found or invalid during mtime update after S3 upload.")
                # If local path is gone, we can't get its mtime. The file is effectively "gone" locally.
                # We might still want to update S3 mtime if client is available.

            # 2. Update original_s3_mtime to the mtime of the newly uploaded S3 object.
            if s3_client_ref:
                try:
                    head = s3_client_ref.head_object(Bucket=s3_bucket, Key=s3_key)
                    new_s3_mtime = head.get('LastModified')
                    if new_s3_mtime:
                        tracked_file_data['original_s3_mtime'] = new_s3_mtime
                        print(f"TEMP_FILE_HANDLER: Updated 'original_s3_mtime' for '{s3_key}' to: {new_s3_mtime}")
                        updated_successfully = True # S3 mtime also updated
                    else:
                        print(f"TEMP_FILE_HANDLER: Warning - head_object for '{s3_key}' did not return LastModified.")
                except Exception as e_head:
                    print(f"TEMP_FILE_HANDLER: Error fetching new S3 mtime for '{s3_key}' after upload: {e_head}")
                    # If fetching S3 mtime fails, updated_successfully remains based on local mtime update.
            else:
                print(f"TEMP_FILE_HANDLER: No S3 client ref provided to update 'original_s3_mtime' for '{s3_key}'.")

        except Exception as e:
            print(f"TEMP_FILE_HANDLER: General error updating mtimes for temp file '{s3_key}' after S3 upload: {e}")
            # In case of an error here, updated_successfully might be False or True based on prior steps.

        finally:
            # Emit the signal. If mtimes were updated, the file is now considered "not modified" (False).
            # If there was an error updating mtimes, it's safer to assume it might still be considered modified
            # by a subsequent check if the mtimes didn't get fully reset.
            # However, since the S3 upload *was* successful, the primary goal is to reflect it's no longer pending save.
            # So, always emit False if the s3_key was found, because the S3 side is now up-to-date.
            # The next `check_single_temp_file_modified_status` will re-evaluate based on these new mtimes.
            if s3_key in self.opened_temp_files: # Re-check in case it was removed by another thread (unlikely here)
                 self.temp_file_modified_status_changed.emit(s3_key, False)
                 print(f"TEMP_FILE_HANDLER: Emitted 'temp_file_modified_status_changed' for '{s3_key}' with False (sync completed).")


    def check_single_temp_file_modified_status(self, s3_key, s3_client_ref):
        """Checks one file, returns modification status and conflict info."""
        # Returns: (is_locally_modified, s3_has_newer_version, current_s3_mtime, current_local_mtime)
        if s3_key not in self.opened_temp_files:
            return False, False, None, None

        data = self.opened_temp_files[s3_key]
        temp_path = data['temp_path']
        s3_bucket_of_temp_file = data['s3_bucket']
        original_s3_mtime_on_open = data.get('original_s3_mtime') # Can be None
        local_mtime_when_opened = data['local_mtime_on_open']

        if not os.path.exists(temp_path):
            self.opened_temp_files.pop(s3_key, None)
            self.temp_file_modified_status_changed.emit(s3_key, False) # No longer exists, so not modified
            return False, False, None, None
        
        is_locally_modified = False
        s3_has_newer_version = False
        current_s3_mtime_val = None
        current_local_mtime_val = None

        try:
            current_local_mtime_val = os.path.getmtime(temp_path)
            print(f"  TEMP_FILE_MGR Check '{s3_key}':")
            print(f"    Local mtime when opened: {local_mtime_when_opened} ({datetime.fromtimestamp(local_mtime_when_opened).strftime('%H:%M:%S.%f') if local_mtime_when_opened else 'N/A'})")
            print(f"    Current local mtime:     {current_local_mtime_val} ({datetime.fromtimestamp(current_local_mtime_val).strftime('%H:%M:%S.%f')})")
            print(f"    Is modified condition: {current_local_mtime_val} > {local_mtime_when_opened + 0.5} ?")

            if current_local_mtime_val > local_mtime_when_opened + 0.5:
                is_locally_modified = True
                print(f"    -> MARKED AS LOCALLY MODIFIED")
            else:
                print(f"    -> NOT LOCALLY MODIFIED")

            if is_locally_modified and original_s3_mtime_on_open and s3_client_ref:
                head_info = s3_client_ref.head_object(Bucket=s3_bucket_of_temp_file, Key=s3_key)
                current_s3_mtime_val = head_info.get('LastModified')
                if current_s3_mtime_val and original_s3_mtime_on_open:
                    # Naive datetime comparison ( stripping tzinfo )
                    naive_current_s3 = current_s3_mtime_val.replace(tzinfo=None) if current_s3_mtime_val.tzinfo else current_s3_mtime_val
                    naive_original_s3 = original_s3_mtime_on_open.replace(tzinfo=None) if original_s3_mtime_on_open.tzinfo else original_s3_mtime_on_open
                    if naive_current_s3 > naive_original_s3:
                        s3_has_newer_version = True
            
            self.temp_file_modified_status_changed.emit(s3_key, is_locally_modified)
            return is_locally_modified, s3_has_newer_version, current_s3_mtime_val, current_local_mtime_val

        except Exception as e: # Covers OSError for mtime and S3 client errors
            print(f"TEMP_FILE_HANDLER: Error checking modification for {s3_key}: {e}")
            # Emit current known state, or assume not modified if error
            current_mod_state = False
            if temp_path and os.path.exists(temp_path):
                 try: current_mod_state = os.path.getmtime(temp_path) > local_mtime_when_opened + 0.5
                 except: pass
            self.temp_file_modified_status_changed.emit(s3_key, current_mod_state)
            return current_mod_state, False, None, current_local_mtime_val


    def get_temp_file_data(self, s3_key):
        return self.opened_temp_files.get(s3_key)

    def get_all_tracked_files(self):
        return list(self.opened_temp_files.keys())

    def cleanup_temp_file(self, s3_key):
        data = self.opened_temp_files.pop(s3_key, None)
        if data and data.get('temp_path') and os.path.exists(data['temp_path']):
            try:
                os.remove(data['temp_path'])
                print(f"TEMP_FILE_HANDLER: Cleaned up temp file for {s3_key}: {data['temp_path']}")
                self.temp_file_modified_status_changed.emit(s3_key, False) # No longer exists
            except OSError as e:
                print(f"TEMP_FILE_HANDLER: Error deleting temp file {data['temp_path']} for {s3_key}: {e}")
        elif data:
             print(f"TEMP_FILE_HANDLER: Temp file for {s3_key} not found or already removed path: {data.get('temp_path')}")


    def cleanup_all_temp_files(self):
        for s3_key in list(self.opened_temp_files.keys()):
            self.cleanup_temp_file(s3_key)
            
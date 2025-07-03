import os
import time
import threading
from watchdog.events import FileSystemEventHandler as WatchdogFileSystemEventHandler


class LiveEditFileChangeHandler(WatchdogFileSystemEventHandler):
    def __init__(self, s3_explorer_instance: 'S3Explorer'):
        super().__init__()
        self.app = s3_explorer_instance # Reference to the main S3Explorer app

    def _should_ignore_event(self, event_path):
        """Checks if an event for this path should be ignored based on TempFileManager state."""
        norm_path = os.path.normpath(event_path)
        # Check TempFileManager's tracking; it might have its own ignore flags or mtime checks
        # For this specific watchdog, we primarily care if it's a tracked file.
        # The main S3Explorer's _handle_live_edit_upload will do the more detailed check
        # if file_info exists.
        
        # A simple check against recently written files by the app itself (e.g., during download)
        # This would require S3Explorer to temporarily flag paths it's writing to.
        # For now, we rely on debouncing and the check within _handle_live_edit_upload.
        
        # A more direct ignore: S3Explorer could have a set of paths it's currently manipulating.
        # if norm_path in self.app.currently_manipulated_live_edit_paths: return True

        file_info = None
        for s3k, data in self.app.temp_file_manager.opened_temp_files.items():
            if os.path.normpath(data['temp_path']) == norm_path:
                file_info = data
                break
        
        if file_info and time.time() < file_info.get("ignore_watchdog_until_sync", 0):
            # This 'ignore_watchdog_until_sync' would be set by S3Explorer just before queueing an upload
            # and cleared after successful upload or if upload fails.
            print(f"LiveEditWatcher: Path {norm_path} currently being synced or recently synced. Ignoring.")
            return True
        
        filename = os.path.basename(norm_path)
        if filename.startswith('~') or filename.endswith('.tmp') or filename.endswith('~'): # Common editor temp/backup
            print(f"LiveEditWatcher: Ignoring editor temp/backup pattern: {norm_path}")
            return True
            
        return False


    def on_modified(self, event):
        if event.is_directory:
            return
        
        event_path = os.path.normpath(event.src_path)
        if self._should_ignore_event(event_path):
            return

        # Check if this path is one of the "live edit" files we are tracking
        # The actual tracking (s3_key, bucket) is now in self.app.temp_file_manager
        is_live_edit_file = False
        for s3_key, data in self.app.temp_file_manager.opened_temp_files.items():
            if os.path.normpath(data['temp_path']) == event_path:
                is_live_edit_file = True
                break
        
        if is_live_edit_file:
            print(f"LiveEditWatcher: Modified '{event_path}' (tracked live edit file).")

            # Debounce the upload
            if event_path in self.app._live_edit_debounce_timers:
                self.app._live_edit_debounce_timers[event_path].cancel()
            
            timer = threading.Timer(
                1.5, # 1.5 second debounce
                lambda p=event_path: self.app._handle_live_edit_upload(p)
            )
            timer.daemon = True
            timer.start()
            self.app._live_edit_debounce_timers[event_path] = timer
        # else:
            # print(f"LiveEditWatcher: Modified '{event_path}' (not a tracked live edit file).")

    def on_deleted(self, event):
        if event.is_directory:
            return
        event_path = os.path.normpath(event.src_path)
        key_to_remove = None
        # Access self.app.temp_file_manager here
        # Use list() to create a copy for safe iteration if modifying the dict
        for s3_key, data in list(self.app.temp_file_manager.opened_temp_files.items()):
            if os.path.normpath(data['temp_path']) == event_path:
                key_to_remove = s3_key # This is the S3 key used for tracking
                break
        if key_to_remove: # If the deleted local path was a tracked temp file
            print(f"LiveEditWatcher: Tracked live edit file '{event_path}' (S3 Key: {key_to_remove}) deleted locally. Untracking.")
            # temp_file_manager.cleanup_temp_file also removes the file from disk if it exists.
            # Since the event IS a delete, the file is already gone. We just need to untrack.
            self.app.temp_file_manager.opened_temp_files.pop(key_to_remove, None) # Just remove from dict
            
            self.app.update_status_bar_message_slot(f"Local temp for {os.path.basename(event_path)} (S3: {key_to_remove}) deleted.", 3000)
            self.app.update_save_action_state()


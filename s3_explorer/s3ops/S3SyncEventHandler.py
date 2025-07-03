import os
import time
import threading # For using threading.Timer

from watchdog.events import FileSystemEventHandler, FileSystemMovedEvent
from PyQt6.QtCore import QObject, pyqtSignal # QTimer removed, QObject kept for signals

from s3ops.S3Operation import S3Operation, S3OpType

class S3SyncEventHandler(FileSystemEventHandler, QObject): # Inherit QObject for pyqtSignal
    status_message_requested = pyqtSignal(str, int) # message, timeout

    deletion_confirmation_requested = pyqtSignal(str, str, str, bool)

    DEBOUNCE_DELAY_MS = 1500  # Milliseconds to wait before processing create/modify

    def __init__(self, local_mount_path, s3_bucket, s3_prefix,
                 s3_op_queue_ref, main_window_ref=None, mount_manager_ref=None):
        FileSystemEventHandler.__init__(self)
        QObject.__init__(self) # Initialize QObject for signals

        self.local_mount_path = os.path.normpath(local_mount_path)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.strip('/')
        self.s3_op_queue = s3_op_queue_ref
        self.main_window = main_window_ref # For general app context if needed
        self.mount_manager = mount_manager_ref # Crucial for centralized ignore logic

        self._ignore_paths_with_expiry = {} # path: expiry_timestamp
        self._debounce_timers = {}          # path: threading.Timer instance

    def add_ignore_path(self, path, duration=2.0):
        """
        Temporarily ignore events for a given path.
        This method is called by MountManager via add_ignore_path_to_specific_handler.
        It uses timestamps for expiry, managed within this (watchdog) thread.
        """
        norm_path = os.path.normpath(path)
        expiry_time = time.time() + duration
        self._ignore_paths_with_expiry[norm_path] = expiry_time
        print(f"S3SyncEH ({self.local_mount_path}): Ignoring '{norm_path}' until {time.strftime('%H:%M:%S', time.localtime(expiry_time))}")
        # Schedule a cleanup for this ignore entry to prevent unbounded growth of the dict
        # This timer runs in a new thread, so it's fine.
        cleanup_timer = threading.Timer(
            duration + 0.5, # Run slightly after expiry
            lambda p=norm_path: self._clear_expired_ignore_entry(p)
        )
        cleanup_timer.daemon = True # Allow main program to exit even if timer is pending
        cleanup_timer.start()

    def _clear_expired_ignore_entry(self, path):
        """Removes an ignore entry if it has truly expired."""
        if path in self._ignore_paths_with_expiry:
            if time.time() >= self._ignore_paths_with_expiry[path]:
                del self._ignore_paths_with_expiry[path]
                # print(f"S3SyncEH ({self.local_mount_path}): Cleaned up expired ignore for '{path}'")

    def _should_ignore(self, path):
        norm_path = os.path.normpath(path)

        # QTimer here is tricky because event handler runs in watchdog thread.
        # For simplicity, main_window can manage the QTimer if it calls this.
        # Or, we use a Python timer if this method is called from non-GUI thread.
        # Let's assume main_window calls this and thus handles the QTimer.
        # If called from watchdog thread, a Python threading.Timer would be needed or signal back to main thread.
        # For now, let's assume main_window calls this. If not, this QTimer needs to be handled carefully.
        # A safer way for _ignore_paths managed within watchdog thread:
        # Store (path, expiry_time) and check against time.time() in _get_s3_key.
        # This avoids QTimer issues from non-GUI threads.

        # Check timestamp-based ignores
        expiry = self._ignore_paths_with_expiry.get(norm_path)
        if expiry:
            if time.time() < expiry:
                # print(f"S3SyncEH ({self.local_mount_path}): Actively ignoring (timestamp) '{norm_path}'")
                return True
            else: # Expired
                del self._ignore_paths_with_expiry[norm_path]
                # print(f"S3SyncEH ({self.local_mount_path}): Expired timestamp ignore for '{norm_path}'")
        
        # Check common temporary/system file patterns
        filename = os.path.basename(norm_path)
        if filename.startswith('~') or \
           filename.startswith('.') or \
           filename.endswith('.tmp') or \
           filename.endswith('.part') or \
           filename.endswith('.crdownload') or \
           filename.endswith('.swp') or \
           filename.endswith('.swx') or \
           filename == 'desktop.ini' or \
           filename == 'Thumbs.db':
            # print(f"S3SyncEH ({self.local_mount_path}): Ignoring pattern: '{norm_path}'")
            return True
        return False

    def _get_s3_key(self, local_path, is_dir_hint=False):
        norm_local_path = os.path.normpath(local_path)

        # Check ignore paths (more robustly, without QTimer directly in this thread)
        # This check is simplified; a timestamp-based expiry for self._ignore_paths would be better if add_ignore_path
        # is called frequently from the watchdog thread itself.

        if not norm_local_path.startswith(self.local_mount_path):
            # print(f"S3SyncEH ({self.local_mount_path}): Path '{norm_local_path}' is outside mount path.")
            return None

        relative_path = os.path.relpath(norm_local_path, self.local_mount_path)
        if relative_path == '.': relative_path = '' # Root of the mount
        
        relative_path = relative_path.replace(os.path.sep, '/') # Ensure S3-style paths

        s3_key_parts = []
        if self.s3_prefix:
            s3_key_parts.append(self.s3_prefix)
        if relative_path:
            s3_key_parts.append(relative_path)
        
        s3_key = "/".join(s3_key_parts)

        if is_dir_hint and s3_key and not s3_key.endswith('/'):
            s3_key += '/'
        
        return s3_key

    def _handle_file_event_debounced(self, event_path, event_is_directory, event_type_str):
        """Generic handler for create/modify with debouncing using threading.Timer."""
        if self._should_ignore(event_path):
            # print(f"S3SyncEH ({self.local_mount_path}): Ignoring debounced event for '{event_path}'")
            return

        # Path key for debounce dictionary
        path_key_for_debounce = os.path.normpath(event_path)

        def action_after_debounce():
            # print(f"S3SyncEH ({self.local_mount_path}): Debounced action for '{event_path}' (is_dir={event_is_directory})")
            s3_key = self._get_s3_key(event_path, event_is_directory)
            if not s3_key:
                # print(f"S3SyncEH ({self.local_mount_path}): Could not get S3 key for '{event_path}' after debounce.")
                return

            if os.path.exists(event_path) or event_type_str == "PreDelete": # Allow pre-delete to proceed even if file gone
                # Add to MountManager's ignore list *before* queueing S3 operation
                # This specific handler instance (self) will then ignore it via _should_ignore
                if self.mount_manager:
                    self.mount_manager.add_ignore_path_to_specific_handler(self.local_mount_path, event_path, duration=3.0)
                else: # Fallback if no mount_manager (less ideal, direct ignore)
                    self.add_ignore_path(event_path, duration=3.0) # add_ignore_path is now internal to handler

                op = None
                if event_is_directory:
                    if event_type_str == "Created":
                        op = S3Operation(S3OpType.CREATE_FOLDER, self.s3_bucket, key=s3_key)
                        self.status_message_requested.emit(f"Mount: Create Dir '{os.path.basename(event_path)}' -> S3", 3000)
                    # Modified for directory is usually not an S3 content change unless we track metadata.
                    # For pre-delete of directory, S3OpType.DELETE_FOLDER will be used.
                else: # File
                    if event_type_str == "Created" or event_type_str == "Modified":
                        op = S3Operation(S3OpType.UPLOAD_FILE, self.s3_bucket, key=s3_key, local_path=event_path)
                        self.status_message_requested.emit(f"Mount: Upload '{os.path.basename(event_path)}' -> S3", 3000)
                
                if op:
                    print(f"S3SyncEH ({self.local_mount_path}): Queuing {op.op_type.name} for S3 Key '{s3_key}' from local '{event_path}'")
                    self.s3_op_queue.put(op)
            else:
                print(f"S3SyncEH ({self.local_mount_path}): File '{event_path}' no longer exists after debounce. Skipping S3 op.")
                self.status_message_requested.emit(f"Skipped S3 op (file gone): {os.path.basename(event_path)}", 3000)
            
            # Clean up the timer from the dictionary
            if path_key_for_debounce in self._debounce_timers:
                del self._debounce_timers[path_key_for_debounce]

        # Clear any existing timer for this path and start a new one
        if path_key_for_debounce in self._debounce_timers:
            self._debounce_timers[path_key_for_debounce].cancel()
        
        timer = threading.Timer(self.DEBOUNCE_DELAY_MS / 1000.0, action_after_debounce)
        timer.daemon = True
        timer.start()
        self._debounce_timers[path_key_for_debounce] = timer
        # print(f"S3SyncEH ({self.local_mount_path}): Debounce timer started for '{event_path}'")


    def on_created(self, event):
        super().on_created(event)
        # print(f"S3SyncEH Raw ON_CREATED: {event.src_path}, is_dir={event.is_directory}")
        self._handle_file_event_debounced(event.src_path, event.is_directory, "Created")

    def on_modified(self, event):
        super().on_modified(event)
        if event.is_directory: # Typically, directory modifications are metadata, not content to sync to S3
            # print(f"S3SyncEH ({self.local_mount_path}): Ignoring directory modification: '{event.src_path}'")
            return
        # print(f"S3SyncEH Raw ON_MODIFIED: {event.src_path}")
        self._handle_file_event_debounced(event.src_path, False, "Modified") # is_directory is False for file modifications

    def on_deleted(self, event):
        super().on_deleted(event)
        
        local_path_deleted_norm = os.path.normpath(event.src_path)

        if self._should_ignore(local_path_deleted_norm):
            # print(f"S3SyncEH ({self.local_mount_path}): Ignoring delete event for '{local_path_deleted_norm}' due to active ignore rule.")
            return

        local_path_basename = os.path.basename(local_path_deleted_norm)
        
        # Heuristic: if the path name has no file extension, it's more likely it was a directory.
        # This is imperfect. A better approach might involve checking against known S3 prefixes,
        # but for a generic watchdog, this is a common starting point.
        was_likely_a_directory_locally = '.' not in local_path_basename
        # Another heuristic: if the original path ended with a separator before normalization,
        # though event.src_path usually doesn't provide this reliably for deletes.

        # Get potential S3 keys. _get_s3_key should be robust.
        # is_dir_hint=True will add a trailing slash if not present, suitable for folder operations.
        # is_dir_hint=False will not add a trailing slash, suitable for file operations.
        s3_key_if_it_was_file = self._get_s3_key(local_path_deleted_norm, is_dir_hint=False)
        s3_key_if_it_was_folder = self._get_s3_key(local_path_deleted_norm, is_dir_hint=True) 

        # Determine the most appropriate S3 key and if it's a folder operation
        s3_key_to_consider_for_delete = None
        is_folder_delete_operation = False

        if was_likely_a_directory_locally:
            # If local path looked like a dir, we prefer the S3 key formatted as a folder.
            if s3_key_if_it_was_folder:
                s3_key_to_consider_for_delete = s3_key_if_it_was_folder
                is_folder_delete_operation = True
            # Fallback if folder key generation failed for some reason but file key is valid
            elif s3_key_if_it_was_file: 
                s3_key_to_consider_for_delete = s3_key_if_it_was_file
                is_folder_delete_operation = False # Treat as file then
        else: # Local path looked like a file
            if s3_key_if_it_was_file:
                s3_key_to_consider_for_delete = s3_key_if_it_was_file
                is_folder_delete_operation = False
            # Fallback if file key failed but folder key is valid (unusual for file-like local path)
            elif s3_key_if_it_was_folder:
                s3_key_to_consider_for_delete = s3_key_if_it_was_folder
                is_folder_delete_operation = True 


        if not s3_key_to_consider_for_delete:
            print(f"S3SyncEH ({self.local_mount_path}): Could not determine a valid S3 key for deleted local path '{local_path_deleted_norm}'. Ignoring event.")
            return

        item_type_for_log = "Folder" if is_folder_delete_operation else "File"
        print(f"S3SyncEH ({self.local_mount_path}): Local item '{local_path_deleted_norm}' deleted.")
        print(f"  -> Potential S3 {item_type_for_log} for deletion: 's3://{self.s3_bucket}/{s3_key_to_consider_for_delete}'")
        print(f"  -> Requesting user confirmation for S3 deletion.")

        # Emit the signal to the main GUI thread for user confirmation.
        # The main_window (S3Explorer) will handle the QMessageBox and then queue the S3Operation if confirmed.
        self.deletion_confirmation_requested.emit(
            local_path_deleted_norm,    # The local path that was deleted
            s3_key_to_consider_for_delete, # The S3 key to potentially delete
            self.s3_bucket,             # The S3 bucket
            is_folder_delete_operation  # Our best guess if this S3 key represents a folder
        )
        
        # DO NOT queue the S3Operation directly here anymore.
        # The S3Explorer slot connected to deletion_confirmation_requested will do that.

        # The MountManager ignore for this path (if needed after a confirmed S3 delete)
        # would be best handled by S3Explorer after the user confirms and the S3 delete op is queued.
        # Or, if the user cancels, no ignore is needed for this specific delete event.
        # For now, we remove the direct call to self.mount_manager.add_ignore_path_to_specific_handler here.
        # If an S3 delete is confirmed and happens, and if there's a risk of S3 client syncing back
        # the deletion (which is unlikely for delete ops), then S3Explorer could manage that ignore.

    def on_moved(self, event: FileSystemMovedEvent):
        super().on_moved(event)
        # print(f"S3SyncEH Raw ON_MOVED: {event.src_path} -> {event.dest_path}, is_dir={event.is_directory}")

        src_path_norm = os.path.normpath(event.src_path)
        dest_path_norm = os.path.normpath(event.dest_path)

        # If source was ignored, the whole operation might be an artifact of something we did.
        # If dest is to be ignored (e.g. moving into .git), then also skip.
        if self._should_ignore(src_path_norm) or self._should_ignore(dest_path_norm):
            # print(f"S3SyncEH ({self.local_mount_path}): Ignoring move event involving '{src_path_norm}' or '{dest_path_norm}'")
            return

        s3_key_old = self._get_s3_key(src_path_norm, event.is_directory)
        s3_key_new = self._get_s3_key(dest_path_norm, event.is_directory)

        # Case 1: Moved completely out of the watched directory (old key valid, new key None)
        if s3_key_old and not s3_key_new:
            print(f"S3SyncEH ({self.local_mount_path}): Item '{src_path_norm}' moved out of watched scope. Deleting from S3: '{s3_key_old}'")
            op_type_del = S3OpType.DELETE_FOLDER if event.is_directory else S3OpType.DELETE_OBJECT
            op_del = S3Operation(op_type_del, self.s3_bucket, key=s3_key_old)
            if self.mount_manager: self.mount_manager.add_ignore_path_to_specific_handler(self.local_mount_path, src_path_norm, duration=1.5)
            self.s3_op_queue.put(op_del)
            self.status_message_requested.emit(f"Mount: Delete '{os.path.basename(src_path_norm)}' (moved out) from S3", 3000)
            return

        # Case 2: Moved into the watched directory (old key None, new key valid)
        if not s3_key_old and s3_key_new:
            print(f"S3SyncEH ({self.local_mount_path}): Item '{dest_path_norm}' moved into watched scope. Creating on S3: '{s3_key_new}'")
            # Treat as a creation event
            if self.mount_manager: self.mount_manager.add_ignore_path_to_specific_handler(self.local_mount_path, dest_path_norm, duration=3.0)
            self._handle_file_event_debounced(dest_path_norm, event.is_directory, "Created") # Use debounced handler
            self.status_message_requested.emit(f"Mount: Create '{os.path.basename(dest_path_norm)}' (moved in) -> S3", 3000)
            return

        # Case 3: Moved within the watched directory (both keys valid)
        if s3_key_old and s3_key_new:
            print(f"S3SyncEH ({self.local_mount_path}): Item moved within scope: '{src_path_norm}' -> '{dest_path_norm}'. S3: '{s3_key_old}' -> '{s3_key_new}'")
            self.status_message_requested.emit(f"Mount: Move '{os.path.basename(src_path_norm)}' -> '{os.path.basename(dest_path_norm)}'", 3000)

            # Add both src and dest to ignore for a short period to prevent chained events
            if self.mount_manager:
                self.mount_manager.add_ignore_path_to_specific_handler(self.local_mount_path, src_path_norm, duration=2.0)
                self.mount_manager.add_ignore_path_to_specific_handler(self.local_mount_path, dest_path_norm, duration=3.0) # Dest longer

            if event.is_directory:
                # Moving directories in S3 (preserving contents) is complex.
                # It involves listing all objects under s3_key_old, copying each to s3_key_new structure,
                # then deleting all original objects and the original folder marker.
                # This is a batch operation. S3OperationWorker doesn't handle batch directory moves itself.
                # For now, we can only signal a high-level intent or simplify.
                print(f"S3SyncEH ({self.local_mount_path}): Complex S3 folder move from '{s3_key_old}' to '{s3_key_new}'. This requires batch operations not directly handled by worker's single COPY_OBJECT for dirs.")
                self.status_message_requested.emit(f"Dir Move: '{s3_key_old}' to '{s3_key_new}' (complex S3 op, placeholder)", 5000)
                # Placeholder: create new folder marker, delete old one (DELETE_FOLDER is recursive)
                # This won't preserve contents unless OperationManager expands DELETE_FOLDER + COPY_OBJECTs
                op_create = S3Operation(S3OpType.CREATE_FOLDER, self.s3_bucket, key=s3_key_new)
                self.s3_op_queue.put(op_create)
                op_delete_old_folder = S3Operation(S3OpType.DELETE_FOLDER, self.s3_bucket, key=s3_key_old)
                self.s3_op_queue.put(op_delete_old_folder)
            else: # File move
                move_op = S3Operation(
                    S3OpType.COPY_OBJECT, self.s3_bucket,
                    key=s3_key_old,  # Source key
                    new_key=s3_key_new, # Destination key
                    is_part_of_move=True,
                    original_source_key_for_move=s3_key_old,
                    callback_data={'ui_source': 'watchdog_move_file'}
                )
                self.s3_op_queue.put(move_op)
            return

        # Case 4: Both keys None (shouldn't happen if caught by _should_ignore or not in mount path)
        # print(f"S3SyncEH ({self.local_mount_path}): Move event with no valid S3 keys for src/dest: {event.src_path} -> {event.dest_path}")
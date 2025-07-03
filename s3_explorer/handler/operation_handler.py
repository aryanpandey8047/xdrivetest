import os
import queue
import time
from datetime import datetime
import platform
import subprocess
import tempfile

from PyQt6.QtCore import QObject, pyqtSignal, QTimer, Qt
from PyQt6.QtWidgets import QProgressDialog, QMessageBox, QApplication # For processEvents

from s3ops.S3Operation import S3Operation, S3OpType
from s3ops.S3OperationWorker import S3OperationWorker
# from temp_file_handler import TempFileManager # For type hinting if needed later

class OperationManager(QObject):
    MAX_WORKER_THREADS = 4

    # Signals for external components (e.g., S3Explorer, S3TabContentWidget)
    list_op_completed = pyqtSignal(object, object, str) # S3Operation, result_dict, error_message
    download_to_temp_op_completed = pyqtSignal(object, object, str) # S3Operation, result_dict, error_message
    download_file_op_completed = pyqtSignal(object, object, str)    # S3Operation, result_dict, error_message
    upload_op_completed = pyqtSignal(object, object, str)           # S3Operation, result_dict, error_message
    delete_op_completed = pyqtSignal(object, object, str)           # S3Operation, result_dict, error_message
    create_folder_op_completed = pyqtSignal(object, object, str)    # S3Operation, result_dict, error_message
    copy_object_op_completed = pyqtSignal(object, object, str)      # S3Operation, result_dict, error_message
    
    batch_processing_update = pyqtSignal(str, int, int) # message, completed, total
    batch_processing_finished = pyqtSignal(str) # batch_id
    
    request_status_bar_message = pyqtSignal(str, int) # For worker to request status bar update

    # Signals for S3OperationWorker to update progress dialogs (internal routing)
    _request_download_progress_update = pyqtSignal(str, int, int, bool) # label, current, total, show/hide
    _request_upload_progress_update = pyqtSignal(str, int, int, bool)   # label, current, total, show/hide
    _request_batch_progress_update = pyqtSignal(str, int, int, bool)    # label, current, total, show/hide


    def __init__(self, parent_widget, temp_file_manager_ref): # parent_widget for dialogs
        super().__init__(parent_widget) 
        self.s3_client = None 
        self.temp_file_manager = temp_file_manager_ref 

        self.s3_operation_queue = queue.Queue()
        self.s3_workers = []
        self.active_batch_operations = {} 
        self.current_batch_id_for_dialog = None 
        self.completed_operation_ids = set()

        # Progress Dialogs
        self.download_progress_dialog = QProgressDialog("Downloading...", "Cancel", 0, 100, parent_widget)
        self._setup_progress_dialog(self.download_progress_dialog)
        QTimer.singleShot(4000, self.download_progress_dialog.hide)
        
        self.upload_progress_dialog = QProgressDialog("Uploading...", "Cancel", 0, 100, parent_widget)
        self._setup_progress_dialog(self.upload_progress_dialog)
        QTimer.singleShot(8000, self.upload_progress_dialog.hide)
        
        self.batch_progress_dialog = QProgressDialog("Processing batch...", "Cancel", 0, 100, parent_widget)
        self._setup_progress_dialog(self.batch_progress_dialog)
        QTimer.singleShot(12000, self.batch_progress_dialog.hide)

        # Connect internal signals for dialog updates
        self._request_download_progress_update.connect(
            lambda lbl, cur, tot, show: self._update_progress_dialog_slot(self.download_progress_dialog, lbl, cur, tot, show)
        )
        self._request_upload_progress_update.connect(
            lambda lbl, cur, tot, show: self._update_progress_dialog_slot(self.upload_progress_dialog, lbl, cur, tot, show)
        )
        self._request_batch_progress_update.connect(
            lambda lbl, cur, tot, show: self._update_progress_dialog_slot(self.batch_progress_dialog, lbl, cur, tot, show)
        )
        
        # This dictionary is passed to S3OperationWorker
        self.worker_signals_passthrough = {
            "request_status_bar_message": self.request_status_bar_message,
            "request_download_progress_dialog_update": self._request_download_progress_update,
            "request_upload_progress_dialog_update": self._request_upload_progress_update,
            "request_batch_progress_dialog_update": self._request_batch_progress_update, # For worker to update batch dialog if needed
        }


    def _setup_progress_dialog(self, dialog: QProgressDialog):
        dialog.setWindowModality(Qt.WindowModality.WindowModal) # Qt was missing here
        dialog.setAutoClose(True)
        dialog.setAutoReset(True)
        dialog.hide()
        # dialog.canceled.connect(self.handle_progress_dialog_cancel) # Optional: Implement cancellation logic

    def _update_progress_dialog_slot(self, dialog: QProgressDialog, label: str, current_value: int, total_value: int, show_dialog: bool):
        if not dialog: return

        if show_dialog:
            if not dialog.isVisible():
                print(f"OP_MGR_PROGRESS_DIALOG_SHOW: Dialog '{dialog.windowTitle()}' for Label: '{label}'")
                dialog.show()
            dialog.setLabelText(label)

            if current_value == -2 and total_value == -2: # Special case: only update label
                pass
            elif total_value <= 0: 
                dialog.setRange(0,0)
            else:
                dialog.setRange(0, total_value)
                dialog.setValue(current_value)
            QApplication.processEvents() # Keep UI responsive
        else: 
            if dialog.isVisible():
                dialog.reset() 

    def set_s3_client(self, s3_client):
        print(f"OPERATION_MANAGER: S3 client {'set' if s3_client else 'cleared'}.")
        old_s3_client = self.s3_client
        self.s3_client = s3_client
        
        if old_s3_client is not s3_client: # Only re-init workers if client actually changed or was set/cleared
            if self.s3_workers: # If workers exist from a previous client
                self.stop_all_s3_workers(join_threads=False) 
            
            if self.s3_client: # If new client is valid
                self.init_s3_workers()
            # If s3_client is None, workers remain stopped.


    def init_s3_workers(self):
        if not self.s3_client:
            print("OPERATION_MANAGER: Cannot init workers, S3 client is not set.")
            return
        if self.s3_workers: 
            print("OPERATION_MANAGER: Workers already exist, stopping them before re-init.")
            self.stop_all_s3_workers(join_threads=False) 

        self.s3_workers = []
        print(f"OPERATION_MANAGER: Initializing {self.MAX_WORKER_THREADS} S3 workers.")
        for i in range(self.MAX_WORKER_THREADS):
            worker = S3OperationWorker(self.s3_operation_queue, main_app_signals=self.worker_signals_passthrough)
            worker.setObjectName(f"S3Worker_{i}")
            worker.set_s3_client(self.s3_client) 
            worker.operation_finished.connect(self.on_worker_s3_operation_finished)
            self.s3_workers.append(worker)
            worker.start()
        print(f"OPERATION_MANAGER: S3 workers started. Count: {len(self.s3_workers)}")

    def stop_all_s3_workers(self, join_threads=True):
        print("OPERATION_MANAGER: Stopping S3 workers...")
        for worker in self.s3_workers:
            worker.stop()
        
        # Send sentinels
        for _ in range(len(self.s3_workers) + self.MAX_WORKER_THREADS): # Ample sentinels
             if self.s3_operation_queue: 
                try: self.s3_operation_queue.put_nowait(None)
                except queue.Full: break # Queue might be full if workers already exited

        if join_threads:
            for worker in self.s3_workers:
                if worker.isRunning():
                    if not worker.wait(1500): # Increased timeout slightly
                        print(f"Warning: S3 worker {worker.objectName()} did not terminate gracefully.")
        
        self.s3_workers.clear()
        print("OPERATION_MANAGER: S3 workers stopped/cleared.")

    def enqueue_s3_operation(self, operation: S3Operation):
        import inspect
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller_name = calframe[1][3] if len(calframe) > 1 else "UnknownCaller"
        print(f"OP_MGR ENQUEUE (from {caller_name}): OpID={operation.id}, OpType={operation.op_type.name}, Bucket='{operation.bucket}', Key='{operation.key}'")
        
        if not self.s3_client:
            error_msg = "S3 Client not configured. Operation cancelled."
            # Emit specific signals to allow main app to handle UI for different op types if needed
            if operation.op_type == S3OpType.LIST: self.list_op_completed.emit(operation, None, error_msg)
            elif operation.op_type == S3OpType.DOWNLOAD_TO_TEMP: self.download_to_temp_op_completed.emit(operation, None, error_msg)
            # ... Add others as appropriate
            else: self.request_status_bar_message.emit(f"Cannot enqueue {operation.op_type.name}: S3 client not ready.", 5000)
            # Show a general popup from the parent widget of the dialogs
            QMessageBox.critical(self.download_progress_dialog.parentWidget() or QApplication.activeWindow(), "S3 Client Error", error_msg)
            return
        
        if not self.s3_workers or not any(w.isRunning() for w in self.s3_workers): # Check if workers are running
            self.init_s3_workers() # Try to restart workers
            if not self.s3_workers or not any(w.isRunning() for w in self.s3_workers):
                error_msg = "S3 Workers not available. Operation cancelled."
                QMessageBox.critical(self.download_progress_dialog.parentWidget() or QApplication.activeWindow(), "S3 Worker Error", error_msg)
                # Emit error signals as above if needed
                return

        self.s3_operation_queue.put(operation)

    def on_worker_s3_operation_finished(self, operation: S3Operation, result, error_message):
        print(f"\n--- OP_MGR: WORKER_OP_FINISHED (ID: {operation.id}) ---")
        print(f"  OpType: {operation.op_type.name}, Bucket: '{operation.bucket}', Key: '{operation.key}'")
        print(f"  Result: {'Dict with keys: ' + str(list(result.keys())) if isinstance(result, dict) else result}")
        print(f"  Error: '{error_message}'")
        
        op_type = operation.op_type

        # --- Debugging block for duplicate LIST operation finishes ---
        if op_type == S3OpType.LIST:
            if operation.id in self.completed_operation_ids:
                print(f"  OP_MGR WARNING: LIST Operation {operation.id} (Key: '{operation.key}') already processed by OpManager. IGNORING DUPLICATE FINISH.")
                # It's crucial to return here to prevent any further processing of this duplicate signal.
                return 
            # If it's the first time seeing this LIST op ID, add it to the set.
            self.completed_operation_ids.add(operation.id)
            
            # Directly call the tab's handler method to update its UI
            target_tab_ref = operation.callback_data.get('tab_widget_ref')
            if target_tab_ref and hasattr(target_tab_ref, 'on_s3_list_finished_tab'):
                print(f"  OP_MGR: Calling target_tab_ref.on_s3_list_finished_tab for LIST op (ID: {operation.id})")
                try:
                    target_tab_ref.on_s3_list_finished_tab(result, error_message)
                except Exception as e_tab_handler:
                    print(f"  OP_MGR ERROR: Exception in target_tab_ref.on_s3_list_finished_tab: {e_tab_handler}")
                    # Optionally emit a critical error signal or show a message box
            else:
                print(f"  OP_MGR WARNING: LIST op (ID: {operation.id}) finished but no valid target_tab_ref or 'on_s3_list_finished_tab' method found in callback_data!")
            
            # Still emit the generic list_op_completed signal for S3Explorer or other potential listeners
            self.list_op_completed.emit(operation, result, error_message)
            
        elif op_type == S3OpType.DOWNLOAD_TO_TEMP:
            self._handle_download_to_temp_finished(operation, result, error_message)
            self.download_to_temp_op_completed.emit(operation, result, error_message)

        elif op_type == S3OpType.UPLOAD_FILE:
            self._handle_upload_finished(operation, result, error_message)
            self.upload_op_completed.emit(operation, result, error_message)

        elif op_type == S3OpType.DELETE_OBJECT or op_type == S3OpType.DELETE_FOLDER:
            # No special internal handling in OpManager needed beyond emitting the signal
            self.delete_op_completed.emit(operation, result, error_message)

        elif op_type == S3OpType.DOWNLOAD_FILE:
            # No special internal handling beyond emitting the signal
            self.download_file_op_completed.emit(operation, result, error_message)
        
        elif op_type == S3OpType.CREATE_FOLDER:
            # No special internal handling beyond emitting the signal
            self.create_folder_op_completed.emit(operation, result, error_message)
        
        elif op_type == S3OpType.COPY_OBJECT:
            # No special internal handling beyond emitting the signal
            self.copy_object_op_completed.emit(operation, result, error_message)

        # Batch progress update logic (should be after specific handlers)
        is_batch_item = "batch_id" in operation.callback_data
        batch_id = operation.callback_data.get("batch_id")
        if is_batch_item and batch_id and batch_id in self.active_batch_operations:
            self._update_batch_progress_state(operation, result, error_message) # Renamed for clarity
        
        print(f"--- END OP_MGR: WORKER_OP_FINISHED (ID: {operation.id}) ---\n")

    def _handle_download_to_temp_finished(self, operation: S3Operation, result, error_message):
        if error_message:
            # Clean up temp file if download failed but file might have been partially created
            if result and (result.get("temp_path") or result.get("local_path")) :
                path_to_clean = result.get("temp_path") or result.get("local_path")
                if path_to_clean and os.path.exists(path_to_clean):
                    try:
                        os.remove(path_to_clean)
                        print(f"OpMgr: Cleaned failed/partial temp download: {path_to_clean}")
                    except OSError as e_clean:
                        print(f"OpMgr: Error cleaning failed temp download {path_to_clean}: {e_clean}")
            # The error_message will be propagated by the download_to_temp_op_completed signal
            return

        # --- Download operation itself was reported as successful by the worker ---
        s3_key = result.get("s3_key")
        # The worker puts the download destination path in "temp_path" AND "local_path" in the result
        downloaded_local_path = result.get("temp_path") # Prefer "temp_path" if available, as per worker's result structure
        s3_bucket_of_file = result.get("s3_bucket")

        if not all([s3_key, downloaded_local_path, s3_bucket_of_file]):
            err_msg = f"OpMgr: Insufficient data in DOWNLOAD_TO_TEMP result: {result} for S3 key {s3_key}"
            print(err_msg)
            # Mutate result to pass this specific error upstream
            result["internal_error"] = err_msg 
            # No 'open_error' yet as we haven't tried to os.startfile
            return

        print(f"OpMgr: Download to temp successful for S3 key '{s3_key}'. Attempting to open local file: '{downloaded_local_path}'")

        if not os.path.exists(downloaded_local_path):
            err_msg = f"OpMgr: File '{downloaded_local_path}' reported as downloaded by worker, but NOT FOUND on disk before attempting to open."
            print(err_msg)
            result["open_error"] = err_msg # Use "open_error" key as S3Explorer expects
            return

        try:
            # Track with TempFileManager (if this is a generic temp file not for live edit)
            # OR if S3Explorer decides to track it after this based on callback_data
            # For 'live_edit_open', S3Explorer will handle tracking via TempFileManager
            # *after* this _handle_download_to_temp_finished signals completion.
            
            # This method's primary job in the refactor is just to os.startfile
            print(f"OpMgr: Executing os.startfile (or equivalent) for: '{downloaded_local_path}'")
            if platform.system() == "Windows":
                os.startfile(downloaded_local_path)
            elif platform.system() == "Darwin": # macOS
                subprocess.run(["open", downloaded_local_path], check=False)
            else: # Linux and other POSIX
                subprocess.run(["xdg-open", downloaded_local_path], check=False)
            
            print(f"OpMgr: OS open command issued for '{downloaded_local_path}'.")
            # Note: os.startfile is non-blocking. The file is now "opened" by the OS.

        except Exception as e_open_local:
            err_msg = f"OpMgr: Error opening local temp file '{downloaded_local_path}' with OS default: {e_open_local}"
            print(err_msg)
            result["open_error"] = err_msg # Add this error to the result for S3Explorer

    def _handle_upload_finished(self, operation: S3Operation, result, error_message):
        if error_message: return # Error will be propagated by upload_op_completed signal

        s3_key_uploaded = result.get("s3_key")
        bucket_uploaded_to = result.get("s3_bucket")
        local_path_that_was_uploaded = result.get("local_path")
        is_temp_update = operation.callback_data.get("is_temp_file_update", False)

        if not all([s3_key_uploaded, bucket_uploaded_to, local_path_that_was_uploaded]):
             print(f"OpMgr: Insufficient data in UPLOAD_FILE result: {result}")
             return

        if is_temp_update:
            self.temp_file_manager.handle_temp_file_upload_success(
                s3_key=s3_key_uploaded,
                s3_bucket=bucket_uploaded_to,
                uploaded_local_path=local_path_that_was_uploaded,
                s3_client_ref=self.s3_client # Pass current S3 client for mtime check
            )

    def _update_batch_progress_state(self, operation: S3Operation, result, error_message):
        batch_id = operation.callback_data.get("batch_id")
        batch_info = self.active_batch_operations.get(batch_id)
        if not batch_info: 
            print(f"OpMgr: Batch ID {batch_id} not found in active_batch_operations for op {operation.id}")
            return

        if error_message and not operation.callback_data.get("is_cleanup_delete", False): 
            batch_info['failed'] += 1
        batch_info['completed'] += 1
        
        processed_count = batch_info['completed']
        total_count = batch_info['total']
        item_name_prog = os.path.basename((operation.key or operation.new_key or "item").rstrip('/'))
        
        # Update the main batch progress dialog via its specific signal
        msg = f"{batch_info.get('op_type_display', 'Processing')}: {item_name_prog} ({processed_count}/{total_count})"
        self._request_batch_progress_update.emit(msg, processed_count, total_count, True)
        
        # Emit a more general signal for S3Explorer or other components
        self.batch_processing_update.emit(msg, processed_count, total_count)

        if processed_count >= total_count:
            self.current_batch_id_for_dialog = None # This batch no longer controls the main dialog
            self._request_batch_progress_update.emit("",0,0,False) # Hide/reset the dialog
            self.batch_processing_finished.emit(batch_id) # Signal S3Explorer to finalize

    def start_batch_operation(self, batch_id, total_items, op_type_display, operations_to_queue, extra_batch_data=None):
        if batch_id in self.active_batch_operations:
            print(f"OpMgr: Warning - Batch ID {batch_id} already active. Overwriting existing batch data.")

        self.active_batch_operations[batch_id] = {
            'total': total_items, 'completed': 0, 'failed': 0,
            'op_type_display': op_type_display, # User-friendly display name for the operation
            **(extra_batch_data or {}) # Merge any additional context
        }
        self.current_batch_id_for_dialog = batch_id # This batch now owns the main progress dialog
        
        initial_msg = f"{op_type_display} (0/{total_items})"
        self._request_batch_progress_update.emit(initial_msg, 0, total_items, True) # Show and initialize dialog

        for op_to_enqueue in operations_to_queue:
            # Ensure the operation is tagged with this batch_id for tracking
            if "batch_id" not in op_to_enqueue.callback_data: 
                op_to_enqueue.callback_data["batch_id"] = batch_id
            self.enqueue_s3_operation(op_to_enqueue)

    def get_active_batch_operation_data(self, batch_id):
        return self.active_batch_operations.get(batch_id)

    def clear_batch_operation_data(self, batch_id):
        return self.active_batch_operations.pop(batch_id, None)

    def get_queue_status(self):
        """Returns True if there are operations in the queue."""
        return not self.s3_operation_queue.empty()
    
    def get_active_batch_operations_status(self):
        """Returns True if there are any active batch operations."""
        return bool(self.active_batch_operations)

    def clear_completed_list_op_ids(self):
        """Clears the set of completed LIST operation IDs. Useful when context changes (e.g. profile switch)."""
        print("OP_MGR: Clearing completed LIST operation IDs.")
        self.completed_operation_ids.clear()
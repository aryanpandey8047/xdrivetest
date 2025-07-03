import os
import queue
from s3ops.S3Operation import S3Operation, S3OpType
from botocore.exceptions import ClientError, ReadTimeoutError, ConnectTimeoutError
from PyQt6.QtCore import QThread, pyqtSignal


# --- S3OperationWorker Thread (processes the queue) ---
class S3OperationWorker(QThread):
    operation_finished = pyqtSignal(S3Operation, object, str)
    # operation_progress = pyqtSignal(S3Operation, int, int)
    # single_item_processed_in_batch = pyqtSignal(str, str) # batch_id, message

    def __init__(self, op_queue, main_app_signals=None, parent=None): # Add main_app_signals
        super().__init__(parent)
        self.s3_client_ref = None
        self.op_queue = op_queue
        self._is_running = True
        self.main_app_signals = main_app_signals # Store reference

    def stop(self):
        self._is_running = False

    def set_s3_client(self, client):
        self.s3_client_ref = client

    def _emit_progress_via_main_app(self, operation, bytes_transferred, total_bytes, dialog_type):
        signal_key = "request_download_progress_dialog_update"
        if self.main_app_signals:
            label_prefix = ""
            item_display_name = os.path.basename((operation.key or operation.new_key or "item").rstrip('/'))
            
            if dialog_type == "download":
                label_prefix = f"Downloading {item_display_name}"
                percent = (bytes_transferred / total_bytes) * 100 if total_bytes > 0 else 0
                label_text = f"{label_prefix} ({percent:.0f}%)" if total_bytes > 0 else f"{label_prefix}..."
                self.main_app_signals[signal_key].emit(
                        label_text, bytes_transferred, total_bytes, True
                    )
            elif dialog_type == "upload":
                signal_key = "request_upload_progress_dialog_update"
                label_prefix = f"Uploading {item_display_name}"
                percent = (bytes_transferred / total_bytes) * 100 if total_bytes > 0 else 0
                label_text = f"{label_prefix} ({percent:.0f}%)" if total_bytes > 0 else f"{label_prefix}..."
                self.main_app_signals[signal_key].emit(
                        label_text, bytes_transferred, total_bytes, True
                    )

    def _emit_single_item_processed_via_main_app(self, batch_id, message): # message like "Processing item_name..."
        if self.main_app_signals:
            signal_key = "request_batch_progress_dialog_update"
            if signal_key in self.main_app_signals:
                # The worker should not know about batch_info['completed'] or batch_info['total'].
                # It only provides a label update.
                # OperationManager is responsible for the numeric progress of the batch.
                # We pass 0, 0 for current/total here, assuming the OperationManager
                # will update these correctly when it gets operation_finished for this item.
                # Or, a convention could be -1 to indicate "only update label".
                # For simplicity, let's ensure the dialog is shown with the new message.
                # The actual numeric progress is handled by OperationManager upon operation_finished.
                try:
                    self.main_app_signals[signal_key].emit(
                        message, # The new label text, e.g., "Processing: some_file.txt"
                        -2,      # Special value to indicate "keep current value" or "OpMgr will set value"
                        -2,      # Special value to indicate "keep current total" or "OpMgr will set total"
                        True     # Ensure dialog is visible
                    )
                    print(f"WORKER: Emitted batch progress label update: {message}")
                except Exception as e:
                    print(f"WORKER: Error emitting batch progress label update signal: {e}")

            else:
                print(f"WORKER: Signal key '{signal_key}' not found for batch progress label update.")

    def run(self):
        while self._is_running:
            try:
                operation: S3Operation = self.op_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if operation is None: # Sentinel
                self.op_queue.task_done()
                break

            if self.s3_client_ref is None:
                error_msg = "S3 client not available in worker."
                print(f"Worker Error: {error_msg} for operation {operation.id if operation else 'None'}")
                if operation: self.operation_finished.emit(operation, None, error_msg)
                if operation: self.op_queue.task_done()
                continue

            s3 = self.s3_client_ref
            result = None
            error_msg = ""
            
            op_type = operation.op_type # Get op_type early for dialog hiding logic
            dialog_type_for_hiding = None
            if op_type == S3OpType.DOWNLOAD_TO_TEMP or op_type == S3OpType.DOWNLOAD_FILE:
                dialog_type_for_hiding = "download"
            elif op_type == S3OpType.UPLOAD_FILE:
                dialog_type_for_hiding = "upload"

            try:
                # These are assigned within the try block if needed by specific ops
                bucket = operation.bucket
                key = operation.key
                new_key = operation.new_key
                local_path = operation.local_path

                # Update batch dialog label if this item is part of a batch
                if operation.callback_data.get("batch_id"):
                    item_name_for_batch_label = os.path.basename((key or new_key or "item").rstrip('/'))
                    self._emit_single_item_processed_via_main_app(
                        operation.callback_data.get("batch_id"),
                        f"Processing: {item_name_for_batch_label}..."
                    )

                if op_type == S3OpType.LIST:
                    paginator = s3.get_paginator('list_objects_v2')
                    folders, files = [], []
                    # For LIST, 'key' is the prefix. Ensure it's correctly formatted.
                    prefix_to_list = key if key is not None else '' # Default to empty string if key is None
                    if prefix_to_list and not prefix_to_list.endswith('/'):
                        prefix_to_list += '/'
                    
                    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_to_list, Delimiter='/'):
                        folders.extend(common_prefix.get('Prefix') for common_prefix in page.get('CommonPrefixes', []))
                        # Exclude the prefix itself if it appears as a "file" (common for folder markers)
                        files.extend(obj for obj in page.get('Contents', []) if obj.get('Key') != prefix_to_list)
                    result = {"folders": folders, "files": files, "requested_prefix": prefix_to_list}
                
                elif op_type == S3OpType.DELETE_OBJECT:
                    s3.delete_object(Bucket=bucket, Key=key)
                    result = True
                
                elif op_type == S3OpType.DELETE_FOLDER:
                    paginator = s3.get_paginator('list_objects_v2')
                    list_prefix_for_delete = key if key.endswith('/') else key + '/'
                    objects_to_delete = []
                    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix_for_delete):
                        if page.get('Contents'):
                            for obj_content in page.get('Contents'):
                                objects_to_delete.append({'Key': obj_content['Key']})
                    
                    deleted_count = 0
                    if objects_to_delete:
                        # S3 delete_objects can take up to 1000 keys at a time
                        for i in range(0, len(objects_to_delete), 1000):
                            chunk_to_delete = {'Objects': objects_to_delete[i:i+1000]}
                            delete_response = s3.delete_objects(Bucket=bucket, Delete=chunk_to_delete)
                            deleted_count += len(delete_response.get('Deleted', []))
                            if delete_response.get('Errors'):
                                # Handle partial deletion errors if necessary
                                error_msg += f" Errors during multi-delete: {delete_response.get('Errors')};"
                    
                    # Some S3 providers might still have an explicit folder object even if empty after contents are deleted.
                    # Attempt to delete the folder marker itself. This is often harmless if it doesn't exist.
                    try:
                        # Check if the folder marker object itself exists (if it wasn't part of Contents)
                        # s3.head_object(Bucket=bucket, Key=list_prefix_for_delete) # This check might be redundant
                        s3.delete_object(Bucket=bucket, Key=list_prefix_for_delete)
                        # If it was an explicit object and deleted, you could count it.
                    except ClientError as ce:
                        if ce.response['Error']['Code'] != '404' and ce.response['Error']['Code'] != 'NoSuchKey': # Ignore if not found
                            error_msg += f" Error deleting folder marker {list_prefix_for_delete}: {ce};"
                        # else: print(f"Folder marker {list_prefix_for_delete} not found or already deleted.")
                    
                    result = {"deleted_count": deleted_count, "key": list_prefix_for_delete}


                elif op_type == S3OpType.DOWNLOAD_TO_TEMP or op_type == S3OpType.DOWNLOAD_FILE:
                    total_size = 0 # Default to 0 if head_object fails
                    try:
                        head = s3.head_object(Bucket=bucket, Key=key)
                        total_size = int(head.get('ContentLength', 0))
                    except Exception as e_head:
                        print(f"Worker: Could not get ContentLength for {key}: {e_head}. Progress may be indeterminate.")
                    
                    bytes_done = 0
                    # Initial progress emit (even if indeterminate)
                    self._emit_progress_via_main_app(operation, bytes_done, total_size, "download")

                    def progress_cb(chunk_size):
                        nonlocal bytes_done
                        bytes_done += chunk_size
                        self._emit_progress_via_main_app(operation, bytes_done, total_size, "download")
                    
                    target_path = local_path 
                    if op_type == S3OpType.DOWNLOAD_FILE: 
                        # Ensure destination directory exists for explicit downloads
                        dest_dir = os.path.dirname(target_path)
                        if dest_dir: # Only create if dirname is not empty (i.e., not root)
                            os.makedirs(dest_dir, exist_ok=True)

                    s3.download_file(bucket, key, target_path, Callback=progress_cb)
                    if op_type == S3OpType.DOWNLOAD_TO_TEMP:
                        result = {"s3_key": key, "temp_path": target_path, "s3_bucket": bucket}
                    else: # DOWNLOAD_FILE
                        result = {"s3_key": key, "local_path": target_path, "s3_bucket": bucket}


                elif op_type == S3OpType.UPLOAD_FILE:
                    if not os.path.exists(local_path):
                        # This error will be caught by the FileNotFoundError handler below
                        raise FileNotFoundError(f"Local file for upload does not exist: {local_path}")
                    
                    total_size = os.path.getsize(local_path) 
                    bytes_done = 0
                    # Initial progress emit
                    self._emit_progress_via_main_app(operation, bytes_done, total_size, "upload")

                    def progress_cb(chunk_size):
                        nonlocal bytes_done
                        bytes_done += chunk_size
                        self._emit_progress_via_main_app(operation, bytes_done, total_size, "upload")
                    
                    s3.upload_file(local_path, bucket, key, Callback=progress_cb)
                    result = {"s3_key": key, "local_path": local_path, "s3_bucket": bucket}
                    # Specific network/client errors are caught in the outer try-except

                elif op_type == S3OpType.CREATE_FOLDER:
                    folder_key_to_create = key if key.endswith('/') else key + '/'
                    s3.put_object(Bucket=bucket, Key=folder_key_to_create, Body='') # Explicit empty body for folder
                    result = {"s3_key": folder_key_to_create, "s3_bucket": bucket}

                elif op_type == S3OpType.COPY_OBJECT:
                    # For COPY_OBJECT:
                    # operation.key is the SOURCE key
                    # operation.bucket is the DESTINATION bucket
                    # operation.new_key is the DESTINATION key
                    source_key_for_copy = operation.key
                    dest_bucket_for_copy = operation.bucket
                    dest_key_for_copy = operation.new_key

                    # Determine the source bucket
                    if "source_bucket_override" in operation.callback_data:
                        source_bucket_for_copy = operation.callback_data["source_bucket_override"]
                    else:
                        # If no override, assume source bucket is same as destination
                        # This depends on how S3Operation was constructed by the caller.
                        source_bucket_for_copy = dest_bucket_for_copy
                    
                    copy_source_dict = {'Bucket': source_bucket_for_copy, 'Key': source_key_for_copy}
                    s3.copy_object(CopySource=copy_source_dict, Bucket=dest_bucket_for_copy, Key=dest_key_for_copy)
                    
                    result_data = {
                        "source_key": source_key_for_copy, "dest_key": dest_key_for_copy,
                        "source_bucket": source_bucket_for_copy, "dest_bucket": dest_bucket_for_copy,
                        "original_deleted": False # Default
                    }

                    if operation.is_part_of_move:
                        key_to_delete_after_move = operation.original_source_key_for_move or source_key_for_copy
                        if key_to_delete_after_move: # Ensure there's something to delete
                            try:
                                s3.delete_object(Bucket=source_bucket_for_copy, Key=key_to_delete_after_move)
                                result_data["original_deleted"] = True
                            except Exception as del_e:
                                print(f"S3OpWorker: Failed to delete original '{key_to_delete_after_move}' from '{source_bucket_for_copy}' after move: {del_e}")
                                result_data["original_deleted"] = False # Explicitly set even if default
                                result_data["original_delete_error"] = str(del_e)
                        else:
                             print(f"S3OpWorker: Warning - part of move but no original_source_key_for_move and source key was None for deletion.")
                    result = result_data
                else:
                    error_msg = f"Unknown S3 operation type: {op_type}"

            except ClientError as e: # Catch specific boto3 client errors
                # Attempt to get a more user-friendly message from the error response
                s3_error_code = e.response.get('Error', {}).get('Code', 'UnknownS3Error')
                s3_error_message = e.response.get('Error', {}).get('Message', str(e))
                error_msg = f"S3 Error ({s3_error_code}) for {operation.op_type.name} on '{key or new_key}': {s3_error_message}"
                print(f"Worker ClientError: {error_msg} | Full error: {e}") # Log full error for debugging
            except FileNotFoundError as e_fnf:
                 error_msg = f"File not found for {operation.op_type.name} on '{local_path or key}': {e_fnf}"
            except (ReadTimeoutError, ConnectTimeoutError) as net_err: # Catch specific network errors
                error_msg = f"Network timeout during {operation.op_type.name} of '{key or local_path}': {net_err}"
            except Exception as e_general: # Catch any other unexpected errors
                error_msg = f"Unexpected error during {operation.op_type.name} on '{key or new_key or local_path}': {str(e_general)}"
                # For critical unexpected errors, you might want to log the full traceback
                import traceback
                print(f"Worker General Exception Traceback for op {operation.id}:\n{traceback.format_exc()}")
            
            finally: # Ensure dialog is hidden if it was shown for this operation
                if dialog_type_for_hiding and self.main_app_signals:
                    signal_key_to_hide = None
                    if dialog_type_for_hiding == "download":
                        signal_key_to_hide = "request_download_progress_dialog_update"
                    elif dialog_type_for_hiding == "upload":
                        signal_key_to_hide = "request_upload_progress_dialog_update"
                    
                    if signal_key_to_hide and signal_key_to_hide in self.main_app_signals:
                        try:
                            self.main_app_signals[signal_key_to_hide].emit("", 0, 0, False) # False to hide
                        except Exception as e_hide:
                             print(f"WORKER: Error emitting hide signal for {signal_key_to_hide}: {e_hide}")
            
            self.operation_finished.emit(operation, result, error_msg)
            self.op_queue.task_done()

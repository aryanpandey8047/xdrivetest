import os
import json
from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtWidgets import QMessageBox # For mount errors if needed
from watchdog.observers import Observer
from s3ops.S3SyncEventHandler import S3SyncEventHandler # Assuming S3SyncEventHandler is in s3ops

class MountManager(QObject):
    mount_status_message = pyqtSignal(str, int) # message, timeout

    def __init__(self, app_data_dir, parent=None):
        super().__init__(parent)
        self.app_data_dir = app_data_dir
        self.mounts_file = os.path.join(self.app_data_dir, "mounts.json")
        self.mounted_paths_config = [] # Runtime config: [{'local_path', 's3_bucket', 's3_prefix', 'observer', 'handler'}, ...]
        
        self.s3_client = None # To be set
        self.s3_op_queue_ref = None # To be set (from OperationManager or S3Explorer)
        self.main_window_ref = None # For S3SyncEventHandler if it needs the main window

    def _ensure_app_data_dir_exists(self):
        if not os.path.exists(self.app_data_dir):
            try:
                os.makedirs(self.app_data_dir, exist_ok=True)
            except OSError as e:
                print(f"Error creating application data directory {self.app_data_dir}: {e}")
                return False
        return True

    def set_dependencies(self, s3_client, s3_op_queue_ref, main_window_ref):
        self.s3_client = s3_client
        self.s3_op_queue_ref = s3_op_queue_ref
        self.main_window_ref = main_window_ref # S3SyncEventHandler might need this for callbacks/signals
        
        # If config already loaded and client is now valid, try starting observers
        if self.mounted_paths_config and self.s3_client and self.s3_op_queue_ref:
            self.start_watchdog_observers()


    def load_mounts_config(self):
        self.mounted_paths_config = [] # Clear runtime parts
        loaded_persistent_configs = []
        try:
            if os.path.exists(self.mounts_file):
                with open(self.mounts_file, 'r') as f:
                    raw_mounts = json.load(f)
                    if isinstance(raw_mounts, list):
                        for mount_data in raw_mounts:
                            if isinstance(mount_data, dict) and 'local_path' in mount_data and 's3_bucket' in mount_data:
                                loaded_persistent_configs.append({
                                    'local_path': mount_data['local_path'],
                                    's3_bucket': mount_data['s3_bucket'],
                                    's3_prefix': mount_data.get('s3_prefix', "").strip('/'),
                                })
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading mounts config from {self.mounts_file}: {e}. Initializing empty mounts config.")
        
        # Initialize runtime config from loaded persistent configs
        for conf in loaded_persistent_configs:
            self.mounted_paths_config.append({**conf, 'observer': None, 'handler': None})

        print(f"MOUNT_MANAGER: Loaded {len(self.mounted_paths_config)} mounts.")
        # If dependencies are already set, try starting observers
        if self.s3_client and self.s3_op_queue_ref:
            self.start_watchdog_observers()
        return self.get_persistent_mount_configs() # Return configs for dialog


    def get_persistent_mount_configs(self):
        # Returns a list of dicts suitable for saving or for the MountConfigDialog
        return [{'local_path':m['local_path'],'s3_bucket':m['s3_bucket'],'s3_prefix':m['s3_prefix']} 
                for m in self.mounted_paths_config if 'local_path' in m]


    def save_mounts_config(self):
        if not self._ensure_app_data_dir_exists(): return False
        config_to_save = self.get_persistent_mount_configs()
        try:
            with open(self.mounts_file, 'w') as f:
                json.dump(config_to_save, f, indent=4)
            print(f"MOUNT_MANAGER: Saved {len(config_to_save)} mounts to {self.mounts_file}.")
            return True
        except IOError as e:
            print(f"MOUNT_MANAGER: Error saving mounts config to {self.mounts_file}: {e}")
            return False

    def update_mounted_paths(self, new_persistent_configs):
        self.stop_watchdog_observers() # Stop existing observers before reconfiguring
        self.mounted_paths_config=[] # Clear old runtime config
        for conf in new_persistent_configs: # conf is a dict from MountConfigDialog
            self.mounted_paths_config.append({
                'local_path':conf['local_path'],
                's3_bucket':conf['s3_bucket'],
                's3_prefix':conf.get('s3_prefix',"").strip('/'),
                'observer':None, 
                'handler':None
            })
        self.save_mounts_config()
        if self.s3_client and self.s3_op_queue_ref: # Check if client is ready
             self.start_watchdog_observers() 
        self.mount_status_message.emit("Mounted paths configuration updated.",3000)


    def add_ignore_path_to_specific_handler(self, local_mount_path_key: str, path_to_ignore: str, duration: float = 2.0):
        """Tells a specific handler (identified by its watched local_mount_path) to ignore a path."""
        for mount_conf in self.mounted_paths_config:
            if mount_conf['local_path'] == local_mount_path_key and mount_conf.get('handler'):
                mount_conf['handler'].add_ignore_path(path_to_ignore, duration)
                return
        print(f"MOUNT_MANAGER: No active handler found for local_mount_path '{local_mount_path_key}' to ignore path.")


    def start_watchdog_observers(self):
        if not self.s3_client:
            print("MOUNT_MANAGER: Cannot start watchdog: S3 client not available.")
            return
        if not self.s3_op_queue_ref:
            print("MOUNT_MANAGER: Cannot start watchdog: S3 operation queue not available.")
            return
        if not self.main_window_ref: # S3Explorer instance needed for connecting the new signal
            print("MOUNT_MANAGER: Cannot start watchdog: Main window reference not available for signal connection.")
            return

        for i in range(len(self.mounted_paths_config)): # Iterate to modify in place
            mount_config = self.mounted_paths_config[i]
            if mount_config.get('observer') and mount_config['observer'].is_alive():
                continue # Already running for this config

            local_path_to_watch = mount_config['local_path']
            if not os.path.exists(local_path_to_watch) or not os.path.isdir(local_path_to_watch):
                # Use QMessageBox via main_window_ref or signal if critical error
                msg = f"Local path for S3 mount does not exist or is not a directory: {local_path_to_watch}. Mount disabled."
                print(f"MOUNT_MANAGER: {msg}")
                self.mount_status_message.emit(msg, 5000)
                continue
            
            event_handler = S3SyncEventHandler(
                local_mount_path=local_path_to_watch, 
                s3_bucket=mount_config['s3_bucket'],
                s3_prefix=mount_config.get('s3_prefix', ""), 
                s3_op_queue_ref=self.s3_op_queue_ref,
                main_window_ref=self.main_window_ref, # Pass main window if S3SyncEventHandler needs it
                mount_manager_ref=self
            )
            # Connect the handler's signal for status messages to MountManager's signal
            # This assumes S3SyncEventHandler has a 'status_message_requested' signal similar to MountManager's
            if hasattr(event_handler, 'status_message_requested') and isinstance(event_handler.status_message_requested, pyqtSignal):
                event_handler.status_message_requested.connect(self.mount_status_message) # Forward signal
            
            if hasattr(self.main_window_ref, 'handle_mount_deletion_confirmation'):
                event_handler.deletion_confirmation_requested.connect(self.main_window_ref.handle_mount_deletion_confirmation)
            else:
                print(f"MountManager: WARNING - S3Explorer instance does not have 'handle_mount_deletion_confirmation' slot.")

            observer = Observer()
            observer.schedule(event_handler, local_path_to_watch, recursive=True)
            try:
                observer.start()
                self.mounted_paths_config[i]['observer'] = observer
                self.mounted_paths_config[i]['handler'] = event_handler 
                msg = f"Watching mounted path: {local_path_to_watch} -> s3://{mount_config['s3_bucket']}/{mount_config.get('s3_prefix', '')}"
                print(f"MOUNT_MANAGER: {msg}")
                self.mount_status_message.emit(msg, 3000)
            except Exception as e:
                msg = f"Could not start watchdog for {local_path_to_watch}: {e}"
                print(f"MOUNT_MANAGER: {msg}")
                if self.main_window_ref: # If main window ref is available for critical popups
                    QMessageBox.critical(self.main_window_ref, "Watchdog Error", msg)
                else:
                    self.mount_status_message.emit(msg, 10000) # Longer display if no popup


    def stop_watchdog_observers(self, clear_runtime_objects=True):
        print("MOUNT_MANAGER: Stopping watchdog observers...")
        for i in range(len(self.mounted_paths_config)):
            mount_config = self.mounted_paths_config[i]
            observer = mount_config.get('observer')
            if observer and observer.is_alive():
                try:
                    handler = mount_config.get('handler')
                    if handler and hasattr(handler, 'status_message_requested') and isinstance(handler.status_message_requested, pyqtSignal):
                        try: handler.status_message_requested.disconnect(self.mount_status_message)
                        except TypeError: pass # Already disconnected or never connected
                    observer.stop()
                    observer.join(timeout=1)
                    if observer.is_alive(): print(f"MOUNT_MANAGER: Watchdog for {mount_config['local_path']} did not stop gracefully.")
                    else: print(f"MOUNT_MANAGER: Stopped watchdog for: {mount_config['local_path']}")
                except Exception as e:
                    print(f"MOUNT_MANAGER: Error stopping watchdog for {mount_config['local_path']}: {e}")
            
            if clear_runtime_objects:
                self.mounted_paths_config[i]['observer'] = None
                self.mounted_paths_config[i]['handler'] = None
        
        if clear_runtime_objects and not self.mounted_paths_config: # e.g. if config list itself is cleared
            pass
        print("MOUNT_MANAGER: All watchdog observers signalled to stop.")
        
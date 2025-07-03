from datetime import datetime
import sys
import os
import platform
# import subprocess # Now in OperationManager/TempFileManager
import tempfile # Now in OperationManager/TempFileManager
# from datetime import datetime # Now in specific handlers
# import queue # Now in OperationManager
import threading
import time # Still used for batch_id
import json

# Third-party libraries
# import boto3 # Now in ProfileManager
from dotenv import load_dotenv, find_dotenv # Only for initial .env migration (ProfileManager handles it)
# from watchdog.observers import Observer # Now in MountManager

import zipfile
import shutil
import atexit
from watchdog.observers import Observer as WatchdogObserver

from server import start_webdav, stop_webdav
from pyupdater.client import Client
from client_config import ClientConfig


from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QLineEdit,
    QHBoxLayout,QWidget,
    QToolBar, QStatusBar, QMessageBox, QDialog,
    QLabel, QFileDialog, QTabWidget,
    QProgressDialog, QInputDialog, QComboBox, QStyle, QSizePolicy, QTreeView, QVBoxLayout, QTextEdit, QPushButton, QSplashScreen
)
from PyQt6.QtGui import QIcon, QAction, QKeySequence, QPixmap
from PyQt6.QtCore import Qt, QSettings, QSize, QTimer, QByteArray, pyqtSlot, pyqtSignal

# Local imports
from credentials_dialog import CredentialsDialog 
from profile_manager_dialog import ProfileManagerDialog
from mount_config_dialog import MountConfigDialog
from properties_dialog import PropertiesDialog 
from help_menu.help_dialogs import show_keyboard_shortcuts, show_about_dialog

from s3ops.S3Operation import S3Operation, S3OpType
# S3OperationWorker is used by OperationManager
from s3ops.S3TabContentWidget import S3TabContentWidget, COL_NAME, COL_TYPE, COL_SIZE, COL_MODIFIED, COL_S3_KEY, COL_IS_FOLDER
from zip_worker import ZipFolderWorker
from download_worker import DownloadFolderWorker

# New Handler/Manager imports
from handler.profile_handler import ProfileManager
from handler.operation_handler import OperationManager
from handler.favorites_handler import FavoritesManager
from handler.temp_file_handler import TempFileManager
from handler.mount_handler import MountManager
from handler.live_edit_handler import LiveEditFileChangeHandler
from handler.sharable_link import generate_shareable_s3_link

from PyQt6.QtGui import QClipboard

__version__ = "0.7"

# --- Application Data Paths ---
# --- Application Data Paths ---
def get_application_base_path():
    """ Get the base path for the application, accounting for PyInstaller. """
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        # Running in a PyInstaller bundle (one-file or one-folder)
        # For one-file, _MEIPASS is the temp dir. For one-folder, it's the app dir.
        # We want the directory of the executable itself for persistent data.
        return os.path.dirname(sys.executable)
    else:
        # Running as a normal script
        return os.path.dirname(os.path.abspath(__file__))

APP_BASE_DIR = get_application_base_path()
APP_DATA_DIR = os.path.join(APP_BASE_DIR, ".s3explorer_data")

# File paths are now managed by their respective handlers, but APP_DATA_DIR is good to have
# PROFILES_FILE = os.path.join(APP_DATA_DIR, "profiles.json")
# FAVORITES_FILE = os.path.join(APP_DATA_DIR, "favorites.json")
# MOUNTS_FILE = os.path.join(APP_DATA_DIR, "mounts.json")

S3_LIVE_EDIT_TEMP_DIR = None
S3_TRASH_PREFIX = "Trash/"

def _ensure_app_data_dir_exists(): # Global utility
    if not os.path.exists(APP_DATA_DIR):
        try:
            os.makedirs(APP_DATA_DIR, exist_ok=True)
        except OSError as e:
            print(f"Error creating application data directory {APP_DATA_DIR}: {e}")
            QMessageBox.critical(None, "Fatal Error", f"Could not create application data directory:\n{APP_DATA_DIR}\n{e}")
            sys.exit(1) # Critical error

def get_s3_live_edit_temp_dir():
    global S3_LIVE_EDIT_TEMP_DIR
    if S3_LIVE_EDIT_TEMP_DIR and os.path.exists(S3_LIVE_EDIT_TEMP_DIR):
        return S3_LIVE_EDIT_TEMP_DIR
    
    # Clean up if path was lost but we want a fresh one per session
    if S3_LIVE_EDIT_TEMP_DIR and not os.path.exists(S3_LIVE_EDIT_TEMP_DIR):
        print(f"Warning: Previous live edit temp dir {S3_LIVE_EDIT_TEMP_DIR} not found.")

    S3_LIVE_EDIT_TEMP_DIR = tempfile.mkdtemp(prefix="s3exp_live_edit_")
    print(f"S3Explorer: Created live edit temp directory: {S3_LIVE_EDIT_TEMP_DIR}")
    return S3_LIVE_EDIT_TEMP_DIR

def cleanup_s3_live_edit_temp_dir():
    global S3_LIVE_EDIT_TEMP_DIR
    if S3_LIVE_EDIT_TEMP_DIR and os.path.exists(S3_LIVE_EDIT_TEMP_DIR):
        try:
            print(f"S3Explorer: Cleaning up live edit temp directory: {S3_LIVE_EDIT_TEMP_DIR}")
            shutil.rmtree(S3_LIVE_EDIT_TEMP_DIR)
            S3_LIVE_EDIT_TEMP_DIR = None
        except Exception as e:
            print(f"S3Explorer: Error cleaning live edit temp dir: {e}")

# --- Main Application Window ---
class S3Explorer(QMainWindow):
    # Signals for thread-safe GUI updates primarily from OperationManager
    # request_status_bar_message = pyqtSignal(str, int) # OpManager will have this
    
    # Note: Progress dialog update signals are now internal to OperationManager,
    # which it provides to its workers. S3Explorer doesn't need to emit them directly.

    def __init__(self):
        super().__init__()
        print(f"S3EXPLORER __INIT__: Start (after super)")
        _ensure_app_data_dir_exists()
        cleanup_s3_live_edit_temp_dir() # Clean up from previous session if any
        self.live_edit_temp_dir = get_s3_live_edit_temp_dir() # Initialize for this session
        self.S3_TRASH_PREFIX = S3_TRASH_PREFIX

        self.settings = QSettings("MyCompany", "S3ExplorerApp_Tabbed_v3_1")
        
        # Initialize Managers/Handlers
        # Order matters for dependencies
        self.profile_manager = ProfileManager(APP_DATA_DIR, parent=self)
        self.temp_file_manager = TempFileManager(parent=self)
        self.operation_manager = OperationManager(parent_widget=self, temp_file_manager_ref=self.temp_file_manager)
        self.favorites_manager = FavoritesManager(APP_DATA_DIR, parent=self)
        self.mount_manager = MountManager(APP_DATA_DIR, parent=self)

        self.s3_clipboard = None # {'type', 'source_bucket', 'keys', 'is_folder'}
        self.tab_widget = None # UI element, initialized in init_ui
        self.add_fav_action_fixed = None # For fixed menu item

        self.modified_check_timer = QTimer(self)
        self.modified_check_timer.timeout.connect(self.check_modified_temp_files)
        # Start timer only if s3_client is valid, or after successful client init

        self._opening_s3_file_key = None # Tracks S3 key of file currently being opened
        self._opening_s3_file_lock = threading.Lock() # To make checking/setting _opening_s3_file_key atomic

        # Connect signals from managers to S3Explorer slots
        self.profile_manager.s3_client_initialized.connect(self.on_s3_client_initialized)
        self.profile_manager.s3_client_init_failed.connect(self.on_s3_client_init_failed)
        self.profile_manager.active_profile_switched.connect(self.on_active_profile_switched)
        self.profile_manager.profiles_loaded.connect(self.on_profiles_loaded_from_manager)

        # Connect profile changes to rebuild_favorites_menu
        self.profile_manager.s3_client_initialized.connect(self.rebuild_favorites_menu)
        self.profile_manager.active_profile_switched.connect(self.rebuild_favorites_menu)
        self.profile_manager.s3_client_init_failed.connect(self.rebuild_favorites_menu)

        self.operation_manager.request_status_bar_message.connect(self.update_status_bar_message_slot)
        # Connect to specific operation completion signals from OperationManager
        self.operation_manager.download_to_temp_op_completed.connect(self.on_op_mgr_download_to_temp_finished)
        self.operation_manager.upload_op_completed.connect(self.on_op_mgr_upload_finished)
        self.operation_manager.delete_op_completed.connect(self.on_op_mgr_delete_finished)
        self.operation_manager.download_file_op_completed.connect(self.on_op_mgr_download_file_finished)
        self.operation_manager.create_folder_op_completed.connect(self.on_op_mgr_create_folder_finished)
        self.operation_manager.copy_object_op_completed.connect(self.on_op_mgr_copy_object_finished)
        self.operation_manager.list_op_completed.connect(self.on_op_mgr_list_op_completed) # For any global actions after list

        self.operation_manager.batch_processing_finished.connect(self.on_batch_operation_complete_from_op_mgr)
        # self.operation_manager.batch_processing_update # If S3Explorer needs to react to individual batch item progress

        self.favorites_manager.favorites_updated.connect(self.rebuild_favorites_menu)
        self.temp_file_manager.temp_file_modified_status_changed.connect(self.on_temp_file_status_changed_update_save_action)
        self.mount_manager.mount_status_message.connect(self.update_status_bar_message_slot)

        # Watchdog for live edit temp files
        self.live_edit_file_watcher = None
        self.live_edit_file_handler = None
        self._live_edit_debounce_timers = {} # path: threading.Timer for debouncing uploads

        self.init_ui() # Creates self.tab_widget and other UI
        self.load_settings()     # Window geometry from QSettings
        
        self.check_for_updates_on_startup()

        self.update_check_timer = QTimer(self)
        self.update_check_timer.timeout.connect(self.check_for_updates_in_background)
        self.update_check_timer.start(15 * 60 * 1000)  # Every 15 mins

        
        # Load initial data using managers
        self.profile_manager.load_aws_profiles() # Triggers on_profiles_loaded_from_manager
        self.favorites_manager.load_favorites()  # Triggers rebuild_favorites_menu via signal
        self.mount_manager.load_mounts_config()  # Loads config, starts observers if client ready

        # Initial S3 client setup attempt (ProfileManager handles this)
        # on_profiles_loaded_from_manager will trigger attempt_initial_s3_connection
        # self.attempt_initial_s3_connection() # Call this after profiles are loaded.

        
        # Initial UI state updates (many depend on s3_client state)
        self.update_navigation_buttons_state()
        self.update_save_action_state()
        self.update_edit_actions_state()

        if self.live_edit_temp_dir:
            self.start_live_edit_file_watcher()

        atexit.register(cleanup_s3_live_edit_temp_dir) # Ensure cleanup on exit
        atexit.register(self.stop_live_edit_file_watcher) # Stop watcher on exit
        
        print(f"S3EXPLORER __INIT__: End")

    def attempt_initial_s3_connection(self):
        print("S3EXPLORER: Attempting initial S3 connection via ProfileManager.")
        if not self.profile_manager.attempt_s3_client_initialization(): # Uses current active profile
            # s3_client_init_failed signal would have been emitted by ProfileManager
            # on_s3_client_init_failed handles UI prompts
            # self.prompt_for_initial_profile_setup() # on_s3_client_init_failed might do this
            pass # Handled by signal slots

    @pyqtSlot(dict, str)
    def on_profiles_loaded_from_manager(self, profiles, active_profile_name):
        print(f"S3EXPLORER: Profiles loaded by manager. Active: '{active_profile_name}'. Triggering initial connection attempt.")
        # Now that profiles are loaded, try to connect.
        # ProfileManager already has active_profile_name set from load.
        self.attempt_initial_s3_connection()
        if hasattr(self, 'profile_combo'): # UI might not be fully ready if this is too early
            self.update_profile_combo_display()


    @pyqtSlot(str, int)
    def update_status_bar_message_slot(self, message, timeout):
        if hasattr(self, 'status_bar'):
            self.status_bar.showMessage(message, timeout)

    @pyqtSlot(object, str) # s3_client, profile_name
    def on_s3_client_initialized(self, s3_client_instance, profile_name):
        print(f"S3EXPLORER: S3 client initialized for profile '{profile_name}'.")
        self.setWindowTitle(f"S3 Explorer - Profile: {profile_name}")
        self.update_status_bar_message_slot(f"Connected with profile: {profile_name}", 3000)

        # Update managers that depend on s3_client
        self.operation_manager.set_s3_client(s3_client_instance)
        self.mount_manager.set_dependencies(s3_client_instance, 
                                            self.operation_manager.s3_operation_queue, # Pass queue ref
                                            self) # Pass main window ref

        # Clear old tabs and add a new one for the new profile context
        self._clear_all_tabs()
        active_profile_data = self.profile_manager.get_profile_data(profile_name)
        default_bucket = active_profile_data.get("default_s3_bucket", "") if active_profile_data else ""
        self.add_new_s3_tab(bucket_to_open=default_bucket, path_to_open="")

        if not self.modified_check_timer.isActive():
            self.modified_check_timer.start(30000) # Check every 30s for modified temp files

        self.update_profile_combo_display()
        self.update_tab_widget_placeholder() # Ensure placeholder is removed
        self.update_navigation_buttons_state()

    @pyqtSlot(str, str) # profile_name, error_message
    def on_s3_client_init_failed(self, profile_name, error_message):
        print(f"S3EXPLORER: S3 client initialization FAILED for profile '{profile_name}'. Error: {error_message}")
        self.setWindowTitle(f"S3 Explorer - Connection Error ({profile_name})")
        self.update_status_bar_message_slot(f"Connection failed for {profile_name}: {error_message}", 7000)
        
        QMessageBox.warning(self, "S3 Connection Error", f"Error connecting with profile '{profile_name}':\n{error_message}")

        self.operation_manager.set_s3_client(None) # Clear client in OpManager
        self.mount_manager.set_dependencies(None, self.operation_manager.s3_operation_queue, self) # Clear client in MountManager

        self._clear_all_tabs()
        self.update_tab_widget_placeholder() # Show placeholder
        self.update_profile_combo_display()
        self.update_navigation_buttons_state(enable_all=False)

        # If this was the initial load and it failed, prompt for setup
        # Check if this is the "first run" scenario (e.g. no valid client yet after app start)
        if not self.profile_manager.get_s3_client(): # Check if any client is active overall
            # Only prompt if manager dialog is not already open from a previous attempt
            # This logic might need refinement to avoid multiple prompts
            is_profile_dialog_open = any(isinstance(w, ProfileManagerDialog) for w in QApplication.topLevelWidgets())
            if not is_profile_dialog_open:
                 self.prompt_for_initial_profile_setup()


    @pyqtSlot(str, object) # new_active_profile_name, new_s3_client
    def on_active_profile_switched(self, new_active_profile_name, new_s3_client):
        # This signal is emitted by ProfileManager when init_s3_client_with_config successfully changes the active profile
        # The on_s3_client_initialized slot already handles most of this logic.
        # This slot is more for reacting to the *fact* of a switch if different actions are needed
        # than just a re-initialization of the same profile.
        print(f"S3EXPLORER: Active profile switched to '{new_active_profile_name}'.")
        self.profile_manager.save_aws_profiles() # Save the new active profile choice
        
        # `on_s3_client_initialized` (if `new_s3_client` is not None) or
        # `on_s3_client_init_failed` (if `new_s3_client` is None) should have handled UI updates.
        # Here, we mainly ensure the profile combo and title are correct.
        self.update_profile_combo_display() # Ensure combo reflects the new active profile
        if new_s3_client:
             self.setWindowTitle(f"S3 Explorer - Profile: {new_active_profile_name}")
             self.operation_manager.clear_completed_list_op_ids() # Clear for new profile context
        else:
             self.setWindowTitle(f"S3 Explorer - Connection Error ({new_active_profile_name})")
        
        # Refresh might be needed if tabs were not cleared by on_s3_client_initialized
        # However, on_s3_client_initialized should clear tabs and add a new one.

    def _clear_all_tabs(self):
        if not self.tab_widget: return
        while self.tab_widget.count() > 0:
            widget_to_close = self.tab_widget.widget(0)
            self.tab_widget.removeTab(0)
            if widget_to_close: widget_to_close.deleteLater()

    def prompt_for_initial_profile_setup(self):
        # Check if a ProfileManagerDialog is already open to avoid stacking them
        for widget in QApplication.topLevelWidgets():
            if isinstance(widget, ProfileManagerDialog) and widget.isVisible():
                print("ProfileManagerDialog is already open. Skipping new prompt.")
                return

        QMessageBox.information(self, "AWS Profile Setup Required",
                                "No valid AWS profile is configured, or the active profile is incomplete. "
                                "Please configure at least one profile to connect to S3.")
        self.show_profile_manager_dialog(is_initial_setup=True) # is_initial_setup might influence dialog behavior
        # After dialog, s3_client state will determine if we need another critical message.
        if not self.profile_manager.get_s3_client():
             QMessageBox.critical(self, "Setup Incomplete", "S3 client could not be initialized. Please configure a valid AWS profile via Settings.")


    def init_ui(self):
        self.setWindowTitle("S3 Explorer")
        self.setGeometry(100, 100, 1000, 700) 

        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_s3_tab)
        self.tab_widget.currentChanged.connect(self.on_tab_changed)
        self.setCentralWidget(self.tab_widget)

        menubar = self.menuBar()
        
        file_menu = menubar.addMenu("&File")
        new_tab_action = QAction(QIcon.fromTheme("tab-new", self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogNewFolder)), "New &Tab", self)
        new_tab_action.setShortcut(QKeySequence.StandardKey.AddTab)
        new_tab_action.triggered.connect(lambda: self.add_new_s3_tab())
        file_menu.addAction(new_tab_action)

        self.save_active_file_action = QAction(QIcon.fromTheme("document-save", self.style().standardIcon(QStyle.StandardPixmap.SP_DialogSaveButton)), "&Save Active File", self)
        self.save_active_file_action.setShortcut(QKeySequence.StandardKey.Save)
        self.save_active_file_action.triggered.connect(self.handle_save_active_file)
        self.save_active_file_action.setEnabled(False)
        file_menu.addAction(self.save_active_file_action)
        
        save_as_s3_action = QAction("Save Local File As S3...", self)
        save_as_s3_action.triggered.connect(self.handle_save_local_as_s3_action)
        file_menu.addAction(save_as_s3_action)
        file_menu.addSeparator()
        exit_action = QAction("E&xit", self)
        exit_action.setShortcut(QKeySequence.StandardKey.Quit)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        edit_menu = menubar.addMenu("&Edit")
        self.copy_action = QAction(QIcon.fromTheme("edit-copy", self.style().standardIcon(QStyle.StandardPixmap.SP_ToolBarHorizontalExtensionButton)), "&Copy S3 Path", self); self.copy_action.setShortcut(QKeySequence.StandardKey.Copy); self.copy_action.triggered.connect(self.handle_copy_s3_items); edit_menu.addAction(self.copy_action)
        self.cut_action = QAction(QIcon.fromTheme("edit-cut", self.style().standardIcon(QStyle.StandardPixmap.SP_ToolBarVerticalExtensionButton)), "Cu&t S3 Path (Move)", self); self.cut_action.setShortcut(QKeySequence.StandardKey.Cut); self.cut_action.triggered.connect(self.handle_cut_s3_items); edit_menu.addAction(self.cut_action)
        self.paste_action = QAction(QIcon.fromTheme("edit-paste", self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)), "&Paste to S3", self); self.paste_action.setShortcut(QKeySequence.StandardKey.Paste); self.paste_action.triggered.connect(self.handle_paste_s3_items); edit_menu.addAction(self.paste_action)
        self.update_edit_actions_state() 

        settings_menu = menubar.addMenu("&Settings")
        open_trash_action = QAction(QIcon.fromTheme("user-trash"), "Open S3 Trash", self)
        open_trash_action.triggered.connect(self.open_s3_trash_view)
        settings_menu.addAction(open_trash_action)
        aws_profiles_action = QAction("AWS Profiles...", self); 
        aws_profiles_action.triggered.connect(self.show_profile_manager_dialog); 
        settings_menu.addAction(aws_profiles_action)
        configure_mounts_action = QAction("Configure S3 Mounts...", self); 
        configure_mounts_action.triggered.connect(self.show_mount_config_dialog); 
        settings_menu.addAction(configure_mounts_action)
        check_update_action = QAction("Check for Updates", self)
        check_update_action.triggered.connect(lambda: self.check_for_updates(show_no_update_dialog=True))
        settings_menu.addAction(check_update_action)

        
        self.favorites_menu_ref = menubar.addMenu("&Favorites")
        self.add_fav_action_fixed = QAction("Add Current Path to Favorites...", self)
        self.add_fav_action_fixed.triggered.connect(self.add_current_path_to_favorites)
        self.favorites_menu_ref.addAction(self.add_fav_action_fixed)
        # self.favorites_menu_ref.addSeparator()
        # self.rebuild_favorites_menu([]) # Initial empty or signal-driven

        help_menu = menubar.addMenu("&Help")

        # Add Keyboard Shortcuts Reference action
        shortcuts_action = QAction("Keyboard Shortcuts Reference", self)
        shortcuts_action.triggered.connect(lambda: show_keyboard_shortcuts(self))
        help_menu.addAction(shortcuts_action)

        # Add About action
        about_action = QAction("About", self)
        about_action.triggered.connect(lambda: show_about_dialog(self))
        help_menu.addAction(about_action)
        
        toolbar = QToolBar("Main Navigation")
        toolbar.setIconSize(QSize(20, 20))
        self.addToolBar(toolbar)

        toolbar.addWidget(QLabel("Profile: "))
        self.profile_combo = QComboBox()
        self.profile_combo.setMinimumWidth(45)
        self.profile_combo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self.profile_combo.setMaximumWidth(150)
        self.update_profile_combo_display() 
        self.profile_combo.activated.connect(self.on_profile_selected_from_combo)
        toolbar.addWidget(self.profile_combo)
        toolbar.addSeparator()

        self.back_button = QAction(self.style().standardIcon(QStyle.StandardPixmap.SP_ArrowBack), "Back", self); 
        self.back_button.triggered.connect(self.go_back); 
        self.back_button.setShortcut(QKeySequence.StandardKey.Back); 
        toolbar.addAction(self.back_button)
        
        self.forward_button = QAction(self.style().standardIcon(QStyle.StandardPixmap.SP_ArrowForward), "Forward", self); 
        self.forward_button.triggered.connect(self.go_forward); 
        self.forward_button.setShortcut(QKeySequence.StandardKey.Forward); 
        toolbar.addAction(self.forward_button)
        
        self.up_button = QAction(self.style().standardIcon(QStyle.StandardPixmap.SP_ArrowUp), "Up", self); 
        self.up_button.triggered.connect(self.go_up); 
        self.up_button.setShortcut(QKeySequence("Alt+Up")); 
        toolbar.addAction(self.up_button)
        
        refresh_action = QAction(self.style().standardIcon(QStyle.StandardPixmap.SP_BrowserReload), "Refresh", self); 
        refresh_action.triggered.connect(self.refresh_view); 
        refresh_action.setShortcut(QKeySequence.StandardKey.Refresh); 
        toolbar.addAction(refresh_action)
        toolbar.addSeparator()

        server_menu = menubar.addMenu("&Server")

        self.start_webdav_action = QAction(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPlay), "Start WebDAV", self)
        self.start_webdav_action.triggered.connect(self.handle_start_webdav)
        server_menu.addAction(self.start_webdav_action)

        self.stop_webdav_action = QAction(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaStop), "Stop WebDAV", self)
        self.stop_webdav_action.triggered.connect(self.handle_stop_webdav)
        self.stop_webdav_action.setEnabled(False)
        server_menu.addAction(self.stop_webdav_action)

        
        self.active_tab_nav_container = QWidget()
        self.active_tab_nav_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.active_tab_nav_layout = QHBoxLayout(self.active_tab_nav_container)
        self.active_tab_nav_layout.setContentsMargins(0,0,0,0)
        toolbar.addWidget(self.active_tab_nav_container)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.update_status_bar_message_slot("Ready", 0)
        
        self.update_tab_widget_placeholder() # Initial placeholder if no client

    def add_new_s3_tab(self, bucket_to_open=None, path_to_open=""):
        if not self.profile_manager.get_s3_client():
            QMessageBox.information(self, "Cannot Open Tab", "Please select/configure a valid AWS Profile first.")
            if self.tab_widget.count() == 0: self.update_tab_widget_placeholder()
            return

        if self.tab_widget.count() == 1 and isinstance(self.tab_widget.widget(0), QLabel):
            self.tab_widget.removeTab(0)
            self.tab_widget.setTabsClosable(True)

        initial_bucket_for_tab = bucket_to_open
        active_prof_data = self.profile_manager.get_active_profile_data()
        
        if not initial_bucket_for_tab and active_prof_data:
            initial_bucket_for_tab = active_prof_data.get("default_s3_bucket", "")
        
        if not initial_bucket_for_tab:
             bucket_name_input, ok = QInputDialog.getText(self, "Open S3 Location", 
                                                    "Enter S3 Bucket name to open in new tab:")
             if ok and bucket_name_input:
                 initial_bucket_for_tab = bucket_name_input.strip()
             else:
                 if self.tab_widget.count() == 0: self.update_tab_widget_placeholder()
                 return

        if not initial_bucket_for_tab:
            QMessageBox.warning(self, "Cannot Open Tab", "No S3 bucket specified for the new tab.")
            if self.tab_widget.count() == 0: self.update_tab_widget_placeholder()
            return

        # Pass operation_manager to S3TabContentWidget
        tab_content = S3TabContentWidget(self, initial_bucket_for_tab, path_to_open, self.operation_manager)
        tab_content.currentS3PathChanged.connect(self.update_window_title_from_tab_signal)
        tab_content.activeFileStatusChanged.connect(self.update_save_action_state) # For save action
        # Connect selection changed from tab to update edit actions globally
        tab_content.tree_view.selectionModel().selectionChanged.connect(self.update_edit_actions_state)


        tab_title_text = os.path.basename(path_to_open.strip('/')) if path_to_open else initial_bucket_for_tab
        if not tab_title_text: tab_title_text = initial_bucket_for_tab
        
        index = self.tab_widget.addTab(tab_content, tab_title_text)
        self.tab_widget.setCurrentIndex(index)
        # on_tab_changed will handle populating view if needed

    def close_s3_tab(self, index):
        widget_to_close = self.tab_widget.widget(index)
        
        if isinstance(widget_to_close, S3TabContentWidget):
            s3_tab_count = sum(1 for i in range(self.tab_widget.count()) if isinstance(self.tab_widget.widget(i), S3TabContentWidget))
            if s3_tab_count <= 1 and self.profile_manager.get_s3_client(): # Don't close last S3 view if client is active
                QMessageBox.information(self, "Cannot Close Tab", "This is the last S3 browser tab.")
                return

        self.tab_widget.removeTab(index)
        if widget_to_close:
            if hasattr(widget_to_close, 'breadcrumb_bar_widget') and widget_to_close.breadcrumb_bar_widget.parent() == self.active_tab_nav_container:
                widget_to_close.breadcrumb_bar_widget.setParent(None)
            if hasattr(widget_to_close, 'path_edit') and widget_to_close.path_edit.parent() == self.active_tab_nav_container:
                widget_to_close.path_edit.setParent(None)
            widget_to_close.deleteLater()

        if self.tab_widget.count() == 0:
            if self.profile_manager.get_s3_client():
                 self.add_new_s3_tab() 
            else:
                self.update_tab_widget_placeholder()

    def update_tab_widget_placeholder(self):
        if not self.tab_widget: return

        current_s3_client = self.profile_manager.get_s3_client()
        
        if self.tab_widget.count() == 0 and not current_s3_client:
            placeholder_label = QLabel("No AWS Profile active or S3 client not initialized.\nPlease configure a profile via Settings > AWS Profiles.")
            placeholder_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.tab_widget.addTab(placeholder_label, "Info")
            self.tab_widget.setTabsClosable(False)
        elif self.tab_widget.count() > 0 and isinstance(self.tab_widget.widget(0), QLabel) and current_s3_client:
            self.tab_widget.removeTab(0)
            self.tab_widget.setTabsClosable(True)
            if self.tab_widget.count() == 0: # Ensure a real tab is added
                active_prof_data = self.profile_manager.get_active_profile_data()
                default_bucket = active_prof_data.get("default_s3_bucket", "") if active_prof_data else ""
                self.add_new_s3_tab(bucket_to_open=default_bucket)

    def get_active_tab_content(self) -> S3TabContentWidget | None:
        if self.tab_widget and self.tab_widget.count() > 0:
            current_widget = self.tab_widget.currentWidget()
            if isinstance(current_widget, S3TabContentWidget):
                return current_widget
        return None

    def update_profile_combo_display(self):
        if not hasattr(self, 'profile_combo'): return
        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        
        all_profiles = self.profile_manager.get_all_profiles()
        active_profile = self.profile_manager.get_active_profile_name()
        profile_names = sorted(all_profiles.keys())

        if not profile_names:
            self.profile_combo.addItem("No Profiles Configured")
            self.profile_combo.setEnabled(False)
        else:
            self.profile_combo.addItems(profile_names)
            self.profile_combo.setEnabled(True)
            if active_profile and active_profile in profile_names:
                self.profile_combo.setCurrentText(active_profile)
            elif profile_names: # Auto-select first if current active is invalid or None
                self.profile_combo.setCurrentText(profile_names[0])
                # Note: self.active_profile_name is updated by on_profile_selected_from_combo's logic path
        self.profile_combo.blockSignals(False)

    def open_s3_trash_view(self):
        if not self.profile_manager.get_s3_client():
            QMessageBox.warning(self, "S3 Error", "S3 client not connected. Cannot open Trash.")
            return

        active_profile = self.profile_manager.get_active_profile_name()
        if not active_profile: # Should not happen if client is active
            QMessageBox.warning(self, "Profile Error", "No active AWS profile.")
            return
        
        # Get current bucket from active profile or active tab if possible
        current_s3_bucket_for_trash = None
        active_tab = self.get_active_tab_content()
        if active_tab and active_tab.current_bucket:
            current_s3_bucket_for_trash = active_tab.current_bucket
        else: # Fallback to profile's default bucket or prompt
            profile_data = self.profile_manager.get_active_profile_data()
            if profile_data:
                current_s3_bucket_for_trash = profile_data.get("default_s3_bucket")
            if not current_s3_bucket_for_trash:
                # If multiple buckets, user might need to select which bucket's trash
                # For simplicity, let's assume one primary bucket or prompt
                bucket_input, ok = QInputDialog.getText(self, "Select Bucket", "Enter S3 bucket name for Trash view:")
                if ok and bucket_input:
                    current_s3_bucket_for_trash = bucket_input.strip()
                else:
                    return # User cancelled

        if not current_s3_bucket_for_trash:
            QMessageBox.warning(self, "Bucket Error", "Could not determine S3 bucket for Trash view.")
            return
            
        print(f"S3Explorer: Opening S3 Trash view for bucket '{current_s3_bucket_for_trash}', prefix '{self.S3_TRASH_PREFIX.strip('/')}'")
        # Add a new tab, or navigate an existing one, to the trash path.
        # The S3_TRASH_PREFIX should not have leading/trailing slashes when used as path_to_open.
        self.add_new_s3_tab(bucket_to_open=current_s3_bucket_for_trash, 
                            path_to_open=self.S3_TRASH_PREFIX.strip('/'))
        
        # After opening, we might want to mark this tab specially if its context menu needs to change.
        # This is handled in S3TabContentWidget.show_context_menu_tab.

    def on_profile_selected_from_combo(self, index_or_text):
        selected_profile_name = ""
        if isinstance(index_or_text, int) and index_or_text >= 0:
            selected_profile_name = self.profile_combo.itemText(index_or_text)
        elif isinstance(index_or_text, str):
            selected_profile_name = index_or_text
        
        current_active_profile = self.profile_manager.get_active_profile_name()
        if not selected_profile_name or selected_profile_name == "No Profiles Configured" or selected_profile_name == current_active_profile:
            return 

        if self.operation_manager.get_active_batch_operations_status() or self.operation_manager.get_queue_status():
            reply = QMessageBox.question(self, "Switch Profile Confirmation",
                                         "Operations are in progress or queued. Switching profiles will attempt to cancel them. Continue?",
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.No:
                self.profile_combo.setCurrentText(current_active_profile or "") 
                return
            # If yes, stop workers (ProfileManager's init will restart them if successful)
            self.operation_manager.stop_all_s3_workers(join_threads=False)


        # ProfileManager handles the actual S3 client re-initialization
        # It will emit s3_client_initialized or s3_client_init_failed
        # And active_profile_switched if the active profile name actually changes due to success
        profile_data_to_init = self.profile_manager.get_profile_data(selected_profile_name)
        if profile_data_to_init:
            if not self.profile_manager.init_s3_client_with_config(profile_data_to_init, selected_profile_name):
                # Init failed. on_s3_client_init_failed will handle UI updates.
                # Revert combo to the previously valid active profile, if any.
                if current_active_profile and current_active_profile in self.profile_manager.get_all_profiles():
                    self.profile_combo.setCurrentText(current_active_profile)
                    # And try to re-initialize with that one if current client is now None
                    if not self.profile_manager.get_s3_client():
                        self.profile_manager.attempt_s3_client_initialization(current_active_profile)
                elif self.profile_manager.get_all_profiles(): # Try first available
                    first_profile = sorted(self.profile_manager.get_all_profiles().keys())[0]
                    self.profile_combo.setCurrentText(first_profile)
                    if not self.profile_manager.get_s3_client():
                         self.profile_manager.attempt_s3_client_initialization(first_profile)
            # else: success, on_active_profile_switched and on_s3_client_initialized handle UI updates.
        else:
             QMessageBox.warning(self, "Profile Error", f"Selected profile '{selected_profile_name}' data not found.")
             if current_active_profile: self.profile_combo.setCurrentText(current_active_profile)


    def show_profile_manager_dialog(self, is_initial_setup=False):
        dialog = ProfileManagerDialog(self, profiles=self.profile_manager.get_all_profiles(), 
                                      active_profile_name=self.profile_manager.get_active_profile_name())
        if dialog.exec() == QDialog.DialogCode.Accepted:
            updated_profiles_data, new_active_profile_name_from_dialog = dialog.get_profiles_data()
            
            if updated_profiles_data is None and new_active_profile_name_from_dialog is None: # Dialog cancelled
                return

            old_active_profile_name = self.profile_manager.get_active_profile_name()
            old_active_profile_data = self.profile_manager.get_active_profile_data()
            
            self.profile_manager.update_profiles_data(updated_profiles_data, new_active_profile_name_from_dialog)
            
            # Determine if a re-initialization is needed
            re_init_needed = False
            if new_active_profile_name_from_dialog != old_active_profile_name:
                re_init_needed = True
            elif new_active_profile_name_from_dialog: # Name is same, check if data changed
                new_data_for_active = updated_profiles_data.get(new_active_profile_name_from_dialog, {})
                if new_data_for_active != old_active_profile_data:
                    re_init_needed = True
            
            # Check if only default_s3_bucket changed without other changes triggering re-init
            old_default_bucket_for_active = old_active_profile_data.get("default_s3_bucket", "") if old_active_profile_data else ""
            new_default_bucket_for_active = ""
            if new_active_profile_name_from_dialog and new_active_profile_name_from_dialog in updated_profiles_data:
                new_default_bucket_for_active = updated_profiles_data[new_active_profile_name_from_dialog].get("default_s3_bucket", "")
            
            if re_init_needed:
                if new_active_profile_name_from_dialog and new_active_profile_name_from_dialog in updated_profiles_data:
                    # Attempt to init with the new (or modified) active profile
                    profile_to_init_data = updated_profiles_data[new_active_profile_name_from_dialog]
                    # Stop workers before re-init. ProfileManager's init will restart if successful.
                    self.operation_manager.stop_all_s3_workers(join_threads=False)
                    if not self.profile_manager.init_s3_client_with_config(profile_to_init_data, new_active_profile_name_from_dialog):
                        QMessageBox.warning(self, "Profile Activation Error",
                                            f"Failed to activate profile '{new_active_profile_name_from_dialog}' after changes.")
                        # Try to revert to old active profile if it was valid
                        if old_active_profile_name and old_active_profile_data and old_active_profile_data.get("aws_access_key_id"):
                            self.profile_manager.init_s3_client_with_config(old_active_profile_data, old_active_profile_name)
                else: # No active profile selected or it's invalid
                    self.operation_manager.set_s3_client(None) # Clear client in OpManager
                    self.profile_manager.s3_client = None # Clear client in ProfileManager
                    self.profile_manager.active_profile_name = None
                    self.on_s3_client_init_failed(new_active_profile_name_from_dialog or "None", "Profile became invalid or unselected.")
                    QMessageBox.information(self, "No Active Profile", "No AWS profile is currently active.")
            elif new_active_profile_name_from_dialog == old_active_profile_name and new_default_bucket_for_active != old_default_bucket_for_active:
                # Active profile name is the same, client not re-initialized, but default bucket changed
                print("S3Explorer: Default bucket changed for active profile without re-init. Rebuilding favorites menu.")
                self.rebuild_favorites_menu() # This will pick up the new default bucket for filtering

            # Actual active profile name might be set by init_s3_client_with_config or needs to be set here if no re-init
            if not re_init_needed and new_active_profile_name_from_dialog != self.profile_manager.get_active_profile_name():
                self.profile_manager.set_active_profile_name_only(new_active_profile_name_from_dialog)

            self.profile_manager.save_aws_profiles()
            self.update_profile_combo_display()
        
        if is_initial_setup and not self.profile_manager.get_s3_client():
            QMessageBox.warning(self, "Setup Incomplete", 
                                "AWS S3 client could not be initialized. The application may not function correctly. "
                                "Please ensure a valid profile is configured and active via Settings > AWS Profiles.")

    def update_window_title_from_tab_signal(self, bucket, path_in_bucket):
        active_profile_name = self.profile_manager.get_active_profile_name()
        profile_display = f"S3 Explorer - {active_profile_name}" if active_profile_name else "S3 Explorer"
        if bucket:
            path_display = path_in_bucket if path_in_bucket else "/"
            self.setWindowTitle(f"{profile_display} - s3://{bucket}{path_display if path_display != '/' else '/'}")
            active_tab_widget = self.get_active_tab_content()
            if active_tab_widget and self.tab_widget.currentWidget() == active_tab_widget:
                idx = self.tab_widget.indexOf(active_tab_widget)
                title = os.path.basename(path_in_bucket.strip('/')) if path_in_bucket else bucket
                self.tab_widget.setTabText(idx, title if title else bucket)
        else:
            self.setWindowTitle(profile_display)

    def on_tab_changed(self, index):
        print(f"\n--- S3EXPLORER ON_TAB_CHANGED --- Index: {index}, Current Tab Count: {self.tab_widget.count()}")
        
        while self.active_tab_nav_layout.count() > 0:
            item = self.active_tab_nav_layout.takeAt(0)
            widget = item.widget()
            if widget: widget.setParent(None)

        active_tab_widget = self.get_active_tab_content()
        if active_tab_widget:
            self.active_tab_nav_layout.addWidget(active_tab_widget.breadcrumb_bar_widget)
            self.active_tab_nav_layout.addWidget(active_tab_widget.path_edit)
            self.active_tab_nav_layout.setStretchFactor(active_tab_widget.path_edit, 1)
            active_tab_widget.update_breadcrumbs_tab()

            if active_tab_widget.current_bucket and \
               not active_tab_widget.has_loaded_once and \
               not active_tab_widget.is_loading:
                active_tab_widget.populate_s3_view_tab()
            
            self.update_window_title_from_tab_signal(active_tab_widget.current_bucket, active_tab_widget.current_path)
            active_tab_widget.tree_view.setFocus()
        else:
            placeholder_nav_label = QLabel("No active S3 view. Select a profile or open a new tab.")
            placeholder_nav_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.active_tab_nav_layout.addWidget(placeholder_nav_label)
            active_profile_name = self.profile_manager.get_active_profile_name()
            self.setWindowTitle(f"S3 Explorer - {active_profile_name or 'No Profile'}")
        
        self.update_navigation_buttons_state()
        self.update_save_action_state()
        self.update_edit_actions_state()
        print(f"--- END S3EXPLORER ON_TAB_CHANGED ---\n")


    def update_navigation_buttons_state(self, enable_all=None):
        active_tab = self.get_active_tab_content()
        if enable_all is False:
            self.back_button.setEnabled(False); self.forward_button.setEnabled(False); self.up_button.setEnabled(False)
            return
        if active_tab and self.profile_manager.get_s3_client(): # Also check S3 client
            self.back_button.setEnabled(active_tab.history_index > 0)
            self.forward_button.setEnabled(active_tab.history_index < len(active_tab.path_history) - 1)
            self.up_button.setEnabled(bool(active_tab.current_bucket) and bool(active_tab.current_path))
        else:
            self.back_button.setEnabled(False); self.forward_button.setEnabled(False); self.up_button.setEnabled(False)

    def update_edit_actions_state(self, *_):
        active_tab = self.get_active_tab_content()
        has_selection = False
        s3_client_is_active = bool(self.profile_manager.get_s3_client())
        can_paste = bool(self.s3_clipboard) and s3_client_is_active

        if active_tab and self.profile_manager.get_s3_client():
            selected_indexes = active_tab.tree_view.selectionModel().selectedRows()
            has_selection = bool(selected_indexes)
            if not active_tab.current_bucket: can_paste = False
        else:
            has_selection = False; can_paste = False

        self.copy_action.setEnabled(has_selection)
        self.cut_action.setEnabled(has_selection)
        self.paste_action.setEnabled(can_paste)
    def handle_start_webdav(self):
        profile = self.profile_manager.get_active_profile_data()
        if not profile:
            QMessageBox.warning(self, "WebDAV Error", "No active AWS profile selected.")
            return

        try:
            access_key = profile["aws_access_key_id"]
            secret_key = profile["aws_secret_access_key"]
            endpoint = profile.get("endpoint_url", "https://s3.amazonaws.com")
            region = profile.get("region_name", "us-east-1")
            bucket = profile["default_s3_bucket"]
        except KeyError as e:
            QMessageBox.warning(self, "WebDAV Error", f"Missing profile field: {e}")
            return

        mount_path = os.path.join(tempfile.gettempdir(), "webdav_mount")
        os.makedirs(mount_path, exist_ok=True)

        self.webdav_thread = threading.Thread(
            target=start_webdav,
            args=(mount_path, access_key, secret_key, region, endpoint, bucket),
            daemon=True
        )
        self.webdav_thread.start()
        self.start_webdav_action.setEnabled(False)
        self.stop_webdav_action.setEnabled(True)
        self.status_bar.showMessage("WebDAV server started", 3000)

    def handle_stop_webdav(self):
        try:
            stop_webdav()
            self.status_bar.showMessage("WebDAV server stopped", 3000)
        except Exception as e:
            QMessageBox.warning(self, "WebDAV Error", f"Failed to stop server: {e}")
        self.start_webdav_action.setEnabled(True)
        self.stop_webdav_action.setEnabled(False)

    @pyqtSlot(str, bool) # s3_key_changed, is_modified_from_signal
    def on_temp_file_status_changed_update_save_action(self, s3_key_changed, is_modified_from_signal):
        # This slot is triggered when TempFileManager has determined the modified status.
        # It should use the provided 'is_modified_from_signal' status directly.
        print(f"S3EXPLORER: Slot on_temp_file_status_changed for '{s3_key_changed}', is_modified: {is_modified_from_signal}")

        active_tab = self.get_active_tab_content()
        if not active_tab: 
            self.save_active_file_action.setEnabled(False)
            return

        # Check if the currently selected file in the active tab is the one whose status changed
        selected_indexes = active_tab.tree_view.selectionModel().selectedRows(COL_S3_KEY)
        if len(selected_indexes) == 1:
            row = selected_indexes[0].row()
            s3_key_item = active_tab.model.item(row, COL_S3_KEY) # Get QStandardItem
            is_folder_item = active_tab.model.item(row, COL_IS_FOLDER) # Get QStandardItem

            if s3_key_item and is_folder_item and is_folder_item.text() == "0": # It's a file
                current_selected_s3_key = s3_key_item.text()
                if current_selected_s3_key == s3_key_changed:
                    # The S3 key of the currently selected file matches the key from the signal.
                    # Use the 'is_modified_from_signal' value directly.
                    file_data = self.temp_file_manager.get_temp_file_data(s3_key_changed)
                    if file_data and file_data['s3_bucket'] == active_tab.current_bucket:
                        # VVVV USE THE SIGNAL'S VALUE VVVV
                        self.save_active_file_action.setEnabled(is_modified_from_signal)
                        # ^^^^ USE THE SIGNAL'S VALUE ^^^^
                        print(f"  S3EXPLORER: Updated save action for selected '{s3_key_changed}' to {is_modified_from_signal} based on signal.")
                        return # The save action for the current selection has been updated.
                    else:
                        print(f"  S3EXPLORER: File data for '{s3_key_changed}' not found or bucket mismatch in on_temp_file_status_changed.")
                else:
                    print(f"  S3EXPLORER: Selected key '{current_selected_s3_key}' does not match signaled key '{s3_key_changed}'. Doing full update.")
            else:
                 print(f"  S3EXPLORER: No file selected or item is a folder in on_temp_file_status_changed.")

        # If the changed file is not the currently selected one, or if multiple/no items are selected,
        # a full refresh of the save action state might be needed if the save action could
        # depend on something other than just the single selected file.
        # However, current `update_save_action_state` only checks the single selected file.
        # So, if the signal was for a non-selected file, `update_save_action_state` will correctly
        # reflect the status of the *currently selected* file.
        print(f"  S3EXPLORER: Falling back to full update_save_action_state() in on_temp_file_status_changed.")
        self.update_save_action_state()


    def update_save_action_state(self, *_):
        is_active_s3_file_modified = False
        active_tab = self.get_active_tab_content()
        print(f"S3EXPLORER: update_save_action_state called. Active tab: {active_tab}")

        if active_tab and self.profile_manager.get_s3_client():
            selected_indexes = active_tab.tree_view.selectionModel().selectedRows(COL_S3_KEY)
            if len(selected_indexes) == 1:
                row = selected_indexes[0].row()
                s3_key_item = active_tab.model.item(row, COL_S3_KEY)
                is_folder_item = active_tab.model.item(row, COL_IS_FOLDER)

                if s3_key_item and is_folder_item and is_folder_item.text() == "0":
                    s3_key = s3_key_item.text()
                    file_data = self.temp_file_manager.get_temp_file_data(s3_key)
                    print(f"  Checking selected file for save state: {s3_key}, bucket: {active_tab.current_bucket}")
                    if file_data and file_data['s3_bucket'] == active_tab.current_bucket:
                        is_loc_mod, _, _, _ = self.temp_file_manager.check_single_temp_file_modified_status(s3_key, self.profile_manager.get_s3_client())
                        is_active_s3_file_modified = is_loc_mod
                        print(f"    Result from check_single_temp_file_modified_status: is_loc_mod={is_loc_mod}")
                    else:
                        print(f"    No tracked temp file data for {s3_key} in bucket {active_tab.current_bucket}, or file_data missing.")
        
        self.save_active_file_action.setEnabled(is_active_s3_file_modified)
        print(f"  Save action enabled: {self.save_active_file_action.isEnabled()}")


    # --- Global Action Handlers (delegated to active tab or managers) ---
    def go_back(self):
        active_tab = self.get_active_tab_content()
        if active_tab:
            active_tab.go_back_tab()
            self.setFocus()  

    def go_forward(self):
        active_tab = self.get_active_tab_content()
        if active_tab:
            active_tab.go_forward_tab()
            self.setFocus()  

    def go_up(self):
        active_tab = self.get_active_tab_content()
        if active_tab:
            active_tab.go_up_tab()
            self.setFocus() 
        
    def refresh_view(self):
        active_tab = self.get_active_tab_content()
        if active_tab and self.profile_manager.get_s3_client():
            active_tab.populate_s3_view_tab()
        elif not self.profile_manager.get_s3_client():
            self.update_status_bar_message_slot("Cannot refresh: No S3 connection.", 3000)
        else:
            self.update_status_bar_message_slot("No active S3 tab to refresh.", 3000)

    # --- S3 Item Actions (Copy/Cut/Paste) ---
    def handle_copy_s3_items(self):
        active_tab = self.get_active_tab_content()
        if not active_tab or not active_tab.current_bucket or not self.profile_manager.get_s3_client():
            QMessageBox.warning(self, "Copy Error", "No active S3 tab/bucket or S3 client not ready.")
            return
        keys, is_folder, _ = active_tab.get_selected_s3_items_info_tab()
        if keys:
            self.s3_clipboard = {'type': 'copy', 'source_bucket': active_tab.current_bucket, 'keys': keys, 'is_folder': is_folder}
            self.update_status_bar_message_slot(f"{len(keys)} item(s) copied to S3 clipboard.", 3000)
            self.update_edit_actions_state()

    def handle_cut_s3_items(self):
        active_tab = self.get_active_tab_content()
        if not active_tab or not active_tab.current_bucket or not self.profile_manager.get_s3_client():
            QMessageBox.warning(self, "Cut Error", "No active S3 tab/bucket or S3 client not ready.")
            return
        keys, is_folder, _ = active_tab.get_selected_s3_items_info_tab()
        if keys:
            self.s3_clipboard = {'type': 'cut', 'source_bucket': active_tab.current_bucket, 'keys': keys, 'is_folder': is_folder}
            self.update_status_bar_message_slot(f"{len(keys)} item(s) cut (for move).", 3000)
            self.update_edit_actions_state()

    def handle_paste_s3_items(self):
        active_tab = self.get_active_tab_content()
        s3_client = self.profile_manager.get_s3_client()
        if not self.s3_clipboard: QMessageBox.warning(self, "Paste Error", "S3 Clipboard is empty."); return
        if not s3_client: QMessageBox.warning(self, "Paste Error", "S3 client not initialized."); return
        if not active_tab or not active_tab.current_bucket: QMessageBox.warning(self, "Paste Error", "No active S3 destination tab/bucket."); return

        source_bucket_clip = self.s3_clipboard['source_bucket']
        source_keys_clip = self.s3_clipboard['keys']
        source_is_folder_flags_clip = self.s3_clipboard['is_folder']
        operation_mode = self.s3_clipboard['type']
        
        dest_bucket_current_tab = active_tab.current_bucket
        dest_path_prefix_current_tab = active_tab.current_path + ('/' if active_tab.current_path and not active_tab.current_path.endswith('/') else '')
        
        active_tab.tree_view.setEnabled(False) # Disable target view during paste
        
        batch_op_type_str = "Copying" if operation_mode == 'copy' else "Moving"
        current_batch_id = f"{batch_op_type_str.lower()}_{time.time()}"
        
        items_to_process_for_batch = []
        try:
            for i, top_src_full_key in enumerate(source_keys_clip):
                top_src_is_folder = source_is_folder_flags_clip[i]
                top_src_base_name = os.path.basename(top_src_full_key.rstrip('/'))

                if top_src_is_folder:
                    list_prefix = top_src_full_key if top_src_full_key.endswith('/') else top_src_full_key + '/'
                    paginator = s3_client.get_paginator('list_objects_v2')
                    all_objects_in_source_folder = []
                    for page in paginator.paginate(Bucket=source_bucket_clip, Prefix=list_prefix):
                        all_objects_in_source_folder.extend(obj['Key'] for obj in page.get('Contents', []))

                    if not all_objects_in_source_folder: # Empty source folder
                        dest_folder_key = dest_path_prefix_current_tab + top_src_base_name + '/'
                        op = S3Operation(S3OpType.CREATE_FOLDER, dest_bucket_current_tab, key=dest_folder_key)
                        items_to_process_for_batch.append(op)
                    else:
                        for src_obj_key in all_objects_in_source_folder:
                            relative_path = src_obj_key[len(list_prefix):]
                            dest_obj_key = dest_path_prefix_current_tab + top_src_base_name + '/' + relative_path
                            if src_obj_key == dest_obj_key and source_bucket_clip == dest_bucket_current_tab: continue
                            
                            cb_data = {}
                            if source_bucket_clip != dest_bucket_current_tab: cb_data["source_bucket_override"] = source_bucket_clip
                            op = S3Operation(S3OpType.COPY_OBJECT, dest_bucket_current_tab, key=src_obj_key, new_key=dest_obj_key, 
                                             is_part_of_move=(operation_mode == 'cut'),
                                             original_source_key_for_move=src_obj_key if operation_mode == 'cut' else None,
                                             callback_data=cb_data)
                            items_to_process_for_batch.append(op)
                else: # Single file
                    dest_file_key = dest_path_prefix_current_tab + top_src_base_name
                    if top_src_full_key == dest_file_key and source_bucket_clip == dest_bucket_current_tab: continue
                    
                    cb_data = {}
                    if source_bucket_clip != dest_bucket_current_tab: cb_data["source_bucket_override"] = source_bucket_clip
                    op = S3Operation(S3OpType.COPY_OBJECT, dest_bucket_current_tab, key=top_src_full_key, new_key=dest_file_key,
                                     is_part_of_move=(operation_mode == 'cut'),
                                     original_source_key_for_move=top_src_full_key if operation_mode == 'cut' else None,
                                     callback_data=cb_data)
                    items_to_process_for_batch.append(op)
        except Exception as e:
            QMessageBox.warning(self, "Paste Error", f"Could not prepare paste operation (e.g., list source folder '{top_src_full_key}'): {e}")
            active_tab.tree_view.setEnabled(True)
            return

        if not items_to_process_for_batch:
            QMessageBox.information(self, "Paste", "No items to paste.")
            active_tab.tree_view.setEnabled(True)
            return

        extra_batch_data = {
            'is_cut_operation': (operation_mode == 'cut'),
            'original_top_level_sources_for_cut': list(zip(source_keys_clip, source_is_folder_flags_clip)) if operation_mode == 'cut' else [],
            'target_tab_ref': active_tab, 
            'target_bucket': dest_bucket_current_tab,
            'source_bucket_for_cut_cleanup': source_bucket_clip if operation_mode == 'cut' else None
        }
        self.operation_manager.start_batch_operation(current_batch_id, len(items_to_process_for_batch), 
                                                     batch_op_type_str, items_to_process_for_batch, extra_batch_data)

    @pyqtSlot(str) # batch_id
    def on_batch_operation_complete_from_op_mgr(self, batch_id: str):
        batch_data = self.operation_manager.get_active_batch_operation_data(batch_id)
        if not batch_data:
            print(f"S3Explorer: Batch data for ID '{batch_id}' not found or already cleared.")
            return

        op_type_display = batch_data.get('op_type_display', "Operation") # Default display name
        completed_count = batch_data.get('completed', 0)
        failed_count = batch_data.get('failed', 0)
        success_count = completed_count - failed_count

        final_message = f"{op_type_display} complete. Successful: {success_count}, Failed: {failed_count}."

        target_tab_ref = batch_data.get('target_tab_ref') # Could be S3TabContentWidget or None
        
        # Re-enable the target tab's tree view if it was disabled during the operation
        # This is particularly relevant for paste operations. Drag-drop doesn't disable the view.
        if target_tab_ref and isinstance(target_tab_ref, S3TabContentWidget) and not target_tab_ref.tree_view.isEnabled():
            target_tab_ref.tree_view.setEnabled(True)


        # --- Specific logic for "cut" operations (paste after cut) ---
        is_cut_operation = batch_data.get('is_cut_operation', False)
        if is_cut_operation:
            if failed_count == 0: # All items in the "cut" (which is a copy then delete) batch succeeded
                original_top_sources = batch_data.get('original_top_level_sources_for_cut', [])
                source_bucket_for_delete = batch_data.get('source_bucket_for_cut_cleanup')

                if source_bucket_for_delete and original_top_sources:
                    delete_ops_for_original_folders = []
                    for orig_key, orig_is_folder in original_top_sources:
                        if orig_is_folder: # Only original top-level FOLDERS need explicit DELETE_FOLDER here
                                           # (individual files are deleted by worker if copy was part of move)
                            folder_key_to_delete = orig_key if orig_key.endswith('/') else orig_key + '/'
                            del_op = S3Operation(S3OpType.DELETE_FOLDER, source_bucket_for_delete, key=folder_key_to_delete,
                                                 callback_data={"is_cleanup_delete": True, 
                                                                "original_batch_id_for_cut": batch_id}) # Link back
                            delete_ops_for_original_folders.append(del_op)
                    
                    if delete_ops_for_original_folders:
                        cleanup_batch_id = f"cleanup_delete_after_cut_{time.time()}"
                        cleanup_extra_data = {
                            'target_tab_ref': target_tab_ref, # Pass along for UI updates after cleanup
                            'original_source_bucket_for_refresh': source_bucket_for_delete,
                            # Add any other context needed for when this cleanup batch finishes
                        }
                        print(f"S3Explorer: Cut operation successful, queueing cleanup batch '{cleanup_batch_id}' to delete original folders.")
                        self.operation_manager.start_batch_operation(
                            cleanup_batch_id, 
                            len(delete_ops_for_original_folders),
                            "Deleting original cut folders", 
                            delete_ops_for_original_folders, 
                            cleanup_extra_data
                        )
                        # Clear the S3 clipboard now that the primary move is done and cleanup is queued
                        if self.s3_clipboard and self.s3_clipboard.get('type') == 'cut':
                            self.s3_clipboard = None
                            self.update_edit_actions_state()
                        
                        self.operation_manager.clear_batch_operation_data(batch_id) # Clear the original cut batch data
                        return # IMPORTANT: Don't proceed further; wait for the cleanup batch to complete.
            else: # Cut operation had failures
                final_message += " Some items may not have been moved, and originals were not deleted."
            
            # Clear S3 clipboard for cut operations, regardless of success/failure of the main batch,
            # if not already cleared (e.g., if no cleanup batch was started).
            if self.s3_clipboard and self.s3_clipboard.get('type') == 'cut':
                self.s3_clipboard = None
                self.update_edit_actions_state()
        
        # --- General batch completion (including drag-drop, copy, or failed/partial cuts) ---
        self.update_status_bar_message_slot(final_message, 7000)
        self.operation_manager.clear_batch_operation_data(batch_id) # Clear the completed batch's data

        # --- Refresh UI based on batch context ---
        # For Drag-and-Drop or Paste operations, refresh the target tab
        # batch_data would have 'target_bucket' and 'target_tab_ref'
        # (and 'target_path_prefix' for drag-drop, though not explicitly used here for refresh,
        # as populate_s3_view_tab uses tab's current_path)
        if target_tab_ref and isinstance(target_tab_ref, S3TabContentWidget):
            target_bucket_from_batch = batch_data.get('target_bucket')
            if target_tab_ref.current_bucket == target_bucket_from_batch:
                print(f"S3Explorer: Batch '{batch_id}' ({op_type_display}) complete. Refreshing target tab: "
                      f"s3://{target_tab_ref.current_bucket}/{target_tab_ref.current_path}")
                target_tab_ref.populate_s3_view_tab()
            else:
                print(f"S3Explorer: Batch '{batch_id}' ({op_type_display}) complete, but target tab's bucket "
                      f"('{target_tab_ref.current_bucket}') differs from batch target ('{target_bucket_from_batch}'). "
                      f"May need a broader refresh or specific handling.")
                # Fallback: if target bucket from batch is known, refresh all views for it.
                if target_bucket_from_batch:
                    self.refresh_views_for_bucket(target_bucket_from_batch)

        # For "cleanup_delete_after_cut" batches, refresh views of the source bucket from where items were deleted.
        original_source_bucket_to_refresh = batch_data.get('original_source_bucket_for_refresh')
        if original_source_bucket_to_refresh:
            print(f"S3Explorer: Cleanup batch '{batch_id}' complete. Refreshing source bucket: '{original_source_bucket_to_refresh}'")
            self.refresh_views_for_bucket(original_source_bucket_to_refresh)
        
        # For "move folder" batches (like trash/restore), refresh source and destination
        source_prefix_moved = batch_data.get('source_prefix_moved')
        destination_prefix_moved = batch_data.get('destination_prefix_moved')
        target_bucket = batch_data.get('target_bucket') # Bucket is same for intra-bucket moves
        if source_prefix_moved and target_bucket:
            self.refresh_views_for_bucket_path(target_bucket, os.path.dirname(source_prefix_moved.strip('/')))
        if destination_prefix_moved and target_bucket and destination_prefix_moved != source_prefix_moved:
            self.refresh_views_for_bucket_path(target_bucket, os.path.dirname(destination_prefix_moved.strip('/')))


    # --- Slots for OperationManager Signals (specific operation outcomes) ---
    @pyqtSlot(object, object, str) # S3Operation, result, error_message
    def on_op_mgr_list_op_completed(self, operation, result, error_message):
        # LIST ops are mostly handled by S3TabContentWidget directly via callback_data['tab_widget_ref']
        # This slot is for any global actions S3Explorer might need to take after a LIST.
        # For example, logging or global state update based on LIST.
        # Currently, S3TabContentWidget.on_s3_list_finished_tab does the UI update.
        # S3Explorer doesn't need to do much here other than maybe status updates if not handled by tab.
        if error_message:
            # Tab might show its own error, or S3Explorer can show a generic one
            # self.update_status_bar_message_slot(f"List error in {operation.bucket}/{operation.key}: {error_message}", 5000)
            pass

    @pyqtSlot(object, object, str)
    def on_op_mgr_delete_finished(self, operation, result, error_message):
        if error_message:
            self.update_status_bar_message_slot(f"Delete failed for '{operation.key}': {error_message}", 5000)
            QMessageBox.critical(self, "Delete Error", f"Failed to delete '{operation.key}':\n{error_message}")
        else:
            self.update_status_bar_message_slot(f"Item '{operation.key}' deleted successfully.", 3000)
            self.refresh_views_for_bucket_path(operation.bucket, os.path.dirname(operation.key.strip('/')))

    # Slot for OperationManager's download_to_temp_op_completed
    @pyqtSlot(object, object, str) # S3Operation, result, error_message
    def on_op_mgr_download_to_temp_finished(self, operation: S3Operation, result: dict, error_message: str):
        # This slot handles the completion of any DOWNLOAD_TO_TEMP operation.
        # We differentiate based on 'ui_source' in callback_data.
        s3_key_for_lock_release = operation.callback_data.get('s3_key_for_lock_release')
        if s3_key_for_lock_release:
            self._clear_opening_file_lock(s3_key_for_lock_release)
        else: # Fallback if key wasn't in callback_data (it should be)
            self._clear_opening_file_lock(operation.key)

        intended_local_path = operation.callback_data.get('intended_local_path') # Path requested by S3Explorer
        s3_key_operated_on = operation.key
        bucket_operated_on = operation.bucket
        original_filename_from_cb = operation.callback_data.get('original_filename', os.path.basename(intended_local_path or s3_key_operated_on))

        if operation.callback_data.get('ui_source') == 'live_edit_open':
            # --- Handling for 'live_edit_open' ---
            if error_message:
                self.update_status_bar_message_slot(f"Open for edit failed for '{s3_key_operated_on}': {error_message}", 5000)
                QMessageBox.critical(self, "S3 Download Error",
                                     f"Could not download S3 file '{s3_key_operated_on}' for editing:\n{error_message}")
                if intended_local_path and os.path.exists(intended_local_path): # Cleanup failed/partial download
                    try: os.remove(intended_local_path)
                    except OSError as e_rem: print(f"S3Explorer: Error cleaning up failed download {intended_local_path}: {e_rem}")
                return

            # Check if OperationManager's os.startfile call failed
            if result and result.get("open_error"):
                QMessageBox.critical(self, "OS Open Error",
                                     f"Could not open the downloaded local file for '{s3_key_operated_on}':\n{result['open_error']}")
                # File was downloaded, but OS couldn't open it. It's now a local temp file.
                # We might still track it, or decide to clean it up. For now, let's not track if OS open failed.
                if intended_local_path and os.path.exists(intended_local_path):
                    try: os.remove(intended_local_path) # Clean up if OS open failed
                    except OSError as e_rem: print(f"S3Explorer: Error cleaning up after OS open fail {intended_local_path}: {e_rem}")
                return

            # At this point, download was successful AND os.startfile (or equivalent) was successful
            actual_downloaded_path = result.get("temp_path") or result.get("local_path") # Path where worker saved it

            if not actual_downloaded_path or not os.path.exists(actual_downloaded_path):
                QMessageBox.critical(self, "Internal Error",
                                     f"Temporary file for '{s3_key_operated_on}' not found after successful download report.")
                return

            # Fetch S3 mtime for accurate tracking
            original_s3_mtime = None
            s3_client = self.profile_manager.get_s3_client()
            if s3_client:
                try:
                    head = s3_client.head_object(Bucket=bucket_operated_on, Key=s3_key_operated_on)
                    original_s3_mtime = head.get('LastModified')
                except Exception as e_head:
                    print(f"S3Explorer: Could not get S3 mtime for '{s3_key_operated_on}' on open: {e_head}")

            # Track with TempFileManager
            current_local_mtime = os.path.getmtime(actual_downloaded_path)
            self.temp_file_manager.track_opened_temp_file(
                s3_key=s3_key_operated_on,      # The S3 key
                temp_path=actual_downloaded_path, # The actual local path of the temp file
                s3_bucket=bucket_operated_on,
                original_s3_mtime=original_s3_mtime,
                local_mtime_on_open=current_local_mtime,
                # Add a flag for watchdog ignore, to be set *after* editor likely opened it
                # This is better handled by setting 'ignore_watchdog_until_sync' in the tracking data itself
            )
            
            # Update the tracking data with an initial ignore period for watchdog
            # This helps if the editor immediately modifies the file upon opening.
            tracked_file_data = self.temp_file_manager.get_temp_file_data(s3_key_operated_on)
            if tracked_file_data:
                tracked_file_data["ignore_watchdog_until_sync"] = time.time() + 2.0 # Ignore for 2 seconds
                print(f"S3Explorer: Set initial watchdog ignore for '{actual_downloaded_path}' for 2s after opening.")

            print(f"S3Explorer: Tracked for live edit: '{actual_downloaded_path}' -> s3://{bucket_operated_on}/{s3_key_operated_on}")
            self.update_status_bar_message_slot(f"File '{original_filename_from_cb}' opened for editing.", 3000)
            self.update_save_action_state() # Update save button state

        else:
            # --- Handling for other (generic) DOWNLOAD_TO_TEMP uses, if any ---
            print(f"S3Explorer: Generic DOWNLOAD_TO_TEMP finished for {s3_key_operated_on}. Error: '{error_message or 'OK'}'")
            if not error_message and result:
                s3_key_result = result.get("s3_key")
                local_path_result = result.get("temp_path") or result.get("local_path")
                s3_bucket_result = result.get("s3_bucket")

                if s3_key_result and local_path_result and os.path.exists(local_path_result):
                    # This path is where OperationManager._handle_download_to_temp_finished would have called os.startfile
                    # If you want to track these generic temp files too (e.g., for a manual save button, but not auto-sync):
                    # self.temp_file_manager.track_opened_temp_file(
                    #     s3_key=s3_key_result,
                    #     temp_path=local_path_result,
                    #     s3_bucket=s3_bucket_result,
                    #     original_s3_mtime=None, # Fetch if needed
                    #     local_mtime_on_open=os.path.getmtime(local_path_result)
                    # )
                    # self.update_save_action_state()
                    self.update_status_bar_message_slot(f"File '{s3_key_result}' opened from temporary location.", 3000)
                elif result and result.get("open_error"):
                     QMessageBox.warning(self, "OS Open Error",
                                        f"Could not open the downloaded local file for '{s3_key_result}':\n{result['open_error']}")
            # No specific action for generic temp files beyond what OperationManager did (os.startfile)
            # unless you decide to track them for manual save.

    @pyqtSlot(object, object, str)
    def on_op_mgr_download_file_finished(self, operation, result, error_message):
        if error_message:
            self.update_status_bar_message_slot(f"Download of {operation.key} failed: {error_message}", 5000)
            QMessageBox.critical(self, "Download Error",f"Download of {operation.key} failed:\n{error_message}")
        else:
            local_path = result["local_path"]
            self.update_status_bar_message_slot(f"File {operation.key} downloaded to {local_path}", 5000)
            QMessageBox.information(self, "Download Complete", f"File '{os.path.basename(operation.key)}' downloaded to:\n{local_path}")

    @pyqtSlot(object, object, str) # S3Operation, result from worker, error_message
    def on_op_mgr_upload_finished(self, operation: S3Operation, result, error_message: str):
        s3_key_involved = operation.key # The S3 key targeted by the upload
        bucket_involved = operation.bucket
        local_path_that_was_uploaded = operation.local_path # The local file that was the source

        is_live_edit_sync = operation.callback_data.get("is_live_edit_sync", False)
        is_manual_temp_save = operation.callback_data.get("is_temp_file_update", False)
        # original_local_path_for_live_edit is used to find the entry in TempFileManager
        original_local_path_for_live_edit = operation.callback_data.get("original_local_path", local_path_that_was_uploaded)


        if error_message:
            # Generic error display for any upload failure
            error_display_key = os.path.basename(local_path_that_was_uploaded) or s3_key_involved
            self.update_status_bar_message_slot(f"Upload of '{error_display_key}' FAILED: {error_message}", 7000)
            QMessageBox.critical(self, "Upload Error", f"Upload of '{error_display_key}' failed:\n{error_message}")

            # If it was a live edit sync that failed, TempFileManager doesn't update mtimes,
            # so the file will likely still appear as modified and eligible for "Save Active File" or another sync attempt.
            # The "Save Active File" button state should reflect this.
            if is_live_edit_sync or is_manual_temp_save:
                self.update_save_action_state() # Ensure save button state is correct after failure
            return # Stop further processing on error

        # --- Upload Succeeded ---
        display_name_for_success = os.path.basename(local_path_that_was_uploaded) or s3_key_involved

        if is_live_edit_sync:
            self.update_status_bar_message_slot(f"Auto-sync of '{display_name_for_success}' to S3 successful.", 4000)
            # Refresh the view where this file might be visible
            self.refresh_views_for_bucket_path(bucket_involved, os.path.dirname(s3_key_involved.strip('/')))
            
            # Notify TempFileManager about the successful upload to update its internal mtimes.
            # This will trigger temp_file_modified_status_changed -> on_temp_file_status_changed_update_save_action
            # which should then set the "Save Active File" button to disabled for this file.
            s3_key_to_update_tracking = operation.callback_data.get('s3_key_for_tracking', operation.key)
            self.temp_file_manager.handle_temp_file_upload_success(
                s3_key=s3_key_to_update_tracking, # The S3 key that was updated
                s3_bucket=bucket_involved,
                uploaded_local_path=original_local_path_for_live_edit, # The path tracked by watchdog
                s3_client_ref=self.profile_manager.get_s3_client()
            )
            # update_save_action_state() will be called via the signal from TempFileManager

        elif is_manual_temp_save:
            self.update_status_bar_message_slot(f"Saved '{display_name_for_success}' to S3.", 4000)
            self.refresh_views_for_bucket_path(bucket_involved, os.path.dirname(s3_key_involved.strip('/')))

            # Notify TempFileManager for manual "Save Active File"
            self.temp_file_manager.handle_temp_file_upload_success(
                s3_key=s3_key_involved,
                s3_bucket=bucket_involved,
                uploaded_local_path=local_path_that_was_uploaded, # This was the temp file path itself
                s3_client_ref=self.profile_manager.get_s3_client()
            )
            # update_save_action_state() will be called via the signal from TempFileManager

        else: # Generic upload (e.g., "Save Local As S3...")
            self.update_status_bar_message_slot(f"File '{display_name_for_success}' uploaded successfully to s3://{bucket_involved}/{s3_key_involved}.", 5000)
            self.refresh_views_for_bucket_path(bucket_involved, os.path.dirname(s3_key_involved.strip('/')))
            # No specific TempFileManager interaction needed for generic uploads unless they also become "opened" files.

        # Note: self.update_save_action_state() is now primarily triggered by the
        # temp_file_modified_status_changed signal from TempFileManager after
        # handle_temp_file_upload_success is called for live-edit or manual temp saves.
        # For generic uploads, the save state isn't directly affected unless the view refresh
        # changes the selection to a tracked temp file.

    @pyqtSlot(object, object, str)
    def on_op_mgr_create_folder_finished(self, operation, result, error_message):
        if error_message:
            self.update_status_bar_message_slot(f"Folder creation for '{operation.key}' failed: {error_message}", 5000)
            QMessageBox.critical(self, "Create Folder Error", f"Folder creation for '{operation.key}' failed:\n{error_message}")
        else:
            self.update_status_bar_message_slot(f"Folder '{operation.key}' created in {operation.bucket}.", 3000)
            self.refresh_views_for_bucket_path(operation.bucket, os.path.dirname(operation.key.strip('/')))

    @pyqtSlot(object, object, str)
    def on_op_mgr_copy_object_finished(self, operation, result, error_message):
        if error_message:
            self.update_status_bar_message_slot(f"Copy/Move from '{operation.key}' to '{operation.new_key}' failed: {error_message}", 5000)
            # QMessageBox handled by batch or individual error popups if not batch
        else:
            msg = f"Copied '{result['source_key']}' to '{result['dest_key']}'."
            if operation.is_part_of_move:
                if result.get("original_deleted"):
                    msg = f"Moved '{result['source_key']}' to '{result['dest_key']}'."
                    self.refresh_views_for_bucket_path(result['source_bucket'], os.path.dirname(result['source_key'].strip('/')))
                else:
                    msg += f" Original NOT deleted from {result['source_bucket']}. Error: {result.get('original_delete_error', 'Unknown')}"
            
            self.update_status_bar_message_slot(msg, 5000)
            self.refresh_views_for_bucket_path(result['dest_bucket'], os.path.dirname(result['dest_key'].strip('/')))

    @pyqtSlot(str, str, str, bool) # local_path, s3_key_to_act_on, s3_bucket, is_potential_folder
    def handle_mount_deletion_confirmation(self, local_path_deleted: str, s3_key_to_act_on: str, s3_bucket: str, is_potential_folder: bool):
        if not self.profile_manager.get_s3_client():
            QMessageBox.warning(self, "S3 Error", "S3 client not connected. Cannot process S3 operation.")
            return

        item_type_str = "folder" if is_potential_folder else "file"
        local_item_name = os.path.basename(local_path_deleted)
        
        message = (f"The local {item_type_str} '{local_item_name}' in your mounted path has been deleted:\n"
                   f"{local_path_deleted}\n\n"
                   f"Do you want to move the corresponding S3 {item_type_str} to the S3 Trash?\n"
                   f"S3 Item: s3://{s3_bucket}/{s3_key_to_act_on}")

        reply = QMessageBox.question(self, f"Move S3 Item to Trash?",
                                     message,
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)

        if reply == QMessageBox.StandardButton.Yes:
            # Construct the destination key in the trash
            # s3_key_to_act_on could be "path/file.txt" or "path/folder/"
            # We want trash path to be "_S3_EXPLORER_TRASH_/path/file.txt" or "_S3_EXPLORER_TRASH_/path/folder/"
            
            original_key_relative_part = s3_key_to_act_on # This is already the full key from bucket root
            
            # Ensure no leading slash if original_key_relative_part is at root, to prevent "//"
            if original_key_relative_part.startswith('/'):
                original_key_relative_part = original_key_relative_part[1:]

            s3_trash_dest_key = S3_TRASH_PREFIX + original_key_relative_part

            print(f"S3Explorer: User confirmed moving to trash. "
                  f"Source: s3://{s3_bucket}/{s3_key_to_act_on} -> Dest: s3://{s3_bucket}/{s3_trash_dest_key}")
            self.update_status_bar_message_slot(f"Queueing move of '{s3_key_to_act_on}' to S3 Trash...", 0)

            # --- This is where it gets complex for folders ---
            # For now, let's assume S3SyncEventHandler correctly identified if it's a folder
            # and `s3_key_to_act_on` is the folder prefix (ends with /).
            # Moving a folder requires moving all its contents. This is a batch operation.
            
            if is_potential_folder:
                # This is a complex operation: list all objects under s3_key_to_act_on,
                # then for each object, queue a COPY_OBJECT to the new trash location (preserving relative path),
                # and mark it as part of a move (so worker deletes original object).
                # After all contents are moved, the original (now empty) folder structure/markers might need cleanup.
                # This is similar to the paste logic for folders.
                self._move_s3_folder_to_trash_batch(s3_bucket, s3_key_to_act_on, s3_trash_dest_key)
            else: # It's a single file
                move_op = S3Operation(
                    S3OpType.COPY_OBJECT, 
                    bucket=s3_bucket, # Destination bucket for copy is the same
                    key=s3_key_to_act_on, # Source key
                    new_key=s3_trash_dest_key, # Destination key in trash
                    is_part_of_move=True, # So worker deletes the original after copy
                    original_source_key_for_move=s3_key_to_act_on,
                    callback_data={'ui_source': 'mount_sync_move_to_trash_file'}
                )
                self.operation_manager.enqueue_s3_operation(move_op)
        else:
            self.update_status_bar_message_slot(f"Move to S3 Trash for '{s3_key_to_act_on}' cancelled by user.", 3000)
            print(f"S3Explorer: User cancelled move to S3 Trash for s3://{s3_bucket}/{s3_key_to_act_on}")

    # --- File Actions (Open, Download, Delete, Create Folder - called by S3TabContentWidget) ---
    # These methods now mostly create S3Operations and enqueue them via OperationManager
    # S3TabContentWidget will call these on its self.main_window reference.

    def _move_s3_folder_to_trash_batch(self, s3_bucket: str, source_folder_prefix: str, trash_dest_folder_prefix: str):
        """
        Handles moving an S3 folder (and its contents) to the S3 trash.
        This involves listing objects, and queueing copy+delete operations.
        """
        s3_client = self.profile_manager.get_s3_client()
        if not s3_client:
            QMessageBox.warning(self, "S3 Error", "S3 client not available for folder move to trash.")
            return

        print(f"S3Explorer: Preparing to move folder '{source_folder_prefix}' to trash '{trash_dest_folder_prefix}' in bucket '{s3_bucket}'.")
        
        operations_to_queue = []
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            # Ensure source_folder_prefix ends with a slash for correct listing
            list_prefix = source_folder_prefix if source_folder_prefix.endswith('/') else source_folder_prefix + '/'
            trash_dest_base = trash_dest_folder_prefix if trash_dest_folder_prefix.endswith('/') else trash_dest_folder_prefix + '/'

            # 1. Add operation to create the main destination folder marker in trash (optional but good practice)
            #    If the source folder was empty, this ensures the trash folder exists.
            #    If it had contents, copy operations for files within it will implicitly create parent "folders".
            # op_create_trash_folder_marker = S3Operation(S3OpType.CREATE_FOLDER, s3_bucket, key=trash_dest_base,
            #                                             callback_data={'ui_source': 'move_to_trash_create_target_folder'})
            # operations_to_queue.append(op_create_trash_folder_marker)


            # 2. List and prepare move operations for all objects within the source folder
            for page in paginator.paginate(Bucket=s3_bucket, Prefix=list_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        source_obj_key = obj['Key']
                        # Calculate relative path from the source folder's root
                        relative_path_in_folder = source_obj_key[len(list_prefix):]
                        dest_obj_key_in_trash = trash_dest_base + relative_path_in_folder

                        print(f"  - Moving object: {source_obj_key} -> {dest_obj_key_in_trash}")
                        move_op = S3Operation(
                            S3OpType.COPY_OBJECT,
                            bucket=s3_bucket, # Destination bucket for copy
                            key=source_obj_key, # Source object key
                            new_key=dest_obj_key_in_trash, # Destination key in trash
                            is_part_of_move=True, # Worker will delete original source_obj_key
                            original_source_key_for_move=source_obj_key,
                            callback_data={'ui_source': 'mount_sync_move_item_to_trash'}
                        )
                        operations_to_queue.append(move_op)
                
                # Handle CommonPrefixes (subfolders) if you want to explicitly create empty subfolder markers in trash.
                # Usually, copying files into paths like "trash/folder/subfolder/file.txt" implicitly creates the
                # "folder" structure in S3. Explicit CREATE_FOLDER for subfolders is mostly for empty ones.
                if 'CommonPrefixes' in page:
                    for common_prefix in page.get('CommonPrefixes', []):
                        source_subfolder_key = common_prefix.get('Prefix')
                        relative_subfolder_path = source_subfolder_key[len(list_prefix):]
                        dest_subfolder_key_in_trash = trash_dest_base + relative_subfolder_path
                        print(f"  - Ensuring trash subfolder marker for: {dest_subfolder_key_in_trash}")
                        op_create_trash_subfolder = S3Operation(S3OpType.CREATE_FOLDER, s3_bucket, key=dest_subfolder_key_in_trash,
                                                               callback_data={'ui_source': 'move_to_trash_create_subfolder'})
                        operations_to_queue.append(op_create_trash_subfolder)


            if not operations_to_queue and not any(page.get('Contents') or page.get('CommonPrefixes') for page in paginator.paginate(Bucket=s3_bucket, Prefix=list_prefix)):
                 # If the folder was completely empty, just create the marker in trash and delete original marker
                 print(f"  - Source folder '{list_prefix}' appears empty. Creating marker in trash and deleting original marker.")
                 op_create_trash_folder_marker = S3Operation(S3OpType.CREATE_FOLDER, s3_bucket, key=trash_dest_base,
                                                            callback_data={'ui_source': 'move_to_trash_create_target_empty_folder'})
                 operations_to_queue.append(op_create_trash_folder_marker)
                 # The original empty folder still needs to be "deleted" (its marker removed)
                 # The worker for DELETE_FOLDER is recursive, but for an empty marker it's simple.
                 # It might be better to just delete the original *folder marker* after all contents (if any) are processed.
                 # For now, we rely on the subsequent DELETE_FOLDER for the original source_folder_prefix if needed.
                 # Let's add a DELETE_FOLDER for the original source folder prefix.
                 # This is important because S3OpType.COPY_OBJECT with is_part_of_move=True only deletes the individual objects.
                 op_delete_original_folder_marker = S3Operation(S3OpType.DELETE_FOLDER, s3_bucket, key=list_prefix,
                                                               callback_data={'ui_source': 'move_to_trash_delete_original_empty_folder_marker'})
                 operations_to_queue.append(op_delete_original_folder_marker)


        except Exception as e:
            QMessageBox.critical(self, "Move to Trash Error", f"Error preparing folder move to trash for '{source_folder_prefix}': {e}")
            return

        if operations_to_queue:
            batch_id = f"move_folder_to_trash_{time.time()}"
            self.operation_manager.start_batch_operation(
                batch_id=batch_id,
                total_items=len(operations_to_queue),
                op_type_display=f"Moving folder '{os.path.basename(source_folder_prefix.strip('/'))}' to Trash",
                operations_to_queue=operations_to_queue,
                extra_batch_data={
                    # 'target_tab_ref': self.get_active_tab_content(), # Optional, if refresh needed on this tab
                    'target_bucket': s3_bucket, # For potential refresh on completion
                    'source_prefix_moved': source_folder_prefix, # For logging or final cleanup
                    'destination_prefix_moved': trash_dest_folder_prefix # To refresh trash view
                }
            )
        else:
            # This case (empty folder) should ideally be handled above by creating the trash marker and deleting original.
            # If it still reaches here with no ops, it means something was unexpected.
            print(f"S3Explorer: No operations queued for moving folder '{source_folder_prefix}' to trash. It might have been empty and already processed, or an error occurred.")
            # Potentially refresh the view if the folder was expected to be non-empty
            self.refresh_views_for_bucket_path(s3_bucket, os.path.dirname(source_folder_prefix.strip('/')))


    # You will also need to modify the "Delete" action from S3TabContentWidget's context menu
    # and any global "Delete" action to use this "move to trash" logic.
    # For example, S3Explorer.request_delete_s3_item would need to be changed.

    def request_open_s3_file(self, s3_key: str, original_filename: str, bucket_name: str, tab_ref: S3TabContentWidget):
        # --- Start: Acquire lock for this S3 key to prevent concurrent open attempts ---
        with self._opening_s3_file_lock:
            if self._opening_s3_file_key == s3_key:
                print(f"S3Explorer: Open request for '{s3_key}' is already in progress or was handled very recently. Ignoring duplicate call.")
                return # Exit if an open operation for this exact S3 key is already underway
            # If not, mark this s3_key as being processed for opening.
            # This lock will be released by on_op_mgr_download_to_temp_finished or on early error.
            self._opening_s3_file_key = s3_key
            print(f"S3Explorer: Acquired 'open file' lock for S3 key: '{s3_key}'")
        # --- End: Acquire lock ---

        try:
            # --- Start: Existing Pre-checks ---
            if not self.profile_manager.get_s3_client():
                QMessageBox.warning(self, "S3 Error", "S3 client not connected. Cannot open file.")
                self._clear_opening_file_lock(s3_key) # Release lock on early exit
                return
            
            if not self.live_edit_temp_dir or not os.path.exists(self.live_edit_temp_dir):
                print("S3Explorer: Live edit temp directory missing, attempting to recreate...")
                self.live_edit_temp_dir = get_s3_live_edit_temp_dir()
                if not self.live_edit_temp_dir or not os.path.exists(self.live_edit_temp_dir):
                    QMessageBox.critical(self, "Fatal Error", "Live edit temporary directory is not available and could not be created.")
                    self._clear_opening_file_lock(s3_key) # Release lock on early exit
                    return
            # --- End: Existing Pre-checks ---

            temp_file_path = os.path.join(self.live_edit_temp_dir, original_filename)
            norm_temp_file_path = os.path.normpath(temp_file_path)
            proceed_with_download = True

            if os.path.exists(norm_temp_file_path):
                print(f"S3Explorer: Local temp file '{norm_temp_file_path}' for S3 key '{s3_key}' already exists.")
                is_tracked_for_this_s3_key = False
                for tracked_s3_obj_key, data in self.temp_file_manager.opened_temp_files.items():
                    if os.path.normpath(data.get('temp_path')) == norm_temp_file_path and tracked_s3_obj_key == s3_key:
                        is_tracked_for_this_s3_key = True
                        break
                
                if is_tracked_for_this_s3_key:
                    reply = QMessageBox.question(self, 
                                                 "File Already Open or Tracked",
                                                 f"A local temporary version of '{original_filename}' (from s3://{bucket_name}/{s3_key}) "
                                                 f"already exists and appears to be tracked for editing:\n\n{norm_temp_file_path}\n\n"
                                                 "Re-downloading from S3 will overwrite this local temporary file. "
                                                 "This could discard unsaved changes if an editor is currently open with it.\n\n"
                                                 "Do you want to re-download the latest version from S3 and open it?",
                                                 QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                                 QMessageBox.StandardButton.No)
                    if reply == QMessageBox.StandardButton.No:
                        self.update_status_bar_message_slot(f"Open action for '{original_filename}' cancelled by user (file already exists/tracked).", 4000)
                        proceed_with_download = False
                    else:
                        print(f"S3Explorer: User opted to overwrite existing temp file '{norm_temp_file_path}'.")
                else:
                    print(f"S3Explorer: Existing temp file '{norm_temp_file_path}' is not tracked for this S3 key. Will overwrite.")
            
            if proceed_with_download:
                self.update_status_bar_message_slot(f"Preparing S3 download of '{s3_key}' to '{norm_temp_file_path}' for live editing...", 0)
                
                open_op = S3Operation(S3OpType.DOWNLOAD_TO_TEMP, bucket_name, key=s3_key, local_path=norm_temp_file_path,
                                      callback_data={
                                          'ui_source': 'live_edit_open', 
                                          'tab_widget_ref': tab_ref,
                                          'original_filename': original_filename,
                                          'intended_local_path': norm_temp_file_path,
                                          's3_key_for_lock_release': s3_key # Important for releasing the lock
                                      })
                self.operation_manager.enqueue_s3_operation(open_op)
                # The lock self._opening_s3_file_key will be cleared by on_op_mgr_download_to_temp_finished
                # when it receives the 's3_key_for_lock_release' from callback_data.
            else:
                # Download was cancelled, so release the lock now.
                self._clear_opening_file_lock(s3_key)
                pass # Status message for cancellation already shown

        except Exception as e_req_open: # Catch any unexpected errors during the synchronous part
            print(f"S3Explorer: Unexpected error in request_open_s3_file for '{s3_key}': {e_req_open}")
            self._clear_opening_file_lock(s3_key) # Ensure lock is cleared on any exception
            # Optionally, re-raise or show a message box to the user
            QMessageBox.critical(self, "Open File Error", f"An unexpected error occurred while trying to open '{original_filename}':\n{e_req_open}")
            # If you re-raise, the application might crash if not handled further up.
            # raise e_req_open

    def generate_shareable_s3_link(self, s3_key: str, bucket_name: str, expiration_seconds: int, item_name: str):
        s3_client = self.profile_manager.get_s3_client()
        generate_shareable_s3_link(s3_client, s3_key, bucket_name, expiration_seconds, item_name, self)

    def _clear_opening_file_lock(self, s3_key_that_was_opening):
        with self._opening_s3_file_lock:
            if self._opening_s3_file_key == s3_key_that_was_opening:
                print(f"S3Explorer: Releasing open lock for '{s3_key_that_was_opening}'")
                self._opening_s3_file_key = None
            # else: lock was for a different key or already cleared, do nothing.

    def request_download_s3_item(self, s3_key: str, name: str, is_folder: bool, bucket_name: str, tab_ref: S3TabContentWidget):
        if is_folder:
            QMessageBox.information(self, "Download Folder", "Recursive folder download not fully implemented. Please download files individually."); return
        if not self.profile_manager.get_s3_client(): QMessageBox.warning(self, "Error", "S3 client not connected."); return

        local_save_path, _ = QFileDialog.getSaveFileName(self, "Save File As", os.path.join(self.settings.value("last_download_dir", os.path.expanduser("~")), name))
        if local_save_path:
            self.settings.setValue("last_download_dir", os.path.dirname(local_save_path))
            download_op = S3Operation(S3OpType.DOWNLOAD_FILE, bucket_name, key=s3_key, local_path=local_save_path,
                                      callback_data={'tab_widget_ref': tab_ref})
            self.operation_manager.enqueue_s3_operation(download_op)

    def request_delete_s3_item(self, s3_key: str, name: str, is_folder: bool, bucket_name: str, tab_ref: S3TabContentWidget):
        # This method is called from S3TabContentWidget context menu or other UI delete actions
        if not self.profile_manager.get_s3_client():
            QMessageBox.warning(self, "S3 Error", "S3 client not connected.")
            return
        
        item_type_str = "folder" if is_folder else "file"
        # Ensure s3_key for folder has trailing slash for consistency if it doesn't already
        if is_folder and not s3_key.endswith('/'):
            s3_key += '/'

        # Construct the destination key in the trash
        original_key_relative_part = s3_key
        if original_key_relative_part.startswith('/'): # Should not happen if keys are relative to bucket
            original_key_relative_part = original_key_relative_part[1:]
        s3_trash_dest_key = S3_TRASH_PREFIX + original_key_relative_part

        message = (f"Are you sure you want to move the S3 {item_type_str} '{name}' to the S3 Trash?\n\n"
                   f"From: s3://{bucket_name}/{s3_key}\n"
                   f"To:   s3://{bucket_name}/{s3_trash_dest_key}")

        reply = QMessageBox.question(self, f"Move to S3 Trash: {name}",
                                     message,
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No) # Default to No
        
        if reply == QMessageBox.StandardButton.Yes:
            self.update_status_bar_message_slot(f"Queueing move of '{name}' to S3 Trash...", 0)
            if is_folder:
                self._move_s3_folder_to_trash_batch(bucket_name, s3_key, s3_trash_dest_key)
            else: # Single file
                move_op = S3Operation(
                    S3OpType.COPY_OBJECT,
                    bucket=bucket_name,
                    key=s3_key, # Source
                    new_key=s3_trash_dest_key, # Destination in trash
                    is_part_of_move=True,
                    original_source_key_for_move=s3_key,
                    callback_data={'ui_source': 'ui_move_to_trash_file', 'target_tab_ref': tab_ref}
                )
                self.operation_manager.enqueue_s3_operation(move_op)
        else:
            self.update_status_bar_message_slot(f"Move to S3 Trash for '{name}' cancelled.", 3000)


    # When on_batch_operation_complete_from_op_mgr is called for these "move to trash" batches,
    # it should refresh the source view (where the item disappeared from)
    # and potentially the trash view if you have one open.
    # Modify on_batch_operation_complete_from_op_mgr:
    # ...
    # if target_tab_ref and isinstance(target_tab_ref, S3TabContentWidget):
    #     # ... existing logic ...
    #     target_tab_ref.populate_s3_view_tab() # Refresh the tab where user initiated delete
    
    # Also, if the batch was 'move_folder_to_trash_...'
    # original_source_prefix_moved = batch_data.get('original_source_prefix_moved')
    # if original_source_prefix_moved:
    #    self.refresh_views_for_bucket_path(batch_data.get('target_bucket'), os.path.dirname(original_source_prefix_moved.strip('/')))

    def request_create_s3_folder(self, bucket_name: str, current_path_in_bucket: str, tab_ref: S3TabContentWidget):
        if not self.profile_manager.get_s3_client(): QMessageBox.warning(self, "Error", "S3 client not connected."); return
        folder_name_input, ok = QInputDialog.getText(self, "Create New S3 Folder", "Enter folder name:")
        if ok and folder_name_input:
            folder_name_clean = folder_name_input.strip().rstrip('/')
            if not folder_name_clean or '/' in folder_name_clean or '\\' in folder_name_clean:
                QMessageBox.warning(self, "Invalid Name", "Folder name invalid."); return
            
            new_folder_full_key = (current_path_in_bucket.strip('/') + '/' if current_path_in_bucket.strip('/') else '') + folder_name_clean + '/'
            create_op = S3Operation(S3OpType.CREATE_FOLDER, bucket_name, key=new_folder_full_key, callback_data={'tab_widget_ref': tab_ref})
            self.operation_manager.enqueue_s3_operation(create_op)
            self.update_status_bar_message_slot(f"Queueing create folder '{folder_name_clean}'...", 0)

    def show_properties_dialog_from_tab(self, s3_key: str, item_name: str, is_folder: bool, bucket_name: str, tab_ref: S3TabContentWidget):
        s3_client = self.profile_manager.get_s3_client()
        if not s3_client: QMessageBox.warning(self, "Properties Error", "S3 client not available."); return
        dialog = PropertiesDialog(s3_client, bucket_name, s3_key, is_folder, item_name, self)
        dialog.exec()

    def handle_save_active_file(self): # Triggered by Save Action
        active_tab = self.get_active_tab_content()
        if not active_tab: QMessageBox.information(self, "Save Error", "No active S3 tab."); return

        selected_indexes = active_tab.tree_view.selectionModel().selectedRows(COL_S3_KEY)
        if len(selected_indexes) == 1:
            row = selected_indexes[0].row()
            s3_key_item = active_tab.model.item(row, COL_S3_KEY)
            is_folder_item = active_tab.model.item(row, COL_IS_FOLDER)

            if s3_key_item and is_folder_item and is_folder_item.text() == "0":
                s3_key = s3_key_item.text()
                # TempFileManager handles prompting and queuing upload if needed
                self.check_modified_temp_files(force_check_s3_key=s3_key) 
                return
        QMessageBox.information(self, "Save", "Select a single, opened, modified S3 file to save.")


    def handle_save_local_as_s3_action(self):
        s3_client = self.profile_manager.get_s3_client()
        if not s3_client: QMessageBox.warning(self, "S3 Not Ready", "S3 client not initialized."); return
        
        active_tab = self.get_active_tab_content()
        active_profile_data = self.profile_manager.get_active_profile_data()
        default_s3_bucket = (active_tab.current_bucket if active_tab else "") or \
                            (active_profile_data.get("default_s3_bucket","") if active_profile_data else "")
        default_s3_path = active_tab.current_path if active_tab else ""

        local_file_path, _ = QFileDialog.getOpenFileName(self, "Select Local File to Upload", self.settings.value("last_upload_source_dir", os.path.expanduser("~")))
        if not local_file_path: return
        self.settings.setValue("last_upload_source_dir", os.path.dirname(local_file_path))

        initial_filename = os.path.basename(local_file_path)
        suggested_s3_key = (default_s3_path.strip('/') + '/' if default_s3_path.strip('/') else '') + initial_filename
        suggested_s3_full_path = f"s3://{default_s3_bucket or '<bucket>'}/{suggested_s3_key.lstrip('/')}"
        
        s3_target_path_input, ok = QInputDialog.getText(self, "Save As to S3", 
                                                  f"Target S3 path for {initial_filename}:",
                                                  QLineEdit.EchoMode.Normal, suggested_s3_full_path)
        if ok and s3_target_path_input:
            s3_target_path_input = s3_target_path_input.strip()
            if s3_target_path_input.startswith("s3://"):
                path_part = s3_target_path_input[5:]
                target_bucket, _, target_key = path_part.partition('/')
                if target_bucket and target_key and not target_key.endswith('/'):
                    upload_op = S3Operation(S3OpType.UPLOAD_FILE, target_bucket, key=target_key, local_path=local_file_path)
                    self.operation_manager.enqueue_s3_operation(upload_op)
                else: QMessageBox.warning(self, "Invalid S3 Path", "S3 path: valid bucket and file key required.")
            else: QMessageBox.warning(self, "Invalid S3 Path", "S3 path must start with s3://")


    # --- Mounted Path Methods (delegated to MountManager) ---
    def show_mount_config_dialog(self):
        current_configs = self.mount_manager.get_persistent_mount_configs()
        active_profile_data = self.profile_manager.get_active_profile_data()
        active_default_bucket = None
        if active_profile_data:
            active_default_bucket = active_profile_data.get("default_s3_bucket", "").strip()
            if not active_default_bucket: # Ensure empty string becomes None for clarity
                active_default_bucket = None
                
        dialog = MountConfigDialog(self, 
                                   existing_mounts=current_configs, 
                                   active_profile_default_bucket=active_default_bucket)
        if dialog.exec():
            new_mount_configs = dialog.get_configured_mounts()
            self.mount_manager.update_mounted_paths(new_mount_configs)

    # --- Refreshing Views ---
    def refresh_views_for_bucket_path(self, bucket_name: str, path_in_bucket: str):
        if not self.tab_widget: return
        normalized_path = path_in_bucket.strip('/')
        for i in range(self.tab_widget.count()):
            tab = self.tab_widget.widget(i)
            if isinstance(tab, S3TabContentWidget) and tab.current_bucket == bucket_name and \
               tab.current_path.strip('/') == normalized_path:
                tab.populate_s3_view_tab()
    
    def refresh_views_for_bucket(self, bucket_name: str):
        if not self.tab_widget: return
        for i in range(self.tab_widget.count()):
            tab = self.tab_widget.widget(i)
            if isinstance(tab, S3TabContentWidget) and tab.current_bucket == bucket_name:
                tab.populate_s3_view_tab()


    # --- Temp File Management (delegated to TempFileManager) ---
    def check_modified_temp_files(self, force_check_s3_key=None):
        print(f"\nS3EXPLORER: CHECK_MODIFIED_TEMP_FILES - Force Key: {force_check_s3_key}")
        s3_client = self.profile_manager.get_s3_client()
        if not s3_client: print("  No S3 client, exiting check."); return
        
        keys_to_check = [force_check_s3_key] if force_check_s3_key else self.temp_file_manager.get_all_tracked_files()
        
        for s3_key in keys_to_check:
            file_data = self.temp_file_manager.get_temp_file_data(s3_key)
            if not file_data: continue

            is_locally_modified, s3_has_newer, s3_mtime, local_mtime = \
                self.temp_file_manager.check_single_temp_file_modified_status(s3_key, s3_client)

            if is_locally_modified:
                temp_path = file_data['temp_path']
                s3_bucket = file_data['s3_bucket']
                prompt_upload = False

                if s3_has_newer:
                    reply = QMessageBox.question(self, "Conflict Detected",
                                                f"File '{os.path.basename(s3_key)}' modified locally AND newer on S3.\n"
                                                f"S3: {s3_mtime.strftime('%Y-%m-%d %H:%M:%S %Z') if s3_mtime else 'N/A'}\n"
                                                f"Local: {datetime.fromtimestamp(local_mtime).strftime('%Y-%m-%d %H:%M:%S') if local_mtime else 'N/A'}\n\n"
                                                "Overwrite S3 version with local changes?",
                                                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
                    if reply == QMessageBox.StandardButton.Yes: prompt_upload = True
                    else: # User chose not to overwrite, update mtimes to "forget" local mod for this check cycle
                          self.temp_file_manager.handle_temp_file_upload_success(s3_key,s3_bucket, temp_path, s3_client) # Resets mtimes as if saved
                else: # No conflict, just locally modified
                    reply = QMessageBox.question(self, "Save Modified File?",
                                                f"File '{os.path.basename(s3_key)}' (from s3://{s3_bucket}/{s3_key}) modified locally. Upload changes to S3?",
                                                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.Yes)
                    if reply == QMessageBox.StandardButton.Yes: prompt_upload = True
                    else: # User chose not to save, update mtimes
                         self.temp_file_manager.handle_temp_file_upload_success(s3_key,s3_bucket, temp_path, s3_client) # Resets mtimes as if saved


                if prompt_upload:
                    print(f"  Queueing upload for modified file: '{s3_key}' from '{temp_path}' to bucket '{s3_bucket}'")
                    upload_op = S3Operation(S3OpType.UPLOAD_FILE, s3_bucket, key=s3_key, local_path=temp_path,
                                            callback_data={"is_temp_file_update": True})
                    self.operation_manager.enqueue_s3_operation(upload_op)
        self.update_save_action_state()


    # --- Favorites Management (delegated to FavoritesManager) ---
    @pyqtSlot() 
    def rebuild_favorites_menu(self, favorites_list_from_signal=None):
        if not hasattr(self, 'favorites_menu_ref'): return
        if not hasattr(self, 'add_fav_action_fixed') or self.add_fav_action_fixed is None: 
            print("S3Explorer: WARNING - add_fav_action_fixed not found or not initialized. Favorites menu might not build correctly.")
            return

        all_current_favorites = favorites_list_from_signal
        if all_current_favorites is None: # Called due to profile change, not favorite data change
            all_current_favorites = self.favorites_manager.get_favorites()

        # --- Filter favorites based on active profile's default_s3_bucket ---
        active_profile_data = self.profile_manager.get_active_profile_data()
        current_default_bucket = None
        if active_profile_data:
            current_default_bucket = active_profile_data.get("default_s3_bucket", "").strip()
        
        display_favorites_list = []
        if current_default_bucket: # Only show favorites if a default bucket is set
            for fav in all_current_favorites:
                if fav.get('bucket') == current_default_bucket:
                    display_favorites_list.append(fav)
        
        # --- Reconstruct the menu: clear old dynamic items, keep fixed ones ---
        current_actions_in_menu = list(self.favorites_menu_ref.actions()) 

        for action in current_actions_in_menu:
            if action != self.add_fav_action_fixed: # Check against the stored QAction object
                self.favorites_menu_ref.removeAction(action)
        
        # Menu now only has self.add_fav_action_fixed
        
        show_separator = False
        if display_favorites_list:
            show_separator = True
        elif current_default_bucket and not display_favorites_list: # Placeholder for "No favs for bucket X"
            show_separator = True
        elif not current_default_bucket: # Placeholder for "Set default bucket..."
            show_separator = True
            
        if show_separator:
            self.favorites_menu_ref.addSeparator()

        if display_favorites_list:
            for fav_item in display_favorites_list:
                action_text = fav_item.get('name', f"s3://{fav_item['bucket']}/{fav_item.get('prefix','').strip('/')}")
                fav_action = QAction(action_text, self)
                fav_action.triggered.connect(
                    lambda checked=False, b=fav_item['bucket'], p=fav_item.get('prefix',''): self.open_favorite_in_new_tab(b, p)
                )
                self.favorites_menu_ref.addAction(fav_action)
        elif current_default_bucket and not display_favorites_list: 
            placeholder_action = QAction(f"No favorites for bucket '{current_default_bucket}'", self)
            placeholder_action.setEnabled(False)
            self.favorites_menu_ref.addAction(placeholder_action)
        elif not current_default_bucket and self.profile_manager.get_s3_client(): 
            placeholder_action = QAction("Set default bucket in active profile for favorites", self)
            placeholder_action.setEnabled(False)
            self.favorites_menu_ref.addAction(placeholder_action)
        elif not self.profile_manager.get_s3_client():
            placeholder_action = QAction("Connect to S3 to see favorites", self)
            placeholder_action.setEnabled(False)
            self.favorites_menu_ref.addAction(placeholder_action)

    def open_favorite_in_new_tab(self, bucket: str, prefix: str):
        self.add_new_s3_tab(bucket_to_open=bucket, path_to_open=prefix)

    def add_current_path_to_favorites(self):
        active_tab = self.get_active_tab_content()
        if not active_tab or not active_tab.current_bucket:
            QMessageBox.warning(self, "Add Favorite Error", "No active S3 path to add."); return

        current_bucket = active_tab.current_bucket
        current_prefix = active_tab.current_path.strip('/')
        default_fav_name = os.path.basename(current_prefix) if current_prefix else current_bucket
        
        fav_name_input, ok = QInputDialog.getText(self, "Add to Favorites", "Name for S3 favorite:", text=default_fav_name)
        if ok and fav_name_input:
            fav_name_clean = fav_name_input.strip()
            if not fav_name_clean: QMessageBox.warning(self, "Invalid Name", "Favorite name empty."); return

            success, message = self.favorites_manager.add_favorite(fav_name_clean, current_bucket, current_prefix)
            if success: self.update_status_bar_message_slot(message, 3000)
            else: QMessageBox.warning(self, "Add Favorite Error", message)

    # --- Watchdog for Live Edit Temp Files ---
    def start_live_edit_file_watcher(self):
        if not self.live_edit_temp_dir or not os.path.exists(self.live_edit_temp_dir):
            print("S3Explorer: Live edit temp directory not available, cannot start watcher.")
            return
        if self.live_edit_file_watcher and self.live_edit_file_watcher.is_alive():
            print("S3Explorer: Live edit file watcher already running.")
            return

        self.live_edit_file_handler = LiveEditFileChangeHandler(self)
        self.live_edit_file_watcher = WatchdogObserver()
        try:
            self.live_edit_file_watcher.schedule(self.live_edit_file_handler, self.live_edit_temp_dir, recursive=False)
            self.live_edit_file_watcher.start()
            print(f"S3Explorer: Live edit file watcher started on '{self.live_edit_temp_dir}'.")
        except Exception as e:
            QMessageBox.critical(self, "Watcher Error", f"Could not start live edit file watcher:\n{e}")
            self.live_edit_file_watcher = None # Clear if failed
    
    def stop_live_edit_file_watcher(self):
        if self.live_edit_file_watcher and self.live_edit_file_watcher.is_alive():
            print("S3Explorer: Stopping live edit file watcher...")
            try:
                self.live_edit_file_watcher.stop()
                self.live_edit_file_watcher.join(timeout=2)
                if self.live_edit_file_watcher.is_alive():
                    print("S3Explorer: Warning - Live edit file watcher did not stop gracefully.")
            except Exception as e:
                print(f"S3Explorer: Error stopping live edit watcher: {e}")
            finally:
                self.live_edit_file_watcher = None
        # Clear debounce timers on stop
        for timer in self._live_edit_debounce_timers.values():
            timer.cancel()
        self._live_edit_debounce_timers.clear()

    def _handle_live_edit_upload(self, local_path):
        norm_local_path = os.path.normpath(local_path)
        print(f"S3Explorer: Debounced: _handle_live_edit_upload for '{norm_local_path}'")

        # Clear the timer from the dict as it has fired
        if norm_local_path in self._live_edit_debounce_timers:
            # No need to cancel, it has already fired to call this method
            del self._live_edit_debounce_timers[norm_local_path]

        if not os.path.exists(norm_local_path): # <<<< RE-CHECK EXISTENCE
            print(f"S3Explorer: File '{norm_local_path}' no longer exists. Skipping auto-sync upload.")
            # Optionally, untrack it from TempFileManager if it's consistently disappearing
            # self.temp_file_manager.cleanup_temp_file_by_local_path(norm_local_path) # (Needs new method in TempFileManager)
            return

        file_info = None
        s3_key_original_tracking = None # The S3 key used when the file was initially tracked
        for s3k, data in self.temp_file_manager.opened_temp_files.items():
            if os.path.normpath(data.get('temp_path')) == norm_local_path:
                file_info = data
                s3_key_original_tracking = s3k
                break
        
        if file_info and s3_key_original_tracking:
            s3_bucket = file_info['s3_bucket']
            
            # Set an ignore flag *before* queueing upload to prevent the upload causing another watchdog event
            file_info["ignore_watchdog_until_sync"] = time.time() + 5.0 # Ignore for 5s during/after upload
            print(f"S3Explorer: Set watchdog ignore for '{norm_local_path}' for 5s before queuing upload.")


            print(f"S3Explorer: Queuing auto-sync upload for {norm_local_path} to s3://{s3_bucket}/{s3_key_original_tracking}")
            self.update_status_bar_message_slot(f"Auto-syncing {os.path.basename(norm_local_path)} to S3...", 0)
            
            upload_op = S3Operation(S3OpType.UPLOAD_FILE, s3_bucket, key=s3_key_original_tracking, local_path=norm_local_path,
                                    callback_data={'is_live_edit_sync': True, 
                                                'original_local_path': norm_local_path,
                                                's3_key_for_tracking': s3_key_original_tracking}) # Pass the tracking key
            self.operation_manager.enqueue_s3_operation(upload_op)
        else:
            print(f"S3Explorer: Live edit upload triggered for untracked or missing file info: {norm_local_path}")

    # --- Application State Persistence ---
    def load_settings(self):
        self.restoreGeometry(self.settings.value("geometry", QByteArray()))
        self.restoreState(self.settings.value("windowState", QByteArray()))
        
    def save_settings(self):
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())

    def closeEvent(self, event):
        self.modified_check_timer.stop()
        self.check_modified_temp_files() # Final chance to save

        if self.operation_manager.get_active_batch_operations_status() or self.operation_manager.get_queue_status():
             reply = QMessageBox.question(self, "Operations Pending",
                                         "S3 operations pending. Quit anyway?",
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
             if reply == QMessageBox.StandardButton.No:
                 event.ignore()
                 if self.profile_manager.get_s3_client(): self.modified_check_timer.start() # Restart timer
                 return
        
        try:
            stop_webdav()
            self.status_bar.showMessage("WebDAV server stopped", 3000)
            self.start_webdav_action.setEnabled(True)
            self.stop_webdav_action.setEnabled(False)
        except Exception as e:
            print(f"Error stopping WebDAV server on exit: {e}")

        self.mount_manager.stop_watchdog_observers(clear_runtime_objects=True)
        self.operation_manager.stop_all_s3_workers() # Stop S3 workers
        self.temp_file_manager.cleanup_all_temp_files() # Clean up temp files

        self.save_settings()
        self.profile_manager.save_aws_profiles()
        self.mount_manager.save_mounts_config()
        self.favorites_manager.save_favorites()
        self.stop_live_edit_file_watcher() # Ensure this is called
        cleanup_s3_live_edit_temp_dir() # Explicit call, though atexit should also run
        super().closeEvent(event)

    def request_restore_from_trash(self, s3_key_in_trash: str, name: str, is_folder: bool, s3_bucket: str, tab_ref: S3TabContentWidget):
        if not self.profile_manager.get_s3_client():
            QMessageBox.warning(self, "S3 Error", "S3 client not connected.")
            return

        # Determine original key by removing the S3_TRASH_PREFIX
        # s3_key_in_trash is like "_S3_EXPLORER_TRASH_/path/to/item"
        if not s3_key_in_trash.startswith(self.S3_TRASH_PREFIX):
            QMessageBox.critical(self, "Restore Error", f"Item '{name}' is not in a valid trash path: {s3_key_in_trash}")
            return
        
        original_s3_key = s3_key_in_trash[len(self.S3_TRASH_PREFIX):]
        if not original_s3_key: # Should not happen if path was valid
            QMessageBox.critical(self, "Restore Error", f"Could not determine original path for '{name}'.")
            return

        item_type_str = "folder" if is_folder else "file"
        message = (f"Are you sure you want to restore the S3 {item_type_str} '{name}'?\n\n"
                   f"From Trash: s3://{s3_bucket}/{s3_key_in_trash}\n"
                   f"To Original: s3://{s3_bucket}/{original_s3_key}")
        
        reply = QMessageBox.question(self, f"Restore {item_type_str} from Trash", message,
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.Yes)
        if reply != QMessageBox.StandardButton.Yes:
            return

        self.update_status_bar_message_slot(f"Restoring '{name}' from S3 Trash...", 0)

        if is_folder:
            # Similar to _move_s3_folder_to_trash_batch, but source and dest are swapped
            # And this is a direct move (copy + delete from trash)
            self._move_s3_folder_batch(s3_bucket, s3_key_in_trash, original_s3_key, 
                                       op_display_name=f"Restoring folder '{name}'")
        else: # Single file
            restore_op = S3Operation(
                S3OpType.COPY_OBJECT,
                bucket=s3_bucket,
                key=s3_key_in_trash, # Source (from trash)
                new_key=original_s3_key, # Destination (original location)
                is_part_of_move=True, # Delete from trash after successful copy
                original_source_key_for_move=s3_key_in_trash,
                callback_data={'ui_source': 'trash_restore_file', 'target_tab_ref': tab_ref}
            )
            self.operation_manager.enqueue_s3_operation(restore_op)

    def request_permanent_delete_from_trash(self, s3_key_in_trash: str, name: str, is_folder: bool, s3_bucket: str, tab_ref: S3TabContentWidget):
        if not self.profile_manager.get_s3_client():
            QMessageBox.warning(self, "S3 Error", "S3 client not connected.")
            return
        
        # Safety check: ensure we are indeed deleting from the trash prefix
        if not s3_key_in_trash.startswith(self.S3_TRASH_PREFIX):
            QMessageBox.critical(self, "Permanent Delete Error", 
                                 f"Item '{name}' is not in the S3 Trash path. Permanent delete aborted for safety.\nPath: {s3_key_in_trash}")
            return

        item_type_str = "folder" if is_folder else "file"
        message = (f"Are you sure you want to PERMANENTLY DELETE the S3 {item_type_str} '{name}' from the Trash?\n\n"
                   f"S3 Item: s3://{s3_bucket}/{s3_key_in_trash}\n\n"
                   "This operation CANNOT be undone.")
        
        reply = QMessageBox.warning(self, f"Confirm Permanent Delete: {name}", message, # Use warning icon
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No) # Default to No for safety
        if reply != QMessageBox.StandardButton.Yes:
            return

        self.update_status_bar_message_slot(f"Permanently deleting '{name}' from S3 Trash...", 0)
        op_type_s3 = S3OpType.DELETE_FOLDER if is_folder else S3OpType.DELETE_OBJECT
        
        delete_op = S3Operation(op_type_s3, s3_bucket, key=s3_key_in_trash,
                                callback_data={'ui_source': 'trash_permanent_delete', 'target_tab_ref': tab_ref})
        self.operation_manager.enqueue_s3_operation(delete_op)

    def _move_s3_folder_batch(self, s3_bucket: str, source_folder_prefix: str, dest_folder_prefix: str, op_display_name: str):
        """
        Generic helper to move an S3 folder (and its contents) from one prefix to another.
        Used for "move to trash" and "restore from trash".
        """
        s3_client = self.profile_manager.get_s3_client()
        if not s3_client:
            QMessageBox.warning(self, "S3 Error", f"S3 client not available for {op_display_name}.")
            return

        print(f"S3Explorer: Batch move folder from '{source_folder_prefix}' to '{dest_folder_prefix}' in bucket '{s3_bucket}'.")
        
        operations_to_queue = []
        source_list_prefix = source_folder_prefix if source_folder_prefix.endswith('/') else source_folder_prefix + '/'
        dest_base_prefix = dest_folder_prefix if dest_folder_prefix.endswith('/') else dest_folder_prefix + '/'
        
        active_tab_for_refresh = self.get_active_tab_content() # For refresh after completion

        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            
            # Handle contents
            for page in paginator.paginate(Bucket=s3_bucket, Prefix=source_list_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        source_obj_key = obj['Key']
                        relative_path_in_folder = source_obj_key[len(source_list_prefix):]
                        dest_obj_key = dest_base_prefix + relative_path_in_folder
                        move_op = S3Operation(S3OpType.COPY_OBJECT, bucket=s3_bucket, key=source_obj_key,
                                              new_key=dest_obj_key, is_part_of_move=True,
                                              original_source_key_for_move=source_obj_key,
                                              callback_data={'ui_source': op_display_name.lower().replace(" ", "_") + "_item"})
                        operations_to_queue.append(move_op)
                
                # Handle subfolder markers explicitly to ensure empty subfolders are "moved"
                if 'CommonPrefixes' in page:
                    for common_prefix in page.get('CommonPrefixes', []):
                        source_subfolder_key = common_prefix.get('Prefix')
                        relative_subfolder_path = source_subfolder_key[len(source_list_prefix):]
                        dest_subfolder_key = dest_base_prefix + relative_subfolder_path
                        op_create_dest_subfolder = S3Operation(S3OpType.CREATE_FOLDER, s3_bucket, key=dest_subfolder_key,
                                                              callback_data={'ui_source': op_display_name.lower().replace(" ", "_") + "_create_subfolder"})
                        operations_to_queue.append(op_create_dest_subfolder)
            
            # After all contents are moved (copied + original objects deleted by worker),
            # the original source_folder_prefix might still have empty subfolder markers
            # or its own marker if it was empty to begin with.
            # A final DELETE_FOLDER on the original source_folder_prefix will clean these up.
            # This is important because S3OpType.COPY_OBJECT with is_part_of_move=True only deletes individual files.
            if not operations_to_queue and not any(page.get('Contents') or page.get('CommonPrefixes') for page in paginator.paginate(Bucket=s3_bucket, Prefix=source_list_prefix)):
                 # If the source folder was completely empty, still create marker at destination and delete source marker
                 print(f"  - Source folder '{source_list_prefix}' is empty. Creating marker at '{dest_base_prefix}' and deleting original marker.")
                 op_create_dest_marker = S3Operation(S3OpType.CREATE_FOLDER, s3_bucket, key=dest_base_prefix,
                                                     callback_data={'ui_source': op_display_name.lower().replace(" ", "_") + "_create_empty_folder_marker"})
                 operations_to_queue.append(op_create_dest_marker)

            op_delete_original_folder_structure = S3Operation(S3OpType.DELETE_FOLDER, s3_bucket, key=source_list_prefix,
                                                              callback_data={'ui_source': op_display_name.lower().replace(" ", "_") + "_delete_original_folder_structure"})
            operations_to_queue.append(op_delete_original_folder_structure)

        except Exception as e:
            QMessageBox.critical(self, "Folder Operation Error", f"Error preparing {op_display_name} for '{source_folder_prefix}': {e}")
            return

        if operations_to_queue:
            batch_id = f"{op_display_name.lower().replace(' ', '_')}_{time.time()}"
            self.operation_manager.start_batch_operation(
                batch_id=batch_id,
                total_items=len(operations_to_queue),
                op_type_display=op_display_name,
                operations_to_queue=operations_to_queue,
                extra_batch_data={
                    'target_tab_ref': active_tab_for_refresh, 
                    'target_bucket': s3_bucket, 
                    'source_prefix_moved': source_folder_prefix, # For refreshing original location
                    'destination_prefix_moved': dest_folder_prefix # For refreshing destination (e.g., trash)
                }
            )
        else:
            print(f"S3Explorer: No operations queued for {op_display_name} on '{source_folder_prefix}'. It might have been empty or an error occurred.")
            # Refresh relevant views if needed
            self.refresh_views_for_bucket_path(s3_bucket, os.path.dirname(source_folder_prefix.strip('/')))
            if dest_folder_prefix != source_folder_prefix: # If it was a move, refresh dest too
                self.refresh_views_for_bucket_path(s3_bucket, os.path.dirname(dest_folder_prefix.strip('/')))
    def request_download_folder_as_zip(self, s3_key: str, name: str, bucket_name: str, tab_ref):
        if not self.profile_manager.get_s3_client():
            QMessageBox.warning(self, "Error", "S3 client not connected.")
            return

        s3_client = self.profile_manager.get_s3_client()

        temp_download_dir = tempfile.mkdtemp(prefix="s3_zip_dl_")
        folder_name = os.path.basename(s3_key.rstrip('/')) or "folder"
        local_folder_path = os.path.join(temp_download_dir, folder_name)
        os.makedirs(local_folder_path, exist_ok=True)

    # Step 1: Show download progress
        self.download_progress = QProgressDialog("Preparing to download...", "Cancel", 0, 0, self)
        self.download_progress.setWindowTitle("Downloading Folder from S3")
        self.download_progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        self.download_progress.setMinimumDuration(0)
        self.download_progress.setFixedSize(420, 160)
        self.download_progress.setStyleSheet("""
            QProgressDialog {
            background-color: #ffffff;
            border: 1px solid #d0d0d0;
            padding: 16px;
            font-family: 'Segoe UI', sans-serif;
            font-size: 10.5pt;
            color: #2e2e2e;
            }

            QLabel {
                color: #333;
                font-size: 10pt;
                padding: 6px;
                qproperty-alignment: 'AlignLeft | AlignVCenter';
            }

            QProgressBar {
                border: 1px solid #cccccc;
                border-radius: 8px;
                background-color: #f2f2f2;
                height: 18px;
                font-size: 9pt;
                text-align: center;
            }

            QProgressBar::chunk {
                background-color: qlineargradient(
                spread:pad, x1:0, y1:0, x2:1, y2:0,
                stop:0 #6dd5ed, stop:1 #2193b0
            );
            border-radius: 6px;
        }

            QPushButton {
                background-color: #e0e0e0;
                border: 1px solid #bbbbbb;
                border-radius: 6px;
                padding: 6px 16px;
                font-size: 9.5pt;
                color: #333;
            }

            QPushButton:hover {
                background-color: #d5d5d5;
            }

            QPushButton:pressed {
                background-color: #c0c0c0;
            }
    """)


        self.download_progress.show()

        self.download_worker = DownloadFolderWorker(s3_client, bucket_name, s3_key, local_folder_path)

        def update_download_progress(current, total, key):
            elapsed = time.time() - self.download_start_time
            processed = current
            avg_time = elapsed / processed if processed else 0
            eta = avg_time * (total - processed)
            eta_str = time.strftime('%M:%S', time.gmtime(eta))

            self.download_progress.setMaximum(total)
            self.download_progress.setValue(current)
            self.download_progress.setLabelText(
                f"Remaining files: {total-current} of {total}\n"
                f"Current File: {os.path.basename(key)}\n"
                f"Estimated time remaining: {eta_str}"
            )
            QApplication.processEvents()

        def on_download_finished(local_path):
            self.download_progress.close()
            self.start_zip_worker(local_path, folder_name)

        def on_download_error(message):
            self.download_progress.close()
            QMessageBox.critical(self, "Download Error", f"Failed to download folder:\n{message}")

        def on_download_canceled():
            self.download_progress.close()
            QMessageBox.information(self, "Cancelled", "Download was cancelled.")

        self.download_worker.progress_updated.connect(update_download_progress)
        self.download_worker.finished.connect(on_download_finished)
        self.download_worker.error.connect(on_download_error)
        self.download_worker.canceled.connect(on_download_canceled)
        self.download_progress.canceled.connect(self.download_worker.cancel)
        self.download_start_time = time.time()
        self.download_worker.start()

    def start_zip_worker(self, local_folder_path, folder_name):
            zip_path = os.path.join(tempfile.gettempdir(), f"{folder_name}.zip")

            self.zip_progress = QProgressDialog("Zipping folder...", "Cancel", 0, 100, self)
            self.zip_progress.setWindowTitle("Creating ZIP")
            self.zip_progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            self.zip_progress.setMinimumDuration(0)
            self.zip_progress.setStyleSheet("""
                QProgressDialog {
                    background-color: #ffffff;
                    border: 1px solid #d0d0d0;
                    border-radius: 12px;
                    padding: 16px;
                    font-family: 'Segoe UI', sans-serif;
                    font-size: 10.5pt;
                    color: #2e2e2e;
                }

                QLabel {
                    color: #333;
                    font-size: 10pt;
                    padding: 6px;
                    qproperty-alignment: 'AlignLeft | AlignVCenter';
                }

                QProgressBar {
                    border: 1px solid #cccccc;
                    border-radius: 8px;
                    background-color: #f2f2f2;
                    height: 18px;
                    font-size: 9pt;
                    text-align: center;
                }

                QProgressBar::chunk {
                    background-color: qlineargradient(
                    spread:pad, x1:0, y1:0, x2:1, y2:0,
                    stop:0 #6dd5ed, stop:1 #2193b0
                );
                    border-radius: 6px;
                }

                QPushButton {
                    background-color: #e0e0e0;
                    border: 1px solid #bbbbbb;
                    border-radius: 6px;
                    padding: 6px 16px;
                    font-size: 9.5pt;
                    color: #333;
                }

                QPushButton:hover {
                    background-color: #d5d5d5;
                }

                QPushButton:pressed {
                    background-color: #c0c0c0;
                }
            """)

            self.zip_progress.show()

            self.zip_worker = ZipFolderWorker(local_folder_path, zip_path)

            def update_zip_progress(current, total, percent, eta):
                self.zip_progress.setMaximum(total)
                self.zip_progress.setValue(current)
                self.zip_progress.setLabelText(
                    f"Zipping folder...\n{current}/{total} ({percent}%)\nEstimated time left: {eta}"
                )
                QApplication.processEvents()

            def on_zip_finished(path):
                self.zip_progress.close()
                save_path, _ = QFileDialog.getSaveFileName(
                    self, "Save ZIP File", f"{folder_name}.zip", "Zip Files (*.zip)"
                )
                if save_path:
                    shutil.move(path, save_path)
                    QMessageBox.information(self, "Download Complete", f"ZIP saved to:\n{save_path}")
                else:
                    os.remove(path)

            def on_zip_error(msg):
                self.zip_progress.close()
                QMessageBox.critical(self, "ZIP Error", f"Failed to zip:\n{msg}")

            def on_zip_canceled():
                self.zip_progress.close()
                QMessageBox.information(self, "Cancelled", "Zipping was cancelled.")

            self.zip_worker.progress_updated.connect(update_zip_progress)
            self.zip_worker.finished.connect(on_zip_finished)
            self.zip_worker.error.connect(on_zip_error)
            self.zip_worker.canceled.connect(on_zip_canceled)
            self.zip_progress.canceled.connect(self.zip_worker.cancel)

            self.zip_worker.start()
    # --- REPLACE ALL UPDATE METHODS in S3Explorer class WITH THIS BLOCK ---

    def check_for_updates_on_startup(self):
        """ Checks for updates in a background thread when the app starts. """
        print("Initiating startup update check...")
        update_thread = threading.Thread(target=self.check_for_updates, args=(False,))
        update_thread.daemon = True
        update_thread.start()

    def check_for_updates_in_background(self):
        """ Periodically checks for updates in the background. """
        print("Initiating background update check...")
        update_thread = threading.Thread(target=self.check_for_updates, args=(False,))
        update_thread.daemon = True
        update_thread.start()

    def check_for_updates(self, show_no_update_dialog=True):
        """
        The core update check logic. This runs in a background thread.

        Args:
            show_no_update_dialog (bool): If True, shows a "You're up to date" dialog
                                      if no update is found. For manual checks.
        """
        try:
            print("Creating PyUpdater client...")
            client = Client(ClientConfig())
            print(f"Checking for updates for app: {ClientConfig.APP_NAME}, version: {__version__}")
            update = client.update_check(ClientConfig.APP_NAME, __version__)
        except Exception as e:
            print(f"[ERROR] Update check failed: {e}")
            # Only show an error dialog if the user manually triggered the check.
            if show_no_update_dialog:
                QMessageBox.critical(self, "Update Error", f"Failed to check for updates:\n{e}")
            return

        if update:
            print(f"Update found: version {update.version}")
            self.prompt_for_update_and_download(update)
        elif show_no_update_dialog:
            print("No new updates found.")
            QMessageBox.information(self, "No Update", "You are using the latest version of xDrive.")

    def prompt_for_update_and_download(self, update_obj):
        """
        Shows the update dialog and handles the download/restart process.
        This must be called from the main thread, so we use a signal or QTimer.singleShot.
        """
        # Use QTimer.singleShot to ensure this GUI code runs on the main thread
        QTimer.singleShot(0, lambda: self._show_update_dialog(update_obj))

    def _show_update_dialog(self, update_obj):
        """Helper method to actually show the GUI dialog."""
        latest_version = update_obj.version
        msg = (f"A new version of xDrive is available!\n\n"
           f"  •  Current Version: {__version__}\n"
           f"  •  Latest Version: {latest_version}\n\n"
           f"Would you like to download and install it now?")

        reply = QMessageBox.question(self, "Update Available", msg,
                                 QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                 QMessageBox.StandardButton.Yes)

        if reply != QMessageBox.StandardButton.Yes:
            return

        # If user says yes, start the download process
        self._download_and_install(update_obj)

    def _download_and_install(self, update_obj):
        """Handles the actual download and restart."""
        progress = QProgressDialog("Downloading update...", None, 0, 100, self)
        progress.setWindowTitle("Downloading Update")
        progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        progress.setCancelButton(None)
        progress.show()

        def progress_hook(status):
            total = status.get("total")
            downloaded = status.get("downloaded")
            percent = 0
            if total and total > 0:
                percent = int((downloaded / total) * 100)
            progress.setValue(percent)
            QApplication.processEvents()

            try:
                # The download is blocking, but the progress hook keeps the UI responsive.
                update_obj.download(progress_hooks=[progress_hook])
            except Exception as e:
                progress.close()
                QMessageBox.critical(self, "Download Error", f"Failed to download the update:\n{e}")
                return

            progress.close()

            if update_obj.is_downloaded():
                QMessageBox.information(self, "Update Ready",
                                "The update has been downloaded. The application will now restart.")
                update_obj.extract_restart()
            else:
                QMessageBox.warning(self, "Update Failed", "The update was not downloaded successfully.")


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setOrganizationName("MyCompany")
    app.setApplicationName("S3ExplorerApp_Tabbed_v3_1")

    # Get the path to the splash image
    splash_image_path = os.path.join(os.path.dirname(__file__), 'icons', 'splash.jpg')
    
    # Load and scale the image
    splash_pix = QPixmap(splash_image_path)
    if splash_pix.isNull():
        print(f"Error: Could not load splash image from {splash_image_path}")
    else:
        # Scale to desired size while maintaining aspect ratio
        splash_pix = splash_pix.scaled(
            400, 300,  # Width, Height
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation
        )

    # Create splash screen with the scaled pixmap
    splash = QSplashScreen(splash_pix)
    
    # Center the splash screen on the screen
    screen_geometry = QApplication.primaryScreen().availableGeometry()
    splash.move(
        (screen_geometry.width() - splash.width()) // 2,
        (screen_geometry.height() - splash.height()) // 2
    )
    
    splash.show()
    app.processEvents()

    main_window = S3Explorer()
    splash.finish(main_window)
    main_window.show()

    sys.exit(app.exec())
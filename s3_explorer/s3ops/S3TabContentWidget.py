import os
import time

# Third-party libraries
from PyQt6.QtWidgets import (
    QTreeView, QLineEdit,
    QPushButton, QHBoxLayout, QVBoxLayout, QWidget,
    QMessageBox, QHeaderView, QLabel, QMenu, QStyle, QAbstractItemView, QProgressDialog
)
from PyQt6.QtGui import QStandardItemModel, QStandardItem, QIcon, QAction
from PyQt6.QtCore import Qt, QModelIndex, pyqtSignal, QUrl, QTimer

from s3ops.S3Operation import S3Operation, S3OpType

# --- Helper Functions ---
def format_size(size_bytes):
    if size_bytes is None: return ""
    if size_bytes == 0: return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = 0
    power = 1024 # Use 1024 for binary prefixes
    while size_bytes >= power and i < len(size_name) - 1:
        size_bytes /= float(power)
        i += 1
    return f"{size_bytes:.2f} {size_name[i]}"

def get_file_type(key):
    if key.endswith('/'): return "Folder"
    _, ext = os.path.splitext(key)
    return ext[1:].upper() if ext else "File"

def get_icon_for_file(filename):
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".pdf":
        return QIcon("icons/pdf.png")
    elif ext in [".txt", ".log", ".md"]:
        return QIcon("icons/text.png")
    elif ext in [".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"]:
        return QIcon("icons/image.png")
    elif ext in [".zip", ".tar", ".gz", ".rar"]:
        return QIcon("icons/archive.png")
    elif ext in [".csv", ".xls", ".xlsx"]:
        return QIcon("icons/excel.png")
    elif ext in [".py", ".js", ".java", ".cpp"]:
        return QIcon("icons/code.png")
    elif ext in [".doc",".docx"]:
        return QIcon("icons/word.png")
    else:
        return QIcon("icons/default.png")


# --- Constants for TreeView Model Columns ---
COL_NAME, COL_TYPE, COL_SIZE, COL_MODIFIED, COL_S3_KEY, COL_IS_FOLDER = range(6)

class S3TabContentWidget(QWidget):
    currentS3PathChanged = pyqtSignal(str, str) # bucket, path_in_bucket
    activeFileStatusChanged = pyqtSignal()      # For main window to update save action

    def __init__(self, main_window: 'S3Explorer', initial_bucket: str, initial_path_in_bucket: str = "", operation_manager_ref=None):
        super().__init__()
        self.main_window = main_window
        self.operation_manager = operation_manager_ref
        if self.operation_manager:
            self.s3_client = self.operation_manager.s3_client # Get client from op_manager if available
        elif hasattr(self.main_window, 'profile_manager'): # Fallback to profile_manager
             self.s3_client = self.main_window.profile_manager.get_s3_client()
        else: # Fallback to older direct way if necessary, though this should be phased out
            self.s3_client = getattr(self.main_window, 's3_client', None)
        self.is_loading = False
        self.has_loaded_once = False # New flag
        self._processing_list_finish = False

        self.current_bucket = initial_bucket
        self.current_path = initial_path_in_bucket.strip('/') # Path within the bucket
        
        self.path_history = []    # List of paths *within* self.current_bucket
        self.history_index = -1
        self._last_activated_s3_key = None
        self._last_activation_time = 0

        self.init_ui_tab()
        # Initial population will be triggered by S3Explorer after tab is added and selected
        # Or we can call it here if tab is immediately active.
        # self.navigate_to_path_tab(self.current_bucket, self.current_path)
        # Enable Drag and Drop on the TreeView (or the whole widget)
        self.tree_view.setAcceptDrops(True)
        self.tree_view.setDragDropMode(QAbstractItemView.DragDropMode.DropOnly) # We only care about dropping onto it
        # Connect D&D events for the tree_view.
        # Note: QTreeView handles dragEnterEvent and dragMoveEvent internally to some extent
        # if you set acceptDrops. We primarily need to override dropEvent.
        # To be explicit, or if dropping on the whole widget, override these:
        self.tree_view.dragEnterEvent = self.dragEnterEvent # Use same handlers for tree_view
        self.tree_view.dragMoveEvent = self.dragMoveEvent
        self.tree_view.dropEvent = self.dropEvent
        self.navigate_to_path_tab(self.current_bucket, self.current_path)  # <- Ensures first path is in history

    def init_ui_tab(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # These widgets will be reparented to the main toolbar when this tab is active
        self.breadcrumb_bar_widget = QWidget()
        self.breadcrumb_layout = QHBoxLayout(self.breadcrumb_bar_widget)
        self.breadcrumb_layout.setContentsMargins(0, 0, 0, 0)

        self.path_edit = QLineEdit()
        self.path_edit.returnPressed.connect(self.handle_path_edited_tab)

        # TreeView setup
        self.tree_view = QTreeView()
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(["Name", "Type", "Size", "Last Modified", "S3 Key", "Is Folder"])
        self.tree_view.setModel(self.model)
        self.tree_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree_view.customContextMenuRequested.connect(self.show_context_menu_tab)
        self.tree_view.doubleClicked.connect(self.on_item_double_clicked_tab)
        self.tree_view.activated.connect(self.on_item_double_clicked_tab)
        self.tree_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tree_view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.tree_view.setSortingEnabled(True)
        self.tree_view.sortByColumn(COL_NAME, Qt.SortOrder.AscendingOrder)
        self.tree_view.header().setSectionResizeMode(COL_NAME, QHeaderView.ResizeMode.Stretch)
        self.tree_view.setColumnHidden(COL_S3_KEY, True)
        self.tree_view.setColumnHidden(COL_IS_FOLDER, True)

        # Connect selection changes to main window's global action updaters
        if self.main_window and self.model.rowCount() > 0 : 
            selection_model = self.tree_view.selectionModel()
            if selection_model:
                 selection_model.selectionChanged.connect(self.main_window.update_edit_actions_state)
                 selection_model.selectionChanged.connect(self.main_window.update_save_action_state)


        layout.addWidget(self.tree_view)
        self.update_breadcrumbs_tab() # Initialize breadcrumb display

    def update_breadcrumbs_tab(self):
        while self.breadcrumb_layout.count(): # Clear previous breadcrumbs
            item = self.breadcrumb_layout.takeAt(0)
            widget = item.widget()
            if widget: widget.deleteLater()

        if not self.s3_client or not self.current_bucket:
            self.path_edit.setText("")
            self.breadcrumb_layout.addWidget(QLabel("No Bucket")) # Placeholder
            return

        # Update path_edit text
        display_path = f"s3://{self.current_bucket}"
        if self.current_path:
            display_path += f"/{self.current_path}"
        self.path_edit.setText(display_path)

        # Emit signal for main window title update
        self.currentS3PathChanged.emit(self.current_bucket, self.current_path)

        # Bucket button
        bucket_btn = QPushButton(self.current_bucket)
        bucket_btn.setStyleSheet("QPushButton { border: none; padding: 2px; text-decoration: underline; color: blue; font-weight: bold; }")
        bucket_btn.clicked.connect(lambda: self.navigate_to_path_tab(self.current_bucket, ""))
        self.breadcrumb_layout.addWidget(bucket_btn)

        # Path parts buttons
        accumulated_path_part = ""
        if self.current_path:
            for part in self.current_path.split('/'):
                if not part: continue
                self.breadcrumb_layout.addWidget(QLabel(">"))
                
                # Correctly capture part for lambda
                current_part_for_lambda = (accumulated_path_part + "/" + part).lstrip('/')
                accumulated_path_part = current_part_for_lambda # Update for next iteration

                part_btn = QPushButton(part)
                part_btn.setStyleSheet("QPushButton { border: none; padding: 2px; text-decoration: underline; color: blue; }")
                part_btn.clicked.connect(lambda checked, p=current_part_for_lambda: self.navigate_to_path_tab(self.current_bucket, p))
                self.breadcrumb_layout.addWidget(part_btn)
        self.breadcrumb_layout.addStretch(1)

    def navigate_to_path_tab(self, bucket: str, path_in_bucket: str, add_to_history=True):
        import inspect
        stack = inspect.stack()
        caller_frame = stack[1] 
        caller_function = caller_frame.function
        caller_lineno = caller_frame.lineno
        caller_filename = os.path.basename(caller_frame.filename)
        print(f"NAVIGATE_TO_PATH_TAB called from: {caller_filename} -> {caller_function}() line {caller_lineno} | For Bucket: '{bucket}', Path: '{path_in_bucket}'")
        
        if not self.s3_client:
            QMessageBox.warning(self, "S3 Error", "S3 client not available for this tab.")
            return
        
        clean_path_in_bucket = path_in_bucket.strip('/')

        if bucket != self.current_bucket: # Bucket changed for this tab
            self.current_bucket = bucket
            self.current_path = clean_path_in_bucket # Path is relative to new bucket
            self.path_history.clear()
            self.history_index = -1
        else:
            self.current_path = clean_path_in_bucket

        if add_to_history:
            # History stores paths *within the current_bucket* of this tab
            if self.history_index < len(self.path_history) -1: # Truncate future if going back then new nav
                self.path_history = self.path_history[:self.history_index + 1]
            
            # Add to history only if it's different from the last entry
            if not self.path_history or self.path_history[-1] != self.current_path:
                 self.path_history.append(self.current_path)
            self.history_index = len(self.path_history) - 1
        
        self.main_window.update_navigation_buttons_state() # Main window updates global nav buttons
        self.update_breadcrumbs_tab()
        self.populate_s3_view_tab()

    def populate_s3_view_tab(self):
        #vvv ADD THIS BLOCK vvv
        import inspect
        stack = inspect.stack()
        caller_frame = stack[1] # The frame that called this method
        caller_function = caller_frame.function
        caller_lineno = caller_frame.lineno
        caller_filename = os.path.basename(caller_frame.filename)
        print(f"POPULATE_S3_VIEW_TAB called from: {caller_filename} -> {caller_function}() line {caller_lineno} | For path: '{self.current_path}'")
        # ^^^ END ADDED BLOCK ^^^

        if self.is_loading:
            print(f"  TAB POPULATE VIEW ({self.current_path}): Already loading, bailing.")
            return
        print(f"  TAB POPULATE VIEW ({self.current_path}): Setting is_loading=True")
        self.is_loading = True
        
        self.model.removeRows(0, self.model.rowCount())
        self.main_window.status_bar.showMessage(f"Loading: s3://{self.current_bucket}/{self.current_path} ...")
        self.tree_view.setEnabled(False)

        prefix_to_list = self.current_path
        if prefix_to_list and not prefix_to_list.endswith('/'):
            prefix_to_list += '/'
        
        # This print is already good:
        # print(f"TAB POPULATE VIEW: current_bucket='{self.current_bucket}', current_path='{self.current_path}', effective prefix_to_list='{prefix_to_list}'")
        if not self.operation_manager: # Ensure op_manager is available
            QMessageBox.critical(self, "Error", "Operation Manager not available for this tab.")
            self.is_loading = False
            self.tree_view.setEnabled(True)
            return

        list_op = S3Operation(S3OpType.LIST, self.current_bucket, key=prefix_to_list,
                            callback_data={'tab_widget_ref': self})
        self.operation_manager.enqueue_s3_operation(list_op)

    def on_s3_list_finished_tab(self, result, error_message):
        s3_trash_prefix_to_hide = getattr(self.main_window, 'S3_TRASH_PREFIX', 'Trash/')
        if self._processing_list_finish:
            print(f"  RE-ENTRANT CALL DETECTED FOR on_s3_list_finished_tab ({self.current_path}). IGNORING.")
            return
        self._processing_list_finish = True

        print(f"TAB LIST FINISHED ({self.current_path}): Setting is_loading=False, has_loaded_once=True")
        self.is_loading = False
        self.has_loaded_once = True
        print(f"  Error Message: '{error_message}'")
        if result:
            print(f"  Result contains 'folders': {result.get('folders')}")
            print(f"  Result contains 'files': {len(result.get('files', []))} file entries")
        else:
            print(f"  Result object is None.")

        self.tree_view.setEnabled(True)
        if error_message:
            QMessageBox.critical(self, "S3 List Error", f"Failed to list objects in tab: {error_message}")
            self.main_window.status_bar.showMessage(f"Error listing in tab: {error_message}", 5000)
            return
        if result is None: # Should be caught by error_message generally
            self.main_window.status_bar.showMessage("Error listing in tab: No result data.", 5000)
            return

        folders = result.get("folders", [])
        files = result.get("files", [])

        for folder_key_full in sorted(folders): # folder_key_full is like "prefix/path/folder/"
            if not self.current_path and folder_key_full == s3_trash_prefix_to_hide:
                print(f"S3TabContentWidget: Hiding trash folder '{folder_key_full}' from view.")
                continue
            folder_name = os.path.basename(folder_key_full.rstrip('/'))
            if not folder_name: continue # Should not happen with CommonPrefixes

            name_item = QStandardItem(self.main_window.style().standardIcon(QStyle.StandardPixmap.SP_DirIcon), folder_name)
            type_item = QStandardItem("Folder")
            size_item = QStandardItem("") # Folders don't have size from list_objects_v2 CommonPrefixes
            modified_item = QStandardItem("") # Same for modified
            s3_key_item = QStandardItem(folder_key_full) # Store the full S3 key (prefix) for the folder
            is_folder_item = QStandardItem("1")
            self.model.appendRow([name_item, type_item, size_item, modified_item, s3_key_item, is_folder_item])

        for obj in sorted(files, key=lambda x: x['Key']):
            file_key_full = obj['Key'] # This is the full S3 key
            file_name = os.path.basename(file_key_full)
            if not file_name: continue # Skip if the key itself represents the current folder prefix being listed

            icon = get_icon_for_file(file_name)
            name_item = QStandardItem(icon, file_name)
            type_item = QStandardItem(get_file_type(file_key_full))
            size_item = QStandardItem(format_size(obj.get('Size')))
            modified_time = obj.get('LastModified')
            modified_str = modified_time.strftime('%Y-%m-%d %H:%M:%S') if modified_time else ""
            modified_item = QStandardItem(modified_str)
            s3_key_item = QStandardItem(file_key_full) # Store full S3 key
            is_folder_item = QStandardItem("0")
            self.model.appendRow([name_item, type_item, size_item, modified_item, s3_key_item, is_folder_item])
        
        self.main_window.status_bar.showMessage(f"Listed {len(folders) + len(files)} items in s3://{self.current_bucket}/{self.current_path}", 3000)
        self.tree_view.sortByColumn(COL_NAME, Qt.SortOrder.AscendingOrder) # Ensure sort is applied
        print(f"  Model populated with {self.model.rowCount()} rows after list finish.")
        self._processing_list_finish = False # Clear guard at the end
        print(f"--- END TAB LIST FINISHED ({self.current_path}) ---\n")

    def go_back_tab(self):
        if self.history_index > 0:
            self.history_index -= 1
            path_in_bucket = self.path_history[self.history_index]
            self.navigate_to_path_tab(self.current_bucket, path_in_bucket, add_to_history=False)

    def go_forward_tab(self):
        if self.history_index < len(self.path_history) - 1:
            self.history_index += 1
            path_in_bucket = self.path_history[self.history_index]
            self.navigate_to_path_tab(self.current_bucket, path_in_bucket, add_to_history=False)

    def go_up_tab(self):
        if self.current_path: # If current_path is "foo/bar", parent is "foo"
            parent_path = os.path.dirname(self.current_path)
            self.navigate_to_path_tab(self.current_bucket, parent_path)
        # Else: at bucket root, "up" does nothing in this context for a tab

    def handle_path_edited_tab(self):
        path_text = self.path_edit.text().strip()
        if path_text.startswith("s3://"):
            parts = path_text[5:].split('/', 1)
            bucket = parts[0]
            path_in_bucket = parts[1] if len(parts) > 1 else ""
            self.navigate_to_path_tab(bucket, path_in_bucket)
        elif self.current_bucket: # Assume path is relative to current tab's bucket
             self.navigate_to_path_tab(self.current_bucket, path_text)
        else:
             QMessageBox.warning(self, "Path Error", "No active bucket context for this path.")
    
    def on_item_double_clicked_tab(self, index: QModelIndex): # Also connected to 'activated' signal
        if not index.isValid():
            print("S3TabContentWidget: Invalid QModelIndex received for activation.")
            return

        row = index.row()
        s3_key_item = self.model.item(row, COL_S3_KEY)
        name_item = self.model.item(row, COL_NAME)
        is_folder_qitem = self.model.item(row, COL_IS_FOLDER)

        if not (name_item and s3_key_item and is_folder_qitem):
            print(f"S3TabContentWidget: Critical model items missing for activated row {row}.")
            return

        current_s3_key_from_model = s3_key_item.text()
        current_time = time.time()

        # Debounce: if the same item was activated very recently, ignore.
        # This helps prevent issues if 'activated' and 'doubleClicked' signals both trigger this for the same user action.
        if current_s3_key_from_model == self._last_activated_s3_key and \
           (current_time - self._last_activation_time) < 0.5: # 500ms debounce window
            print(f"S3TabContentWidget: Debounced item activation for S3 key: {current_s3_key_from_model}")
            return

        self._last_activated_s3_key = current_s3_key_from_model
        self._last_activation_time = current_time
        
        print(f"\n--- S3TabContentWidget: ITEM ACTIVATED (Double-click or Enter) ---")
        print(f"  Row: {row}")

        item_name = name_item.text()
        # s3_key is already current_s3_key_from_model
        is_folder_text_value = is_folder_qitem.text()
        is_folder = (is_folder_text_value == "1")

        print(f"  Item Name: '{item_name}'")
        print(f"  S3 Key (from model): '{current_s3_key_from_model}'")
        print(f"  Is Folder: {is_folder} (from text: '{is_folder_text_value}')")
        print(f"  Current Tab Bucket: '{self.current_bucket}', Current Tab Path: '{self.current_path}'")

        if is_folder:
            print(f"  Action: Navigating into FOLDER.")
            # For folders, current_s3_key_from_model is the full S3 prefix of the folder (e.g., "path/to/folder/")
            path_to_navigate_into = current_s3_key_from_model.strip('/') # Remove trailing slash for consistency with self.current_path
            
            # If the folder's S3 key starts with the current bucket name (it should if generated correctly),
            # extract the path *within* the bucket.
            # Example: s3_key = "bucket_name/folder_prefix/sub_folder/"
            #          current_bucket = "bucket_name"
            #          path_in_bucket = "folder_prefix/sub_folder"
            # This logic might be too complex if s3_key is already relative to current_path.
            # Assuming s3_key_item.text() for a folder is its *full prefix from bucket root*.
            
            # If current_path is 'parent_folder' and folder s3_key is 'parent_folder/child_folder/',
            # then the new path to navigate to is 'parent_folder/child_folder'.
            # The current_s3_key_from_model for a folder should be its absolute path from the bucket root.
            print(f"    Navigating to (within bucket '{self.current_bucket}'): '{path_to_navigate_into}'")
            self.navigate_to_path_tab(self.current_bucket, path_to_navigate_into)
        else:
            print(f"  Action: Opening FILE.")
            print(f"    Calling main_window.request_open_s3_file for s3_key='{current_s3_key_from_model}', item_name='{item_name}'")
            if self.main_window: # Ensure main_window (S3Explorer) reference is valid
                self.main_window.request_open_s3_file(current_s3_key_from_model, item_name, self.current_bucket, self)
            else:
                print("S3TabContentWidget: Error - main_window reference is not set.")
        print(f"--- END S3TabContentWidget: ITEM ACTIVATED ---\n")

    def get_selected_s3_items_info_tab(self):
        selected_keys, selected_is_folder, selected_names = [], [], []
        # Ensure we only process each selected row once, even if multiple columns are selected
        selected_rows = set()
        for index in self.tree_view.selectionModel().selectedIndexes():
            selected_rows.add(index.row())
        
        for row in sorted(list(selected_rows)): # Process in model order
            s3_key_item = self.model.item(row, COL_S3_KEY)
            if s3_key_item: # Check item exists
                selected_keys.append(s3_key_item.text())
                selected_is_folder.append(self.model.item(row, COL_IS_FOLDER).text() == "1")
                selected_names.append(self.model.item(row, COL_NAME).text())
        return selected_keys, selected_is_folder, selected_names

    def show_context_menu_tab(self, position):
        indexes = self.tree_view.selectedIndexes()
        if not indexes: return

        # Context usually for the primary selected item or all if multi-select actions are available
        # For simplicity, let's base it on the item at the click position if possible, or first selected.
        index_at_pos = self.tree_view.indexAt(position)
        current_row = index_at_pos.row() if index_at_pos.isValid() else indexes[0].row()

        s3_key = self.model.item(current_row, COL_S3_KEY).text()
        name = self.model.item(current_row, COL_NAME).text()
        is_folder = self.model.item(current_row, COL_IS_FOLDER).text() == "1"
        
        menu = QMenu(self)
        style = self.main_window.style()

        is_this_trash_view = self._is_trash_view()

        if is_this_trash_view:
            # --- Context Menu for TRASH VIEW ---
            restore_action = QAction(QIcon.fromTheme("edit-undo"), "Restore", self)
            restore_action.triggered.connect(lambda: self.main_window.request_restore_from_trash(s3_key, name, is_folder, self.current_bucket, self))
            menu.addAction(restore_action)

            menu.addSeparator()

            perm_delete_action = QAction(QIcon.fromTheme("edit-delete", style.standardIcon(QStyle.StandardPixmap.SP_TrashIcon)), 
                                         "Delete Permanently", self)
            perm_delete_action.triggered.connect(lambda: self.main_window.request_permanent_delete_from_trash(s3_key, name, is_folder, self.current_bucket, self))
            menu.addAction(perm_delete_action)

            # Option to "Empty Trash" if right-clicking on empty space or a general trash action
            # This is more complex; for now, item-specific actions.
            # if not index_at_pos.isValid() or is_folder (if on root trash folder):
            # menu.addSeparator()
            # empty_trash_action = QAction(QIcon.fromTheme("user-trash-full"), "Empty S3 Trash", self)
            # empty_trash_action.triggered.connect(self.main_window.request_empty_s3_trash)
            # menu.addAction(empty_trash_action)

        else:
            selected_rows = set(index.row() for index in indexes)
            if not is_folder and len(selected_rows) == 1:
                open_action = QAction(style.standardIcon(QStyle.StandardPixmap.SP_DialogOpenButton), "Open", self)
                open_action.triggered.connect(lambda: self.main_window.request_open_s3_file(s3_key, name, self.current_bucket, self))
                menu.addAction(open_action)

            properties_action = QAction(QIcon.fromTheme("document-properties", style.standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView)), "Properties", self)
            properties_action.triggered.connect(lambda: self.main_window.show_properties_dialog_from_tab(s3_key, name, is_folder, self.current_bucket, self))
            menu.addAction(properties_action)
            menu.addSeparator()


            download_action = QAction(style.standardIcon(QStyle.StandardPixmap.SP_ArrowDown), "Download", self)
            download_action.triggered.connect(lambda: self.main_window.request_download_s3_item(s3_key, name, is_folder, self.current_bucket, self))
            menu.addAction(download_action)
            menu.addSeparator()
            if is_folder:
                download_zip_action = QAction(QIcon.fromTheme("folder-download"), "Download Folder as ZIP", self)
                download_zip_action.triggered.connect(
                lambda: self.main_window.request_download_folder_as_zip(s3_key, name, self.current_bucket, self)
                )
                menu.addAction(download_zip_action)
                menu.addSeparator()

            if not is_folder and len(selected_rows) == 1 and self.s3_client:
                share_link_menu = menu.addMenu("Generate Shareable Link")
                time_options = [
                    ("1 Minute", 60),
                    ("5 Minutes", 300),
                    ("30 Minutes", 1800),
                    ("1 Hour", 3600)
                ]
                for label, seconds in time_options:
                    share_action = QAction(label, self)
                    share_action.triggered.connect(
                        lambda checked, s3_key=s3_key, bucket=self.current_bucket, seconds=seconds, name=name:
                        self.main_window.generate_shareable_s3_link(s3_key, bucket, seconds, name)
                    )
                    share_link_menu.addAction(share_action)
                menu.addSeparator()

            # Use global actions from main window for copy/cut/paste
            # Their enabled state is managed by S3Explorer.update_edit_actions_state
            if self.main_window.copy_action.isEnabled(): menu.addAction(self.main_window.copy_action)
            if self.main_window.cut_action.isEnabled(): menu.addAction(self.main_window.cut_action)
            if self.main_window.paste_action.isEnabled(): menu.addAction(self.main_window.paste_action)
            menu.addSeparator()

            delete_action = QAction(style.standardIcon(QStyle.StandardPixmap.SP_TrashIcon), f"Delete {'Folder' if is_folder else 'File'}", self)
            delete_action.triggered.connect(lambda: self.main_window.request_delete_s3_item(s3_key, name, is_folder, self.current_bucket, self))
            menu.addAction(delete_action)
            
            if self.current_bucket: # Can only create folder if tab has a bucket context
                menu.addSeparator()
                create_folder_action = QAction(style.standardIcon(QStyle.StandardPixmap.SP_FileDialogNewFolder), "New Folder Here...", self)
                create_folder_action.triggered.connect(lambda: self.main_window.request_create_s3_folder(self.current_bucket, self.current_path, self))
                menu.addAction(create_folder_action)

        menu.exec(self.tree_view.viewport().mapToGlobal(position))

    # --- Drag and Drop Event Handlers ---
    def dragEnterEvent(self, event):
        # Check if the data being dragged contains file URLs
        if event.mimeData().hasUrls():
            event.acceptProposedAction() # Accept the drag operation
        else:
            event.ignore() # Reject if not files/URLs

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event):
        if not self.main_window or not self.operation_manager:
            print("S3TabContentWidget: Main window or operation manager not available for drop.")
            event.ignore()
            return

        if not self.current_bucket:
            QMessageBox.warning(self, "Drop Error", "No active S3 bucket in this tab to drop items into.")
            event.ignore()
            return

        if event.mimeData().hasUrls():
            event.setDropAction(Qt.DropAction.CopyAction) # Indicate it's a copy
            event.accept()

            local_paths_to_upload = []
            for url in event.mimeData().urls():
                if url.isLocalFile():
                    local_paths_to_upload.append(url.toLocalFile())
            
            if not local_paths_to_upload:
                event.ignore()
                return

            print(f"S3TabContentWidget: Dropped items: {local_paths_to_upload}")
            self.handle_dropped_items_upload(local_paths_to_upload)
        else:
            event.ignore()

    def handle_dropped_items_upload(self, local_paths: list):
        """
        Processes a list of local file/folder paths dropped onto the widget.
        Queues S3 upload operations.
        """
        if not self.current_bucket: # Should be checked before calling, but good safeguard
            return

        target_s3_prefix = self.current_path.strip('/')
        if target_s3_prefix: # Ensure prefix ends with a slash if it's not root
            target_s3_prefix += '/'

        operations_to_queue = []
        total_files_to_upload_in_batch = 0 # For batch progress tracking

        for local_path in local_paths:
            if not os.path.exists(local_path):
                print(f"S3TabContentWidget: Dropped path '{local_path}' does not exist. Skipping.")
                continue

            base_name = os.path.basename(local_path)

            if os.path.isfile(local_path):
                s3_key = target_s3_prefix + base_name
                print(f"  - File: '{local_path}' -> s3://{self.current_bucket}/{s3_key}")
                op = S3Operation(S3OpType.UPLOAD_FILE, self.current_bucket, key=s3_key, local_path=local_path,
                                 callback_data={'ui_source': 'drag_drop_file'})
                operations_to_queue.append(op)
                total_files_to_upload_in_batch += 1
            
            elif os.path.isdir(local_path):
                print(f"  - Folder: '{local_path}' -> s3://{self.current_bucket}/{target_s3_prefix}{base_name}/")
                # Create the main folder marker for the dropped directory
                main_dropped_folder_s3_key = target_s3_prefix + base_name + '/'
                op_create_main_folder = S3Operation(S3OpType.CREATE_FOLDER, self.current_bucket, key=main_dropped_folder_s3_key,
                                                    callback_data={'ui_source': 'drag_drop_folder_create'})
                operations_to_queue.append(op_create_main_folder)
                
                # Recursively walk the local directory and queue uploads for files/subfolders
                for root, dirs, files in os.walk(local_path):
                    # Relative path from the initially dropped folder's root
                    relative_root = os.path.relpath(root, local_path)
                    if relative_root == '.': relative_root = '' # Root of the dropped folder
                    
                    # Create S3 keys for subdirectories
                    for dir_name in dirs:
                        s3_subfolder_key = main_dropped_folder_s3_key + \
                                           (os.path.join(relative_root, dir_name).replace(os.path.sep, '/') + '/')
                        print(f"    - Subfolder: '{os.path.join(root, dir_name)}' -> s3://{self.current_bucket}/{s3_subfolder_key}")
                        op_create_subfolder = S3Operation(S3OpType.CREATE_FOLDER, self.current_bucket, key=s3_subfolder_key,
                                                          callback_data={'ui_source': 'drag_drop_subfolder_create'})
                        operations_to_queue.append(op_create_subfolder)
                        # Note: CREATE_FOLDER ops don't usually have progress, so not counted in total_files_to_upload_in_batch

                    # Queue uploads for files
                    for file_name in files:
                        local_file_to_upload = os.path.join(root, file_name)
                        s3_file_key = main_dropped_folder_s3_key + \
                                      os.path.join(relative_root, file_name).replace(os.path.sep, '/')
                        print(f"    - File in folder: '{local_file_to_upload}' -> s3://{self.current_bucket}/{s3_file_key}")
                        op_upload_file = S3Operation(S3OpType.UPLOAD_FILE, self.current_bucket, key=s3_file_key, local_path=local_file_to_upload,
                                                     callback_data={'ui_source': 'drag_drop_file_in_folder'})
                        operations_to_queue.append(op_upload_file)
                        total_files_to_upload_in_batch += 1
            else:
                print(f"S3TabContentWidget: Dropped path '{local_path}' is neither a file nor a directory. Skipping.")

        if operations_to_queue:
            # Use OperationManager to handle this as a batch
            # If only one file, it's still a batch of one for consistency.
            batch_id = f"drag_drop_upload_{time.time()}"
            # Count only UPLOAD_FILE operations for the progress dialog's total, as CREATE_FOLDER is quick.
            # Or, count all operations if you want a total item count.
            # For now, let's count all distinct S3 "write" ops (uploads and main folder creates)
            # total_ops_for_dialog = len(operations_to_queue) # Or just total_files_to_upload_in_batch
            
            # Let's use the number of S3 UPLOAD_FILE ops for the batch dialog's count, as they are long-running.
            # CREATE_FOLDER ops are quick and typically don't show individual progress.
            # S3Explorer will show the progress dialog via OperationManager signals.
            
            # Let OperationManager start the batch and handle its progress dialog
            # We provide the list of operations. OperationManager.start_batch_operation will enqueue them.
            # We can also provide a display name for the batch.
            # `total_files_to_upload_in_batch` only counts files that will show progress.
            # The `operations_to_queue` list contains all ops (folder creates + file uploads)
            if self.operation_manager:
                 self.operation_manager.start_batch_operation(
                     batch_id=batch_id,
                     total_items=len(operations_to_queue), # Or use total_files_to_upload_in_batch for more accurate file progress
                     op_type_display=f"Uploading {len(local_paths)} dropped item(s)",
                     operations_to_queue=operations_to_queue,
                     extra_batch_data={
                         'target_tab_ref': self, # So view can be refreshed
                         'target_bucket': self.current_bucket, # For refresh context
                         'target_path_prefix': self.current_path # For refresh context
                     }
                 )
                 self.main_window.update_status_bar_message_slot(f"Started upload for {len(local_paths)} dropped item(s)...", 0)
            else:
                 QMessageBox.critical(self, "Error", "Operation Manager not available to handle uploads.")
        else:
            QMessageBox.information(self, "Drop Info", "No valid files or folders found in the dropped items to upload.")

    def _is_trash_view(self) -> bool:
        """Helper to determine if this tab is currently viewing the S3 trash."""
        s3_trash_prefix_to_check = getattr(self.main_window, 'S3_TRASH_PREFIX', '_S3_EXPLORER_TRASH_/')
        # Check if current_path *starts with* the trash prefix (allowing for subfolders in trash)
        # And ensure the bucket matches (though current_path is within a bucket)
        return self.current_path.strip('/') == s3_trash_prefix_to_check.strip('/') or \
               self.current_path.strip('/').startswith(s3_trash_prefix_to_check.strip('/') + '/')

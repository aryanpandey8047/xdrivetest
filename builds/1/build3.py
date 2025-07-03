# s3_file_explorer.py
import boto3
import os
import tempfile
import threading
import time
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from tkinterdnd2 import DND_FILES, TkinterDnD  # type: ignore
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
from classes.tooltip import Tooltip
import pyperclip3  # For non-Windows text/URI clipboard & general text
import shutil
import atexit
import sys
from urllib.parse import unquote

# Platform-specific imports for clipboard
if sys.platform == "win32":
    try:
        import win32clipboard
        import win32con  # Defines constants like CF_HDROP

        # import pythoncom # For COM initialization if needed, often handled by win32clipboard
    except ImportError:
        messagebox.showwarning(
            "Windows Dependency Missing",
            "pywin32 is not installed. Windows file copy/paste will be limited. "
            "Please install with: pip install pywin32",
        )
        win32clipboard = None  # Fallback
else:
    win32clipboard = None  # Not on Windows

load_dotenv()

# === AWS Config ===
secret_key = os.getenv("SECRET_KEY")
secret_value = os.getenv("SECRET_VALUE")
endpoint = os.getenv("ENDPOINT")
region = os.getenv("REGION")
bucket = os.getenv("BUCKET")

s3 = boto3.client(
    "s3",
    aws_access_key_id=secret_key,
    aws_secret_access_key=secret_value,
    region_name=region,
    endpoint_url=endpoint,
)
bucket_name = bucket or "oakstree"


# === Temporary Directory Management ===
TEMP_S3_DOWNLOAD_DIR = None


def get_s3_download_temp_dir():
    global TEMP_S3_DOWNLOAD_DIR
    if TEMP_S3_DOWNLOAD_DIR and os.path.exists(TEMP_S3_DOWNLOAD_DIR):
        try:
            shutil.rmtree(TEMP_S3_DOWNLOAD_DIR)
        except Exception as e:
            print(f"Warning: Could not clean previous temp dir: {e}")
    TEMP_S3_DOWNLOAD_DIR = tempfile.mkdtemp(prefix="s3_explorer_copy_")
    return TEMP_S3_DOWNLOAD_DIR


def cleanup_s3_download_temp_dir():
    global TEMP_S3_DOWNLOAD_DIR
    if TEMP_S3_DOWNLOAD_DIR and os.path.exists(TEMP_S3_DOWNLOAD_DIR):
        try:
            shutil.rmtree(TEMP_S3_DOWNLOAD_DIR)
        except Exception as e:
            print(f"Error cleaning temp dir: {e}")


atexit.register(cleanup_s3_download_temp_dir)


def os_paste_files_from_clipboard() -> list:
    """Gets a list of file/folder paths from the OS clipboard."""
    paths = []
    if sys.platform == "win32" and win32clipboard:
        try:
            win32clipboard.OpenClipboard()
            if win32clipboard.IsClipboardFormatAvailable(win32con.CF_HDROP):
                data = win32clipboard.GetClipboardData(
                    win32con.CF_HDROP
                )  # Returns a tuple of path strings
                paths.extend(list(data))
            win32clipboard.CloseClipboard()
            if paths:
                print(f"WIN32: Pasted {len(paths)} files from clipboard via CF_HDROP.")
                return paths  # Return immediately if successful with CF_HDROP
        except Exception as e:
            print(f"WIN32 Clipboard Error (Paste): {e}")
            try:
                win32clipboard.CloseClipboard()
            except:
                pass
            # Do not return here, allow fallback to text paste

    # Fallback for non-Windows, or if Windows CF_HDROP failed or returned no paths
    if (
        not paths
    ):  # Only try text paste if CF_HDROP didn't yield results or not on Windows
        try:
            clipboard_content = pyperclip3.paste()
            if not clipboard_content:
                return []

            potential_paths_raw = [
                p.strip() for p in clipboard_content.strip().split("\n") if p.strip()
            ]
            cleaned_paths = []
            for p_raw in potential_paths_raw:
                p = p_raw
                if p.startswith("file:///"):
                    p_decoded = unquote(p[len("file://") :])
                    if (
                        sys.platform == "win32"
                        and p_decoded.startswith("/")
                        and len(p_decoded) > 2
                        and p_decoded[2] == ":"
                    ):
                        p_decoded = p_decoded[1:]
                    p = p_decoded
                elif p.startswith("file://localhost/"):
                    p_decoded = unquote(p[len("file://localhost") :])
                    if (
                        sys.platform == "win32"
                        and p_decoded.startswith("/")
                        and len(p_decoded) > 2
                        and p_decoded[2] == ":"
                    ):
                        p_decoded = p_decoded[1:]
                    p = p_decoded
                cleaned_paths.append(os.path.normpath(p))

            paths = [p for p in cleaned_paths if os.path.exists(p)]
            if paths:
                print(
                    f"NON-WIN32/Fallback: Pasted {len(paths)} paths from clipboard text."
                )
        except Exception as e:
            print(f"NON-WIN32/Fallback Clipboard Error (Paste with pyperclip3): {e}")
            return []  # Return empty list on error

    return paths


# === GUI App Class ===
class S3FileExplorer(TkinterDnD.Tk):
    def __init__(self):
        super().__init__()
        self.title("S3 File Explorer")
        self.geometry("900x600")

        self.current_prefix = ""
        self.opened_files = {}
        self.history = []
        self.history_index = -1
        self.suppress_history = False

        self.path_var = tk.StringVar()
        self.breadcrumb_mode = True

        self.setup_widgets()
        self.setup_clipboard_bindings()
        self.refresh_list(self.current_prefix)

        # Shortcut keys
        self.bind("<BackSpace>", lambda e: self.go_back())
        self.bind("<Shift-BackSpace>", lambda e: self.go_forward())
        self.focus_set()

        self.icons = {
            "folder": tk.PhotoImage(file="icons/folder.png"),
            "file": tk.PhotoImage(file="icons/file.png"),
            "pdf": tk.PhotoImage(file="icons/pdf.png"),
            "image": tk.PhotoImage(file="icons/image.png"),
            "word": tk.PhotoImage(file="icons/word.png"),
            "excel": tk.PhotoImage(file="icons/excel.png"),
            "archive": tk.PhotoImage(file="icons/archive.png"),
            "text": tk.PhotoImage(file="icons/text.png"),
        }

        threading.Thread(target=self.start_watcher, daemon=True).start()
        threading.Thread(target=self.auto_refresh_loop, daemon=True).start()

    def setup_widgets(self):
        # Path Bar and Buttons
        nav_frame = tk.Frame(self)
        nav_frame.pack(fill="x", padx=5, pady=5)

        # Navigation Buttons
        self.back_button = ttk.Button(nav_frame, text="←", command=self.go_back)
        self.back_button.pack(side="left", padx=2)
        Tooltip(self.back_button, "Back (Backspace)")

        self.forward_button = ttk.Button(nav_frame, text="→", command=self.go_forward)
        self.forward_button.pack(side="left", padx=2)
        Tooltip(self.forward_button, "Forward (Shift+Backspace)")

        # search bar
        self.path_entry = ttk.Entry(nav_frame, textvariable=self.path_var)
        self.path_entry.bind("<Return>", lambda event: self.on_path_entry())

        self.breadcrumb_frame = tk.Frame(nav_frame)

        # Initially show breadcrumb, not path input
        self.path_entry.pack_forget()
        self.breadcrumb_frame.pack(side="left", fill="x", expand=True)

        self.edit_path_button = ttk.Button(
            nav_frame, text="✎", width=3, command=self.toggle_path_edit
        )
        self.edit_path_button.pack(side="left", padx=2)
        Tooltip(self.edit_path_button, "Edit Path")

        # File List
        columns = ("type", "size", "modified")
        self.list_view = ttk.Treeview(
            self, columns=columns, show="tree headings", selectmode="extended"
        )
        self.sort_column = None
        self.sort_reverse = False
        for col in columns:
            self.list_view.heading(
                col, text=col.capitalize(), command=lambda c=col: self.sort_by_column(c)
            )
            self.list_view.column(col, width=150)
        self.list_view.pack(fill="both", expand=True)
        self.list_view.bind("<Double-1>", self.on_item_double_click)
        self.list_view.bind("<Return>", self.on_item_double_click)

        # File Operation Buttons
        button_frame = tk.Frame(self)
        button_frame.pack(fill="x", padx=5, pady=5)

        ttk.Button(button_frame, text="Open", command=self.download_and_open).pack(
            side="left", padx=2
        )
        ttk.Button(button_frame, text="Upload", command=self.upload_file_dialog).pack(
            side="left", padx=2
        )
        ttk.Button(button_frame, text="Rename", command=self.rename_item).pack(
            side="left", padx=2
        )
        ttk.Button(button_frame, text="Delete", command=self.delete_item).pack(
            side="left", padx=2
        )
        ttk.Button(button_frame, text="Versions", command=self.show_versions).pack(
            side="left", padx=2
        )
        ttk.Button(button_frame, text="New Folder", command=self.create_folder).pack(
            side="left", padx=2
        )

        self.list_view.drop_target_register(DND_FILES)
        self.list_view.dnd_bind("<<Drop>>", self.on_drop)

        # Right-click menu
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="Open", command=self.download_and_open)
        self.context_menu.add_command(label="Rename", command=self.rename_item)
        self.context_menu.add_command(label="Delete", command=self.delete_item)
        self.context_menu.add_command(label="New Folder", command=self.create_folder)
        self.context_menu.add_separator()
        # self.context_menu.add_command(label="Copy to OS", command=self.handle_copy_to_os)
        # self.context_menu.add_command(label="Cut to OS", command=self.handle_cut_to_os)
        self.context_menu.add_command(
            label="Paste from OS", command=self.handle_paste_from_os
        )

        self.list_view.bind("<Button-3>", self.show_context_menu)

    def setup_clipboard_bindings(self):
        self.bind_all("<Control-v>", lambda e: self.handle_paste_from_os_event(e))
        if sys.platform == "darwin":
            self.bind_all("<Command-c>", lambda e: self.handle_copy_to_os_event(e))
            self.bind_all("<Command-x>", lambda e: self.handle_cut_to_os_event(e))
            self.bind_all("<Command-v>", lambda e: self.handle_paste_from_os_event(e))

    def _is_event_from_list_view(self, event):
        return event.widget == self.list_view

    def _is_event_from_input_field(self, event):
        return isinstance(event.widget, (ttk.Entry, tk.Text))

    def handle_paste_from_os_event(self, event):
        if self._is_event_from_input_field(event):
            event.widget.event_generate("<<Paste>>")
        else:
            self.handle_paste_from_os()

    def toggle_path_edit(self):
        if self.breadcrumb_mode:
            self.breadcrumb_frame.pack_forget()
            self.path_entry.pack(
                side="left", fill="x", expand=True, before=self.edit_path_button
            )
            self.path_entry.focus()
        else:
            self.path_entry.pack_forget()
            self.breadcrumb_frame.pack(
                side="left", fill="x", expand=True, before=self.edit_path_button
            )
            self.build_breadcrumb()
        self.breadcrumb_mode = not self.breadcrumb_mode

    def on_path_entry(self, event=None):
        path = self.path_var.get().strip().strip("/")
        normalized_path = f"{path}/" if path else ""
        self.refresh_list(normalized_path)
        if not self.breadcrumb_mode:
            self.toggle_path_edit()

    def format_size(self, size_bytes):
        if not isinstance(size_bytes, (int, float)) or size_bytes < 0:
            return ""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes / 1024**2:.1f} MB"
        return f"{size_bytes / 1024**3:.1f} GB"

    def sort_by_column(self, col):
        items = [
            (self.list_view.set(k, col), k) for k in self.list_view.get_children("")
        ]

        def convert(value):
            try:
                return (0, datetime.strptime(value, "%Y-%m-%d %H:%M"))
            except:
                pass
            try:
                units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}
                val_str = str(value).strip()
                for unit, multiplier in units.items():
                    if val_str.endswith(unit):
                        num = float(val_str[: -len(unit)].strip())
                        return (1, num * multiplier)
            except:
                pass
            return (2, str(value).lower())

        if self.sort_column == col:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = col
            self.sort_reverse = False
        items.sort(key=lambda t: convert(t[0]), reverse=self.sort_reverse)
        for index, (_, k) in enumerate(items):
            self.list_view.move(k, "", index)
        for c_col in self.list_view["columns"]:
            heading = c_col.capitalize()
            if c_col == self.sort_column:
                heading += " ↓" if self.sort_reverse else " ↑"
            self.list_view.heading(
                c_col, text=heading, command=lambda cn=c_col: self.sort_by_column(cn)
            )

    def build_breadcrumb(self):
        for widget in self.breadcrumb_frame.winfo_children():
            widget.destroy()
        parts = self.current_prefix.strip("/").split("/") if self.current_prefix else []

        def go_to(p):
            self.refresh_list(p + "/" if p else "")

        ttk.Button(self.breadcrumb_frame, text="Home", command=lambda: go_to("")).pack(
            side="left"
        )
        current_path_parts = []
        for i, part in enumerate(parts):
            if not part:
                continue
            ttk.Label(self.breadcrumb_frame, text=" / ").pack(side="left")
            current_path_parts.append(part)
            path_so_far = "/".join(current_path_parts)
            ttk.Button(
                self.breadcrumb_frame,
                text=part,
                command=lambda p_nav=path_so_far: go_to(p_nav),
            ).pack(side="left")

    def create_folder(self):
        name = simpledialog.askstring("New Folder", "Enter folder name:")
        if not name:
            return
            name = name.strip().rstrip("/")
        if not name or "/" in name:
            messagebox.showerror(
                "Invalid Name", "Folder name must not be empty or contain slashes."
            )
            return
        folder_key = f"{self.current_prefix}{name}/"
        try:
            s3.put_object(Bucket=bucket_name, Key=folder_key)
            self.refresh_list(self.current_prefix)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create folder: {e}")

    def show_context_menu(self, event):
        selection = self.list_view.identify_row(event.y)
        if selection:
            if selection not in self.list_view.selection():
                self.list_view.selection_set(selection)

        has_selection = bool(self.list_view.selection())

        for cmd in ["Open", "Rename", "Delete"]:
            self.context_menu.entryconfigure(
                cmd, state="normal" if has_selection else "disabled"
            )

        # Check if clipboard has potentially paste-able content for files
        paste_enabled = False
        if sys.platform == "win32" and win32clipboard:
            try:
                win32clipboard.OpenClipboard()
                paste_enabled = win32clipboard.IsClipboardFormatAvailable(
                    win32con.CF_HDROP
                )
                win32clipboard.CloseClipboard()
            except Exception:
                pass  # Keep paste_enabled as False

        if (
            not paste_enabled
        ):  # Fallback for non-Windows or if CF_HDROP check failed/false
            try:
                # This is a weaker check, as text could be anything
                if pyperclip3.paste().strip():
                    paste_enabled = True
            except Exception:
                pass

        self.context_menu.entryconfigure(
            "Paste from OS", state="normal" if paste_enabled else "disabled"
        )
        self.context_menu.tk_popup(event.x_root, event.y_root)

    def refresh_list(self, prefix):
        def load():
            new_prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
            try:
                response = s3.list_objects_v2(
                    Bucket=bucket_name, Prefix=new_prefix, Delimiter="/"
                )
                folders = []
                files = []
                for cp in response.get("CommonPrefixes", []):
                    folders.append(
                        (cp["Prefix"][len(new_prefix) :].rstrip("/"), "Folder", "", "")
                    )
                for obj in response.get("Contents", []):
                    key = obj["Key"]
                    if key == new_prefix:
                        continue
                    name = key[len(new_prefix) :]
                    if "/" in name:
                        continue
                    files.append(
                        (
                            name,
                            "File",
                            self.format_size(obj["Size"]),
                            obj["LastModified"].strftime("%Y-%m-%d %H:%M"),
                        )
                    )

                def update_ui():
                    self.current_prefix = new_prefix
                    self.path_var.set(self.current_prefix.rstrip("/"))
                    self.build_breadcrumb()
                    self.list_view.delete(*self.list_view.get_children())
                    for name, type_, size, modified in folders + files:
                        icon_key = "folder" if type_ == "Folder" else self.get_icon_key(name)
                        self.list_view.insert(
                            "", "end",
                            text=name,  # Required for image to show
                            image=self.icons.get(icon_key),
                            values=(type_, size, modified)
                        )
                    if not self.suppress_history and (
                        not self.history
                        or self.history[self.history_index] != self.current_prefix
                    ):
                        self.history = self.history[: self.history_index + 1] + [
                            self.current_prefix
                        ]
                        self.history_index += 1
                    self.suppress_history = False
                    self.back_button.config(
                        state="normal" if self.history_index > 0 else "disabled"
                    )
                    self.forward_button.config(
                        state="normal"
                        if self.history_index < len(self.history) - 1
                        else "disabled"
                    )

                self.after(0, update_ui)
            except Exception as e:
                self.after(
                    0,
                    lambda: messagebox.showerror(
                        "Error", f"Failed to list '{new_prefix}': {e}"
                    ),
                )

        threading.Thread(target=load, daemon=True).start()
    def get_icon_key(self, filename):
        ext = filename.lower().split('.')[-1]
        if ext in ["jpg", "jpeg", "png", "gif", "bmp"]: return "image"
        if ext in ["pdf"]: return "pdf"
        if ext in ["doc", "docx"]: return "word"
        if ext in ["xls", "xlsx"]: return "excel"
        if ext in ["zip", "rar", "7z"]: return "archive"
        if ext in ["txt", "md", "csv"]: return "text"
        return "file"


    def on_item_double_click(self, event):
        selected_id = self.list_view.selection()
        if not selected_id:
            return
        item = self.list_view.item(selected_id[0])
        name = item["text"]
        values = item["values"]
        item_type = values[0]

        if item_type == "Folder":
            self.refresh_list(
                os.path.join(self.current_prefix, name).replace("\\", "/") + "/"
            )
        elif item_type == "File":
            self.download_and_open()


    def go_back(self):
        if self.history_index > 0:
            self.history_index -= 1
            self.suppress_history = True
            self.refresh_list(self.history[self.history_index])

    def go_forward(self):
        if self.history_index < len(self.history) - 1:
            self.history_index += 1
            self.suppress_history = True
            self.refresh_list(self.history[self.history_index])

    def get_selected_s3_paths(self):
        paths = []
        selected_ids = self.list_view.selection()
        if not selected_ids:
            return []
        for item_id in selected_ids:
            item = self.list_view.item(item_id)
            name = item["text"]
            values = item["values"]
            item_type = values[0]  # Fix: get the correct type from values[0]
            paths.append(
                {
                    "s3_path": os.path.join(self.current_prefix, name).replace("\\", "/"),
                    "name": name,
                    "type": item_type,
                }
            )

        return paths

    def download_and_open(self):
        selected = self.get_selected_s3_paths()
        if not selected or len(selected) > 1:
            if len(selected) > 1:
                messagebox.showwarning("Open", "Please select only one file to open.")
                return
        item = selected[0]
        if item["type"] != "File":
            return
        key = item["s3_path"]
        local_path = os.path.join(tempfile.gettempdir(), item["name"])
        try:
            s3.download_file(bucket_name, key, local_path)
            self.opened_files[local_path] = key
            os.startfile(local_path)
        except Exception as e:
            messagebox.showerror("Error", f"Download failed for {key}: {e}")

    def upload_file_dialog(self):
        filepaths = filedialog.askopenfilenames()
        if filepaths:
            for filepath in filepaths:
                self._upload_single_item(
                    filepath, self.current_prefix
                )  # Threading handled in _upload_single_item

    def _upload_single_item(
        self, local_path, s3_target_prefix, refresh_after=True
    ):  # Modified to accept refresh_after
        # This function is now synchronous for simplicity when called in loops.
        # The calling function should handle threading for multiple items.
        base_name = os.path.basename(local_path)
        success = False
        try:
            if os.path.isfile(local_path):
                s3_key = os.path.join(s3_target_prefix, base_name).replace("\\", "/")
                print(f"Uploading file: {local_path} to s3://{bucket_name}/{s3_key}")
                s3.upload_file(local_path, bucket_name, s3_key)
                success = True
            elif os.path.isdir(local_path):
                s3_folder_key_base = os.path.join(s3_target_prefix, base_name).replace(
                    "\\", "/"
                )
                print(
                    f"Uploading folder: {local_path} to s3://{bucket_name}/{s3_folder_key_base}/"
                )
                s3.put_object(Bucket=bucket_name, Key=s3_folder_key_base + "/")
                self._upload_local_folder_recursively(local_path, s3_folder_key_base)
                success = True
        except Exception as e:
            messagebox.showerror("Upload Error", f"Failed to upload {base_name}: {e}")

        # if success and refresh_after: # Refresh handled by caller after all items
        # self.after(0, lambda: self.refresh_list(self.current_prefix))
        return success

    def rename_item(self):
        selected = self.get_selected_s3_paths()
        if not selected or len(selected) > 1:
            if len(selected) > 1:
                messagebox.showwarning("Rename", "Please select only one item.")
                return
        item = selected[0]
        old_s3_path = item["s3_path"]
        item_type = item["type"]
        new_name = simpledialog.askstring("Rename", f"Rename '{item['name']}' to:")
        if not new_name or new_name == item["name"] or "/" in new_name:
            if "/" in new_name:
                messagebox.showerror("Invalid Name", "Name cannot contain slashes.")
                return
        new_s3_path = os.path.join(self.current_prefix, new_name).replace("\\", "/")
        try:
            if item_type == "File":
                s3.copy_object(
                    CopySource={"Bucket": bucket_name, "Key": old_s3_path},
                    Bucket=bucket_name,
                    Key=new_s3_path,
                )
                s3.delete_object(Bucket=bucket_name, Key=old_s3_path)
            elif item_type == "Folder":
                old_s3_prefix = old_s3_path + "/"
                new_s3_prefix = new_s3_path + "/"
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(
                    Bucket=bucket_name, Prefix=old_s3_prefix
                ):
                    for obj in page.get("Contents", []):
                        old_obj_key = obj["Key"]
                        new_obj_key = new_s3_prefix + old_obj_key[len(old_s3_prefix) :]
                        s3.copy_object(
                            CopySource={"Bucket": bucket_name, "Key": old_obj_key},
                            Bucket=bucket_name,
                            Key=new_obj_key,
                        )
                        s3.delete_object(Bucket=bucket_name, Key=old_obj_key)
                s3.put_object(Bucket=bucket_name, Key=new_s3_prefix)
                s3.delete_object(Bucket=bucket_name, Key=old_s3_prefix)
            self.refresh_list(self.current_prefix)
        except Exception as e:
            messagebox.showerror("Error", f"Rename failed: {e}")

    def show_versions(self):
        selected = self.get_selected_s3_paths()
        if not selected or len(selected) > 1 or selected[0]["type"] != "File":
            messagebox.showwarning("Versions", "Select one file.")
            return
        key = selected[0]["s3_path"]
        try:
            versions_response = s3.list_object_versions(Bucket=bucket_name, Prefix=key)
            versions = versions_response.get("Versions", [])
            delete_markers = versions_response.get("DeleteMarkers", [])
            if not versions and not delete_markers:
                messagebox.showinfo("Versions", "No versions/markers found.")
                return
            version_window = tk.Toplevel(self)
            version_window.title(f"Versions of {os.path.basename(key)}")
            version_window.geometry("600x400")
            tree = ttk.Treeview(
                version_window,
                columns=("id", "type", "last_modified", "size", "is_latest"),
                show="headings",
            )
            tree.heading("id", text="Version/Marker ID")
            tree.heading("type", text="Type")
            tree.heading("last_modified", text="Last Modified")
            tree.heading("size", text="Size")
            tree.heading("is_latest", text="Latest")
            tree.column("id", width=200)
            all_entries = []
            if versions:
                all_entries.extend([dict(v, Type="Version") for v in versions])
            if delete_markers:
                all_entries.extend(
                    [dict(dm, Type="DeleteMarker", Size=0) for dm in delete_markers]
                )
            all_entries.sort(key=lambda x: x["LastModified"], reverse=True)
            for entry in all_entries:
                tree.insert(
                    "",
                    "end",
                    values=(
                        entry.get("VersionId", "N/A"),
                        entry.get("Type", "Version"),
                        entry["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
                        if entry.get("LastModified")
                        else "N/A",
                        self.format_size(entry.get("Size", 0))
                        if entry.get("Type") == "Version"
                        else "N/A",
                        "Yes" if entry.get("IsLatest") else "No",
                    ),
                    tags=(entry.get("Type"),),
                )
            tree.tag_configure("DeleteMarker", background="lightcoral")
            tree.pack(fill="both", expand=True, padx=5, pady=5)

            def download_selected_version():
                sel = tree.selection()
                if not sel:
                    return
                item_vals = tree.item(sel[0], "values")
                ver_id, item_type = item_vals[0], item_vals[1]
                if item_type == "DeleteMarker":
                    messagebox.showinfo("Download", "Cannot download delete marker.")
                    return
                if ver_id == "N/A":
                    messagebox.showerror("Error", "Invalid version ID.")
                    return
                local_save_path = filedialog.asksaveasfilename(
                    initialfile=os.path.basename(key), title="Save Version As"
                )
                if not local_save_path:
                    return
                try:
                    s3.download_file(
                        bucket_name,
                        key,
                        local_save_path,
                        ExtraArgs={"VersionId": ver_id},
                    )
                    messagebox.showinfo("Download Complete", f"Version saved.")
                except Exception as e_dl:
                    messagebox.showerror("Error", f"Failed to download version: {e_dl}")

            ttk.Button(
                version_window,
                text="Download Selected Version",
                command=download_selected_version,
            ).pack(pady=5)
        except Exception as e:
            messagebox.showerror("Error", f"Could not retrieve versions: {e}")

    def delete_item(self):
        selected = self.get_selected_s3_paths()
        if not selected:
            return
        names_to_delete = ", ".join(
            [f"'{item['name']}' ({item['type']})" for item in selected]
        )
        if not messagebox.askyesno("Delete", f"Delete {names_to_delete}?"):
            return

        def do_delete():
            deleted_count = 0
            errors = []
            for item in selected:
                try:
                    if item["type"] == "File":
                        s3.delete_object(Bucket=bucket_name, Key=item["s3_path"])
                        deleted_count += 1
                    elif item["type"] == "Folder":
                        folder_prefix = item["s3_path"] + "/"
                        paginator = s3.get_paginator("list_objects_v2")
                        objects_to_delete = {"Objects": []}
                        for page in paginator.paginate(
                            Bucket=bucket_name, Prefix=folder_prefix
                        ):
                            for obj in page.get("Contents", []):
                                objects_to_delete["Objects"].append({"Key": obj["Key"]})
                            if len(objects_to_delete["Objects"]) >= 1000:
                                s3.delete_objects(
                                    Bucket=bucket_name, Delete=objects_to_delete
                                )
                                deleted_count += len(objects_to_delete["Objects"])
                                objects_to_delete = {"Objects": []}
                        if objects_to_delete["Objects"]:
                            s3.delete_objects(
                                Bucket=bucket_name, Delete=objects_to_delete
                            )
                            deleted_count += len(objects_to_delete["Objects"])
                        s3.delete_object(Bucket=bucket_name, Key=folder_prefix)
                except Exception as e_del:
                    errors.append(f"Failed to delete {item['name']}: {e_del}")
            self.after(0, lambda: self.refresh_list(self.current_prefix))
            if errors:
                self.after(
                    0,
                    lambda: messagebox.showerror("Delete Error(s)", "\n".join(errors)),
                )
            elif deleted_count > 0 or (
                len(selected) > 0 and all(s["type"] == "Folder" for s in selected)
            ):  # Show success if folders were processed or files deleted
                self.after(
                    0,
                    lambda: messagebox.showinfo(
                        "Delete",
                        f"Deletion process completed for selected item(s). {deleted_count} S3 objects removed.",
                    ),
                )

        threading.Thread(target=do_delete, daemon=True).start()

    def on_drop(
        self, event
    ):  # Modified to use a single thread for sequential upload and final refresh
        try:
            file_paths = self.tk.splitlist(event.data)
        except tk.TclError:
            file_paths = event.data.split("\n")
        if not file_paths:
            return

        valid_paths = [p for p in file_paths if os.path.exists(p)]
        if not valid_paths:
            print("No valid paths found in drop data.")
            return

        def do_drop_upload():
            successful_uploads = 0
            for path in valid_paths:
                if self._upload_single_item(
                    path, self.current_prefix, refresh_after=False
                ):
                    successful_uploads += 1
            self.after(0, lambda: self.refresh_list(self.current_prefix))
            self.after(
                0,
                lambda: messagebox.showinfo(
                    "Drop Upload",
                    f"Finished processing {len(valid_paths)} dropped item(s). {successful_uploads} uploaded.",
                ),
            )

        messagebox.showinfo(
            "Drop Upload", f"Starting upload of {len(valid_paths)} items..."
        )
        threading.Thread(target=do_drop_upload, daemon=True).start()

    def start_watcher(self):
        class FileChangeHandler(FileSystemEventHandler):
            def __init__(self_handler, app_instance):
                self_handler.app = app_instance

            def on_modified(self_handler, event):
                if event.is_directory:
                    return
                filepath = event.src_path
                normalized_filepath = os.path.normcase(os.path.normpath(filepath))
                s3_key_to_upload = None
                for temp_path, s3_key in list(
                    self_handler.app.opened_files.items()
                ):  # Iterate over a copy for safe deletion
                    if (
                        os.path.normcase(os.path.normpath(temp_path))
                        == normalized_filepath
                    ):
                        s3_key_to_upload = s3_key
                        break
                if s3_key_to_upload:
                    print(f"Auto-sync: {filepath} to {s3_key_to_upload}")
                    try:
                        if os.path.exists(filepath):
                            s3.upload_file(filepath, bucket_name, s3_key_to_upload)
                            print(f"Auto-sync OK")
                        else:
                            print(f"Auto-sync skipped: {filepath} gone.")
                            del self_handler.app.opened_files[
                                temp_path
                            ]  # Use original temp_path for dict key
                    except Exception as e_sync:
                        print(f"Auto-sync failed: {e_sync}")

        temp_dir_to_watch = tempfile.gettempdir()
        event_handler = FileChangeHandler(self)
        observer = Observer()
        observer.schedule(event_handler, temp_dir_to_watch, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

    def auto_refresh_loop(self, interval=60):
        while True:
            time.sleep(interval)
            if self.focus_get() != self.path_entry and not any(
                isinstance(w, (simpledialog.Dialog, filedialog.FileDialog, tk.Toplevel))
                for w in self.winfo_children()
                if w.winfo_viewable()
            ):
                if hasattr(self, "current_prefix"):
                    self.after(0, lambda: self.refresh_list(self.current_prefix))

    def _download_s3_folder_recursively(
        self, s3_folder_prefix, local_target_dir
    ):  # Your existing
        if not s3_folder_prefix.endswith("/"):
            s3_folder_prefix += "/"
        os.makedirs(local_target_dir, exist_ok=True)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_folder_prefix):
            for obj in page.get("Contents", []):
                s3_key = obj["Key"]
                if s3_key == s3_folder_prefix:
                    continue
                relative_path = s3_key[len(s3_folder_prefix) :]
                local_file_path = os.path.join(local_target_dir, relative_path)
                if s3_key.endswith("/"):
                    os.makedirs(local_file_path, exist_ok=True)
                else:
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    s3.download_file(bucket_name, s3_key, local_file_path)
        print(f"Finished recursive download to {local_target_dir}")

    def _upload_local_folder_recursively(
        self, local_folder_path, s3_target_prefix
    ):  # Your existing
        if not s3_target_prefix.endswith("/"):
            s3_target_prefix += "/"
        for root, dirs, files in os.walk(local_folder_path):
            for filename in files:
                local_file_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_file_path, local_folder_path)
                s3_key = os.path.join(s3_target_prefix, relative_path).replace(
                    "\\", "/"
                )
                s3.upload_file(local_file_path, bucket_name, s3_key)
            for dirname in dirs:
                relative_dir_path = os.path.relpath(
                    os.path.join(root, dirname), local_folder_path
                )
                s3_folder_key = (
                    os.path.join(s3_target_prefix, relative_dir_path).replace("\\", "/")
                    + "/"
                )
                s3.put_object(Bucket=bucket_name, Key=s3_folder_key)
        print(f"Finished recursive upload from {local_folder_path}")

    def handle_paste_from_os(self):
        paths_to_upload = os_paste_files_from_clipboard()

        if not paths_to_upload:
            messagebox.showinfo(
                "Paste from OS",
                "No valid file/folder paths found on clipboard to paste.",
            )
            return

        def _do_paste_upload():
            uploaded_count = 0
            errors = []
            for i, path in enumerate(paths_to_upload):
                print(
                    f"Paste from OS: Processing {i+1}/{len(paths_to_upload)} - {path}"
                )
                if not self._upload_single_item(
                    path, self.current_prefix, refresh_after=False
                ):
                    errors.append(os.path.basename(path))
                else:
                    uploaded_count += 1

            if uploaded_count > 0:
                self.after(0, lambda: self.refresh_list(self.current_prefix))

            summary_message = (
                f"Pasted {uploaded_count} of {len(paths_to_upload)} item(s) to S3."
            )
            if errors:
                summary_message += f"\nFailures: {', '.join(errors)}"

            if errors:
                self.after(
                    0,
                    lambda: messagebox.showwarning(
                        "Paste Partially Successful", summary_message
                    ),
                )
            elif uploaded_count > 0:
                self.after(
                    0, lambda: messagebox.showinfo("Paste Successful", summary_message)
                )
            # else: no message if nothing was uploaded and no errors (e.g. filtered out)

        threading.Thread(target=_do_paste_upload, daemon=True).start()
        messagebox.showinfo(
            "Paste from OS", f"Starting paste of {len(paths_to_upload)} item(s)..."
        )


# === Main Execution ===
if __name__ == "__main__":
    if not all([secret_key, secret_value, endpoint, region]):
        messagebox.showerror(
            "AWS Config Error", "AWS config missing. Check .env or env vars."
        )
    else:
        app = S3FileExplorer()
        app.mainloop()

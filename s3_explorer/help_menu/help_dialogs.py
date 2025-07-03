from datetime import datetime
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QTextEdit, QPushButton, 
                             QMessageBox, QStyle)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QTextFormat

def show_keyboard_shortcuts(parent):
    shortcuts = [
        ("General", [
            ("New Tab", "Ctrl+T"),
            ("Save", "Ctrl+S")
        ]),
        ("Navigation", [
            ("Back", "Alt+Left Arrow"),
            ("Forward", "Alt+Right Arrow"),
            ("Up", "Alt+Up")
        ]),
        ("Edit", [
            ("Copy", "Ctrl+C"),
            ("Cut", "Ctrl+X"),
            ("Paste", "Ctrl+V")
        ]),
    ]
    
    dialog = QDialog(parent)
    dialog.setWindowTitle("Keyboard Shortcuts Reference")
    dialog.setMinimumSize(400, 300)
    
    layout = QVBoxLayout()
    text_edit = QTextEdit()
    text_edit.setReadOnly(True)
    text_edit.setStyleSheet("font-family: monospace;")
    
    content = "<h2>Keyboard Shortcuts Reference</h2>"
    for category, items in shortcuts:
        content += f"<h3>{category}</h3><ul>"
        for name, shortcut in items:
            content += f"<li><b>{name}</b>: {shortcut}</li>"
        content += "</ul>"
    
    text_edit.setHtml(content)
    layout.addWidget(text_edit)
    
    close_button = QPushButton("Close")
    close_button.clicked.connect(dialog.close)
    layout.addWidget(close_button)
    
    dialog.setLayout(layout)
    dialog.exec()

def show_about_dialog(parent):
    about_text = f"""
    <h2>Xdrive</h2>
    <p>Version: 0.8</p>
    <p>A graphical interface for managing Amazon S3 storage</p>
    <p>Developed by Oakstree</p>
    <p>Copyright © {datetime.now().year} All rights reserved</p>
    <p>This software is provided under the MIT License.</p>
    """
    
    msg_box = QMessageBox(parent)
    msg_box.setWindowTitle("About Xdrive")
    msg_box.setTextFormat(Qt.TextFormat.RichText)
    msg_box.setText(about_text)
    msg_box.setIconPixmap(parent.style().standardIcon(QStyle.StandardPixmap.SP_MessageBoxInformation).pixmap(64, 64))
    msg_box.exec()
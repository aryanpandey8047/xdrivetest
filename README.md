Generate .exe file for Windows

```
pyinstaller --name xDrive --onefile --windowed s3_explorer_app.py
```

Features

- Profile Manager: To configure multiple buckets
- Right-Click Context Menu
- Breadcrumb navigation
- Sort columns
- Display the last updated time.
- Back and Forward Support
- folder creation button
- Column Sorting
- Paste Folder/Files from OS
- File type icons
- Add a manual Refresh button too.
- Drag-and-drop ~doesn’t~ handle folders
- Trash Bin
- Multi Tab Interface

Upcoming Improvements

- Copy files to OS
- Search Feature: Filter/search box
- Logging and debug panel
- Make the refresh interval configurable.
- Integration with versioning (if bucket has it)
  - Restore this version
- Progress Bar for Upload/Download
- Multiple File Uploads

Roadmap

- Custom Actions
- Plugins Support
- SSO

import os
import json
from PyQt6.QtCore import QObject, pyqtSignal

class FavoritesManager(QObject):
    favorites_updated = pyqtSignal(list) # Emits the new list of favorites

    def __init__(self, app_data_dir, parent=None):
        super().__init__(parent)
        self.app_data_dir = app_data_dir
        self.favorites_file = os.path.join(self.app_data_dir, "favorites.json")
        self.favorites = []

    def _ensure_app_data_dir_exists(self):
        if not os.path.exists(self.app_data_dir):
            try:
                os.makedirs(self.app_data_dir, exist_ok=True)
            except OSError as e:
                print(f"Error creating application data directory {self.app_data_dir}: {e}")
                return False
        return True

    def load_favorites(self):
        self.favorites = []
        try:
            if os.path.exists(self.favorites_file):
                with open(self.favorites_file, 'r') as f:
                    loaded_favs = json.load(f)
                    if isinstance(loaded_favs, list):
                        valid_favorites = [] 
                        for fav in loaded_favs:
                            if isinstance(fav, dict) and 'bucket' in fav and 'prefix' in fav:
                                fav.setdefault('name', f"s3://{fav['bucket']}/{fav.get('prefix','').strip('/')}")
                                valid_favorites.append(fav)
                        self.favorites = valid_favorites
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading favorites from {self.favorites_file}: {e}. Initializing empty favorites.")
        self.favorites_updated.emit(self.favorites.copy())
        return self.favorites.copy()

    def save_favorites(self):
        if not self._ensure_app_data_dir_exists(): return False
        try:
            with open(self.favorites_file, 'w') as f:
                json.dump(self.favorites, f, indent=4)
            print(f"Favorites saved to {self.favorites_file}")
            self.favorites_updated.emit(self.favorites.copy()) # Notify about save
            return True
        except IOError as e:
            print(f"Error saving favorites to {self.favorites_file}: {e}")
            return False

    def add_favorite(self, name: str, bucket: str, prefix: str):
        # Basic validation, S3Explorer can do more extensive checks (e.g., duplicates)
        if not name or not bucket:
            return False, "Favorite name and bucket cannot be empty."
        
        # Optional: Check for duplicates (name or path)
        if any(f['name'] == name for f in self.favorites):
             return False, "A favorite with this name already exists."
        if any(f['bucket'] == bucket and f.get('prefix','').strip('/') == prefix.strip('/') for f in self.favorites):
             return False, "This S3 location is already in favorites."

        self.favorites.append({'name': name, 'bucket': bucket, 'prefix': prefix})
        self.save_favorites() # This will emit favorites_updated
        return True, "Favorite added."

    def get_favorites(self):
        return self.favorites.copy()
    
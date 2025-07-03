from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, unquote
import os,sys
import json
import threading
import platform
from pathlib import Path

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

PROFILE_PATH = os.path.join(APP_DATA_DIR, "profiles.json")

def load_PROFILE():
    if os.path.exists(PROFILE_PATH):
        with open(PROFILE_PATH, "r") as f:
            return json.load(f)
    else:
        return {
            "profiles": {},
            "active_profile_name": "Default"
        }

def save_PROFILE(PROFILE):
    os.makedirs(os.path.dirname(PROFILE_PATH), exist_ok=True)
    with open(PROFILE_PATH, "w") as f:
        json.dump(PROFILE, f, indent=2)

class CallbackHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/callback":
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8')
            params = parse_qs(post_data)

            creds = {
                "aws_access_key_id": unquote(params.get("access_key", [""])[0]),
                "aws_secret_access_key": unquote(params.get("secret_key", [""])[0]),
                "aws_default_region": unquote(params.get("region", ["us-east-1"])[0]),
                "endpoint_url": unquote(params.get("endpoint_url", [""])[0]),
                "default_s3_bucket": unquote(params.get("bucket_name", [""])[0])
            }

            PROFILE = load_PROFILE()
            profiles = PROFILE.get("profiles", {})

            i = 1
            while f"Default{i}" in profiles:
                i += 1
            new_profile_name = f"Default{i}"

            profiles[new_profile_name] = creds
            PROFILE["profiles"] = profiles

            save_PROFILE(PROFILE)

            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h2>Credentials received. You may close this window.</h2></body></html>")

            print(f"Credentials saved for profile: {new_profile_name}")
        else:
            self.send_error(404)


def run_callback_server(timeout=15):
    def shutdown_timer(server):
        import time
        time.sleep(timeout)
        try:
            print("Timeout reached. Shutting down server.")
            server.shutdown()
        except Exception:
            pass

    server_address = ('localhost', 1234)
    httpd = HTTPServer(server_address, CallbackHandler)
    threading.Thread(target=shutdown_timer, args=(httpd,), daemon=True).start()
    print("Waiting for credentials on http://localhost:1234/callback")
    httpd.serve_forever()
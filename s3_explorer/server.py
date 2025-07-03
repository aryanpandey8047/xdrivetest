import os
import time
import threading
import tempfile
import hashlib
import logging
from io import BytesIO
import psutil

import boto3
from botocore.exceptions import ClientError
from wsgidav.wsgidav_app import WsgiDAVApp
from wsgidav.dav_provider import DAVProvider, DAVCollection, DAVNonCollection
from wsgidav.dav_error import DAVError, HTTP_NOT_FOUND, HTTP_FORBIDDEN, HTTP_CONFLICT, HTTP_INTERNAL_ERROR
from cheroot import wsgi
from wsgidav import util

# --- Logging ---
logging.basicConfig(level=logging.DEBUG)  # Changed to DEBUG for better diagnostics
logger = logging.getLogger("S3WebDAV")

# --- Globals ---
server = None
CACHE_FOLDER = None
open_write_buffers = {}
upload_lock = threading.Lock()
CACHE_TTL = 10
_dir_cache = {}
_head_cache = {}
_head_cache_lock = threading.Lock()
_dir_cache_lock = threading.Lock()

# These are passed at runtime
s3 = None
bucket = None

# --- Helpers ---
def get_cached_head(key):
    with _head_cache_lock:
        if key in _head_cache:
            entry = _head_cache[key]
            if time.time() - entry["timestamp"] < CACHE_TTL:
                logger.debug(f"get_cached_head: Cache hit for key '{key}'")
                return entry["data"]
            logger.debug(f"get_cached_head: Cache expired for key '{key}'")
            del _head_cache[key]

    logger.debug(f"get_cached_head: Fetching head for key '{key}' from S3")
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        with _head_cache_lock:
            _head_cache[key] = {"timestamp": time.time(), "data": response}
        logger.debug(f"get_cached_head: Stored head for key '{key}' in cache")
        return response
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            with _head_cache_lock:
                _head_cache[key] = {"timestamp": time.time(), "data": None}
            logger.debug(f"get_cached_head: Key '{key}' not found in S3")
            return None
        logger.error(f"get_cached_head: Error fetching head for key '{key}': {e}")
        raise

def get_cached_listing(prefix):
    with _dir_cache_lock:
        if prefix in _dir_cache and time.time() - _dir_cache[prefix]["timestamp"] < CACHE_TTL:
            logger.debug(f"get_cached_listing: Cache hit for prefix '{prefix}'")
            return _dir_cache[prefix]["data"]
        logger.debug(f"get_cached_listing: Cache expired or miss for prefix '{prefix}'")
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
        with _dir_cache_lock:
            _dir_cache[prefix] = {"timestamp": time.time(), "data": response}
        logger.debug(f"get_cached_listing: Stored listing for prefix '{prefix}' in cache")
        return response
    except ClientError as e:
        logger.error(f"get_cached_listing: Failed to list objects for prefix '{prefix}': {e}")
        raise

def invalidate_caches_for_key(key):
    """Invalidate cache entries for a key and its parent directory."""
    with _head_cache_lock:
        if key in _head_cache:
            logger.debug(f"invalidate_caches_for_key: Clearing head cache for key '{key}'")
            del _head_cache[key]
    parent_prefix = os.path.dirname(key) + "/"
    with _dir_cache_lock:
        if parent_prefix in _dir_cache:
            logger.debug(f"invalidate_caches_for_key: Clearing dir cache for prefix '{parent_prefix}'")
            del _dir_cache[parent_prefix]

# --- WebDAV Resources ---
class UploadingFileWrapper:
    def __init__(self, tmp_file, key, tmp_path):
        self._file = tmp_file
        self._key = key
        self._tmp_path = tmp_path
        self._closed = False
        self._write_complete = False
        self._lock = threading.Lock()
        logger.debug(f"UploadingFileWrapper initialized for {self._key}, temp file: {self._tmp_path}")

    def write(self, data):
        with self._lock:
            return self._file.write(data)

    def read(self, size=-1):
        with self._lock:
            return self._file.read(size)

    def seek(self, offset, whence=0):
        with self._lock:
            return self._file.seek(offset, whence)

    def tell(self):
        with self._lock:
            return self._file.tell()

    def flush(self):
        with self._lock:
            self._file.flush()
            os.fsync(self._file.fileno())  # Ensure physical write to disk
            logger.debug(f"Flushed data for {self._key}")

    def close(self):
        if self._closed:
            logger.debug(f"UploadingFileWrapper.close: File for key '{self._key}' already closed")
            return

        logger.info(f"UploadingFileWrapper.close: Closing and uploading '{self._key}'")
        
        try:
            # Final flush and sync
            self.flush()
            
            # Get current position and size
            current_pos = self._file.tell()
            self._file.seek(0, os.SEEK_END)
            file_size = self._file.tell()
            self._file.seek(0)
            
            if file_size == 0:
                logger.warning(f"Attempting to upload empty file for {self._key}")
            
            # Use a new file handle for reading to avoid locks
            with open(self._tmp_path, 'rb') as read_file:
                try:
                    # Upload with a fresh file handle
                    s3.upload_fileobj(
                        read_file,
                        bucket,
                        self._key,
                        Callback=lambda bytes_transferred: logger.debug(f"Upload progress for {self._key}: {bytes_transferred} bytes")
                    )
                    self._write_complete = True
                    logger.info(f"Successfully uploaded {self._key} ({file_size} bytes)")
                    
                    # Verify upload
                    try:
                        head = s3.head_object(Bucket=bucket, Key=self._key)
                        if head['ContentLength'] != file_size:
                            raise Exception(f"Upload size mismatch. Expected {file_size}, got {head['ContentLength']}")
                        logger.debug(f"Upload verification passed for {self._key}")
                    except Exception as verify_error:
                        logger.error(f"Upload verification failed for {self._key}: {verify_error}")
                        raise
                    
                    # Update cache
                    with _head_cache_lock:
                        _head_cache[self._key] = {
                            "timestamp": time.time(),
                            "data": head
                        }
                    
                except Exception as upload_error:
                    logger.error(f"Upload failed for {self._key}: {upload_error}", exc_info=True)
                    raise DAVError(HTTP_INTERNAL_ERROR, context_info=str(upload_error))
                
        except Exception as e:
            logger.error(f"Error during close/upload for {self._key}: {e}", exc_info=True)
            raise
        finally:
            try:
                # Close the original file handle
                if hasattr(self._file, 'close'):
                    self._file.close()
                self._closed = True
                
                # Clean up temp file
                if os.path.exists(self._tmp_path):
                    try:
                        os.unlink(self._tmp_path)
                        logger.debug(f"Deleted temp file {self._tmp_path}")
                    except OSError as cleanup_error:
                        logger.warning(f"Could not delete temp file {self._tmp_path}: {cleanup_error}")
                
                # Remove from open buffers
                with upload_lock:
                    if self._key in open_write_buffers:
                        del open_write_buffers[self._key]
                        logger.debug(f"Removed {self._key} from open_write_buffers")
                
                # Invalidate caches if write was successful
                if self._write_complete:
                    invalidate_caches_for_key(self._key)
            except Exception as final_error:
                logger.error(f"Error in final cleanup for {self._key}: {final_error}")


class S3DAVProvider(DAVProvider):
    def __init__(self, s3_client, bucket_name):
        super().__init__()
        self.s3 = s3_client
        self.bucket = bucket_name

    def get_resource_inst(self, path, environ):
        key = path.strip("/")
        logger.debug(f"get_resource_inst: Processing path '{path}' (key: '{key}'), method: {environ.get('REQUEST_METHOD', 'UNKNOWN')}")
        if not key:
            logger.debug("get_resource_inst: Root path, returning collection")
            return S3Collection(self, "", environ)
        if path.endswith("/"):
            logger.debug(f"get_resource_inst: Path ends with '/', treating as collection: '{key}'")
            return S3Collection(self, key, environ)
        
        try:
            # Check if the file exists or is being written
            if key in open_write_buffers:
                logger.debug(f"get_resource_inst: Key '{key}' is in open_write_buffers, returning resource")
                return S3Resource(self, key, environ)
            
            head = get_cached_head(key)
            if head is not None:
                logger.debug(f"get_resource_inst: Key '{key}' exists in S3, returning resource")
                return S3Resource(self, key, environ)
            
            # Check if it's a folder
            folder_key = key + "/"
            listing = get_cached_listing(folder_key)
            if listing.get("KeyCount", 0) > 0 or listing.get("CommonPrefixes"):
                logger.debug(f"get_resource_inst: Key '{key}' is a folder (has contents), returning collection")
                return S3Collection(self, key, environ)
            
            # Non-existent file: return S3Resource only for PUT requests
            if environ.get("REQUEST_METHOD") == "PUT":
                logger.debug(f"get_resource_inst: Non-existent key '{key}', but PUT request, returning resource for creation")
                return S3Resource(self, key, environ)
            
            logger.debug(f"get_resource_inst: Key '{key}' does not exist and not a PUT request, returning None")
            return None
        except Exception as e:
            logger.error(f"get_resource_inst: Error processing path '{path}': {e}", exc_info=True)
            raise DAVError(HTTP_INTERNAL_ERROR, context_info=str(e))

class S3Collection(DAVCollection):
    def __init__(self, provider, path, environ):
        super().__init__("/" + path, environ)
        self.provider = provider
        self.path = path

    def exists(self):
        if not self.path.strip("/"):
            logger.debug("S3Collection.exists: Root path, exists")
            return True
        prefix = self.path.strip("/") + "/"
        result = get_cached_listing(prefix)
        exists = result.get("KeyCount", 0) > 0 or result.get("CommonPrefixes")
        logger.debug(f"S3Collection.exists: Path '{self.path}' exists: {exists}")
        return exists

    def get_member_names(self):
        prefix = self.path.strip("/")
        if prefix:
            prefix += "/"
        else:
            prefix = ""
        result = get_cached_listing(prefix)
        members = set()
        for obj in result.get("Contents", []):
            name = obj["Key"][len(prefix):]
            if name and "/" not in name:
                members.add(name)
        for cp in result.get("CommonPrefixes", []):
            folder_name = cp["Prefix"][len(prefix):].strip("/")
            if folder_name:
                members.add(folder_name)
        logger.debug(f"S3Collection.get_member_names: Members for prefix '{prefix}': {members}")
        return list(members)

    def get_member(self, name):
        member_path = self.path.strip("/") + "/" + name
        logger.debug(f"S3Collection.get_member: Getting member '{name}' (path: '{member_path}')")
        return self.provider.get_resource_inst(member_path, self.environ)

    def create_collection(self, name):
        key = self.path.strip("/") + "/" + name + "/"
        logger.info(f"S3Collection.create_collection: Creating folder '{key}'")
        try:
            s3.put_object(Bucket=bucket, Key=key)
            with _dir_cache_lock:
                _dir_cache.pop(self.path.strip("/") + "/", None)
            invalidate_caches_for_key(key)
        except ClientError as e:
            logger.error(f"Failed to create collection: {key} - {e}")
            raise DAVError(HTTP_FORBIDDEN, context_info=str(e))

class S3Resource(DAVNonCollection):
    def __init__(self, provider, path, environ):
        super().__init__("/" + path, environ)
        self.provider = provider
        self.key = path
        logger.debug(f"S3Resource.init: Created resource for key '{self.key}'")

    def exists(self):
        if self.key in open_write_buffers:
            logger.debug(f"S3Resource.exists: Key '{self.key}' in open_write_buffers, exists")
            return True
        meta = get_cached_head(self.key)
        exists = meta is not None
        logger.debug(f"S3Resource.exists: Key '{self.key}' exists in S3: {exists}")
        return exists

    def get_content_length(self):
        if self.key in open_write_buffers:
            file = open_write_buffers[self.key]["file"]
            pos = file.tell()
            file.seek(0, 2)
            size = file.tell()
            file.seek(pos)
            logger.debug(f"S3Resource.get_content_length: Key '{self.key}' in write buffer, size: {size}")
            return size
        meta = get_cached_head(self.key)
        if not meta:
            logger.debug(f"S3Resource.get_content_length: No metadata for '{self.key}'")
            return None
        size = meta.get("ContentLength")
        logger.debug(f"S3Resource.get_content_length: Key '{self.key}', size: {size}")
        return size

    def get_etag(self):
        if self.key in open_write_buffers:
            etag = "in-progress-" + hashlib.md5(self.key.encode()).hexdigest()
            logger.debug(f"S3Resource.get_etag: Key '{self.key}' in write buffer, etag: {etag}")
            return etag
        meta = get_cached_head(self.key)
        if not meta or "ETag" not in meta:
            logger.debug(f"S3Resource.get_etag: No metadata for '{self.key}'")
            return None
        etag = meta.get("ETag", "").strip('"')
        logger.debug(f"S3Resource.get_etag: Key '{self.key}', etag: {etag}")
        return etag

    def get_last_modified(self):
        if self.key in open_write_buffers:
            modified = time.time()
            logger.debug(f"S3Resource.get_last_modified: Key '{self.key}' in write buffer, modified: {modified}")
            return modified
        meta = get_cached_head(self.key)
        if not meta or "LastModified" not in meta:
            logger.debug(f"S3Resource.get_last_modified: No metadata for '{self.key}'")
            return None
        modified = meta.get("LastModified").timestamp()
        logger.debug(f"S3Resource.get_last_modified: Key '{self.key}', modified: {modified}")
        return modified

    def get_content(self):
        if self.key in open_write_buffers:
            buffer = open_write_buffers[self.key]["file"]
            buffer.seek(0)
            logger.debug(f"S3Resource.get_content: Reading from write buffer for '{self.key}'")
            return BytesIO(buffer.read())
        logger.debug(f"S3Resource.get_content: Fetching content from S3 for '{self.key}'")
        try:
            response = s3.get_object(Bucket=bucket, Key=self.key)
            return BytesIO(response["Body"].read())
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.debug(f"S3Resource.get_content: Key '{self.key}' not found")
                raise DAVError(HTTP_NOT_FOUND)
            logger.error(f"S3Resource.get_content: Error fetching '{self.key}': {e}")
            raise DAVError(HTTP_FORBIDDEN, context_info=str(e))
        
    def begin_write(self, content_type=None):
        logger.info(f"S3Resource.begin_write: Starting write for '{self.key}'")
        try:
            # Check if this is actually a folder
            folder_key = self.key + "/"
            listing = get_cached_listing(folder_key)
            if listing.get("KeyCount", 0) > 0 or listing.get("CommonPrefixes"):
                logger.warning(f"S3Resource.begin_write: Key '{self.key}' is a folder")
                raise DAVError(HTTP_CONFLICT, context_info=f"Cannot write to '{self.key}' as it is a folder")

            with upload_lock:
                # Clean up any existing buffer
                if self.key in open_write_buffers:
                    logger.warning(f"S3Resource.begin_write: Key '{self.key}' already being written, cleaning up")
                    try:
                        old_wrapper = open_write_buffers[self.key]["file"]
                        if hasattr(old_wrapper, 'close'):
                            old_wrapper.close()
                    except Exception as e:
                        logger.error(f"Error cleaning up previous buffer: {e}")
                    finally:
                        open_write_buffers.pop(self.key, None)

            # Create temp directory if it doesn't exist
            os.makedirs(CACHE_FOLDER, exist_ok=True)
            
            # Create temp file with unique name to prevent conflicts
            temp_fd, temp_path = tempfile.mkstemp(dir=CACHE_FOLDER)
            os.close(temp_fd)  # Close the file descriptor we just opened
            
            # Open with explicit mode and buffering=0 to prevent OS-level buffering
            tmp_file = open(temp_path, 'w+b', buffering=0)
            
            # Create wrapper
            wrapper = UploadingFileWrapper(tmp_file, self.key, temp_path)
            
            with upload_lock:
                open_write_buffers[self.key] = {"file": wrapper}
                
            logger.debug(f"S3Resource.begin_write: Created new write buffer for '{self.key}'")
            return wrapper

        except Exception as e:
            logger.error(f"S3Resource.begin_write: Error starting write for '{self.key}': {e}", exc_info=True)
            raise DAVError(HTTP_INTERNAL_ERROR, context_info=str(e))

        except Exception as e:
            logger.error(f"S3Resource.begin_write: Error starting write for '{self.key}': {e}", exc_info=True)
            raise DAVError(HTTP_INTERNAL_ERROR, context_info=str(e))


    def delete(self):
        logger.info(f"S3Resource.delete: Deleting '{self.key}'")
        try:
            s3.delete_object(Bucket=bucket, Key=self.key)
            invalidate_caches_for_key(self.key)
        except ClientError as e:
            logger.error(f"S3Resource.delete: Failed to delete '{self.key}': {e}")
            raise DAVError(HTTP_FORBIDDEN)
        logger.debug(f"S3Resource.delete: Deleted '{self.key}' and invalidated caches")

    def support_etag(self):
        return True

    def get_display_name(self):
        name = os.path.basename(self.key)
        logger.debug(f"S3Resource.get_display_name: Key '{self.key}', display name: '{name}'")
        return name

# --- Server Lifecycle ---
def start_webdav(mount_path, aws_access_key, aws_secret_key, region, endpoint, bucket_name, host="localhost", port=8080):
    global CACHE_FOLDER, server, s3, bucket
    logger.info(f"start_webdav: Starting WebDAV server with mount_path: {mount_path}, bucket: {bucket_name}, endpoint: {endpoint}")
    CACHE_FOLDER = mount_path
    os.makedirs(CACHE_FOLDER, exist_ok=True)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
        endpoint_url=endpoint,
    )
    bucket = bucket_name

    try:
        s3.head_bucket(Bucket=bucket)
        logger.debug(f"start_webdav: Successfully accessed bucket '{bucket}'")
    except ClientError as e:
        logger.error(f"start_webdav: Bucket access failed: {e}")
        raise

    app = WsgiDAVApp({
        "provider_mapping": {"/": S3DAVProvider(s3, bucket)},
        "simple_dc": {"user_mapping": {"*": True}},
        "verbose": 3,
        "logging": {"enable_loggers": ["wsgidav"]},
        "lock_storage": True,  # Disable locking
    })

    try:
        server = wsgi.Server((host, port), app)
        logger.info(f"start_webdav: Starting WebDAV server on http://{host}:{port}")
        server.start()
    except Exception as e:
        logger.error(f"start_webdav: Failed to start server: {e}", exc_info=True)
        raise

def stop_webdav():
    global server
    if server:
        logger.info("Stopping WebDAV server")
        server.stop()
        server = None
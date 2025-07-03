import uuid
from enum import Enum


# --- S3 Operation Types and Data Class ---
class S3OpType(Enum):
    LIST = "list"
    DELETE_OBJECT = "delete_object"
    DELETE_FOLDER = "delete_folder"
    DOWNLOAD_TO_TEMP = "download_to_temp"
    DOWNLOAD_FILE = "download_file"
    UPLOAD_FILE = "upload_file"
    CREATE_FOLDER = "create_folder"
    COPY_OBJECT = "copy_object"


class S3Operation:
    def __init__(self, op_type: S3OpType, bucket: str, key: str = None,
                 new_key: str = None, local_path: str = None,
                 is_part_of_move: bool = False, original_source_key_for_move: str = None,
                 callback_data: dict = None):
        self.id = uuid.uuid4()
        self.op_type = op_type
        self.bucket = bucket
        self.key = key
        self.new_key = new_key
        self.local_path = local_path
        self.is_part_of_move = is_part_of_move
        self.original_source_key_for_move = original_source_key_for_move
        self.callback_data = callback_data if callback_data else {} # Ensure it's a dict

    def __repr__(self):
        return f"<S3Operation {self.op_type.value} on s3://{self.bucket}/{self.key or self.new_key or ''}>"

import os
import shutil
from fastapi import UploadFile
from .config import UPLOAD_DIR

def save_upload(file: UploadFile) -> str:
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return file_path

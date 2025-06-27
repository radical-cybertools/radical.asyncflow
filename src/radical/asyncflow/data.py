import os
import requests

from pathlib import Path
from typing import List, Union

import radical.utils as ru

URL_SCHEMES = [
    "http",           # Hypertext Transfer Protocol (HTTP)
    "https",          # Hypertext Transfer Protocol Secure (HTTPS)
    "ftp",            # File Transfer Protocol (FTP)
    "ftps",           # File Transfer Protocol Secure (FTPS, FTP over SSL/TLS)
    "sftp",           # Secure File Transfer Protocol (SFTP, SSH-based)
    "file",           # Local file system (Access files directly from the file system)
    "data",           # Data URIs (Inline base64-encoded files, though not common for large files)
    "s3",             # Amazon S3 (Cloud storage URL, used to expose files stored in S3)
    "azure",          # Azure Storage (Blob storage URL)
    "r2",             # Cloudflare R2 (Object storage URL)
    "gs",             # Google Cloud Storage (Bucket URL)
]

class File:
    def __init__(self) -> None:
        self.filename = None
        self.filepath = None

    @staticmethod
    def download_remote_url(url: str) -> Path:
        """Download the remote file to the current directory and return its full path."""
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check if the download was successful

        # Use the file name from the URL, defaulting if not available
        filename = url.split("/")[-1] or "downloaded_file"
        file_path = Path(filename)

        # Save the file content
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return file_path.resolve()  # Return the absolute path


class InputFile(File):
    def __init__(self, file):
        # Initialize file-related variables
        self.remote_url = None
        self.local_file = None
        self.other_task_file = None
        
        self.filepath = None  # Ensure that filepath is initialized

        # Determine file type (remote, local, or task-produced)
        possible_url = ru.Url(file)
        if possible_url.scheme in URL_SCHEMES:
            self.remote_url = file
        elif os.path.exists(file):  # Check if it's a local file
            self.local_file = file
        else:
            self.other_task_file = file

        # Handle remote file (download and resolve path)
        if self.remote_url:
            self.filepath = self.download_remote_url(self.remote_url)
        
        # Handle local file (ensure it exists and resolve path)
        elif self.local_file:
            self.filepath = Path(self.local_file).resolve()  # Convert to absolute path

        # Handle file from another task. We do not resolve Path here as this
        # file is not created yet and it will be resolved when the task is executed.
        elif self.other_task_file:
            self.filepath = Path(self.other_task_file)

        # If file resolution failed, raise an exception with a more descriptive message
        if not self.filepath:
            raise Exception(f"Failed to resolve InputFile: {file}. "
                             "Ensure it's a valid URL, local path, or task output.")

        # Set the filename from the resolved filepath
        self.filename = self.filepath.name


class OutputFile(File):
    def __init__(self, filename):
        if not filename:
            raise ValueError("Filename cannot be empty")

        # Use os.path.basename() to handle paths
        self.filename = os.path.basename(filename)

        # Edge case: If the filename ends with a separator (e.g., '/')
        if not self.filename:
            raise ValueError(f"Invalid filename, the path {filename} does not include a file")

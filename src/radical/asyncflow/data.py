import os
from pathlib import Path
from urllib.parse import urlparse

import requests

URL_SCHEMES = [
    "http",  # Hypertext Transfer Protocol (HTTP)
    "https",  # Hypertext Transfer Protocol Secure (HTTPS)
    "ftp",  # File Transfer Protocol (FTP)
    "ftps",  # File Transfer Protocol Secure (FTPS, FTP over SSL/TLS)
    "sftp",  # Secure File Transfer Protocol (SFTP, SSH-based)
    "file",  # Local file system (Access files directly from the file system)
    "data",  # Data URIs (Inline base64-encoded files)
    "s3",  # Amazon S3 (Cloud storage URL, used to expose files stored in S3)
    "azure",  # Azure Storage (Blob storage URL)
    "r2",  # Cloudflare R2 (Object storage URL)
    "gs",  # Google Cloud Storage (Bucket URL)
]


class File:
    """Base class for file handling in task execution systems.

    Provides common attributes and functionality for managing files with filename and
    filepath properties.
    """

    def __init__(self) -> None:
        """Initialize a File object with default None values.

        Sets filename and filepath attributes to None, to be populated by subclasses
        during file resolution.
        """
        self.filename = None
        self.filepath = None

    @staticmethod
    def download_remote_url(url: str) -> Path:
        """Download a remote file to the current directory and return its full path.

        Downloads file content from a remote URL using streaming to handle large files
        efficiently. Saves the file with a name derived from the URL.

        Args:
            url: The remote URL to download from.

        Returns:
            Path: Absolute path to the downloaded file.

        Raises:
            requests.exceptions.RequestException: If the download fails
            or URL is invalid.

        Example:
            ::

                file_path = File.download_remote_url("https://example.com/data.txt")
                print(f"Downloaded to: {file_path}")
        """
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check if the download was successful

        # Use the file name from the URL, defaulting if not available
        filename = url.split("/")[-1] or "downloaded_file"
        file_path = Path(filename)

        # Save the file content
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return file_path.resolve()  # Return the absolute path


class InputFile(File):
    """Represents an input file that can be sourced from remote URLs, local paths, or
    task outputs.

    Automatically detects the file source type and handles appropriate resolution.
    Supports remote file downloading, local file path resolution, and task output file
    references.
    """

    def __init__(self, file):
        """Initialize an InputFile with automatic source type detection and resolution.

        Determines whether the input is a remote URL, local file path, or reference
        to another task's output file, then resolves the appropriate file path.

        Args:
            file: Input file specification. Can be:
                - Remote URL (http, https, ftp, s3, etc.)
                - Local file path (absolute or relative)
                - Task output file reference (filename for future resolution)

        Raises:
            Exception: If file resolution fails or file source cannot be determined.

        Attributes:
            remote_url (str): URL if file is remote, None otherwise.
            local_file (str): Local path if file exists locally, None otherwise.
            other_task_file (str): Task reference if file is from another task.
            filepath (Path): Resolved file path.
            filename (str): Extracted filename from the resolved path.
        """
        # Initialize file-related variables
        self.remote_url = None
        self.local_file = None
        self.other_task_file = None

        self.filepath = None  # Ensure that filepath is initialized

        # Determine file type (remote, local, or task-produced)
        parsed_url = urlparse(file)
        if parsed_url.scheme in URL_SCHEMES:
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
            raise Exception(
                f"Failed to resolve InputFile: {file}. "
                "Ensure it's a valid URL, local path, or task output."
            )

        # Set the filename from the resolved filepath
        self.filename = self.filepath.name


class OutputFile(File):
    """Represents an output file that will be produced by a task.

    Handles filename validation and extraction from file paths, ensuring proper output
    file naming for task execution.
    """

    def __init__(self, filename):
        """Initialize an OutputFile with filename validation.

        Extracts the filename from the provided path and validates that it
        represents a valid file (not a directory or empty path).

        Args:
            filename: The output filename or path. Can be a simple filename
                or a path, but must resolve to a valid filename.

        Raises:
            ValueError: If filename is empty or resolves to an invalid file path.

        Attributes:
            filename (str): The extracted filename for the output file.

        Example:
            ::

                # Valid initializations
                output1 = OutputFile("result.txt")
                output2 = OutputFile("path/to/result.txt")

                # Invalid - will raise ValueError
                output3 = OutputFile("")  # Empty filename
                output4 = OutputFile("path/")  # Path ends with separator
        """
        if not filename:
            raise ValueError("Filename cannot be empty")

        # Use os.path.basename() to handle paths
        self.filename = os.path.basename(filename)

        # Edge case: If the filename ends with a separator (e.g., '/')
        if not self.filename:
            raise ValueError(
                f"Invalid filename, the path {filename} does not include a file"
            )

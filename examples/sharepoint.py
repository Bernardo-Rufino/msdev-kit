"""SharePoint file examples.

The helpers are intentionally not called by default because uploads and folder
creation modify the configured site.
"""


def download(sp, remote_path, local_dir):
    """Download a file to a local directory."""
    path = sp.download_file(remote_path, local_dir=local_dir)
    print(f"Saved to: {path}")
    return path


def upload(sp, folder, remote_path, local_path):
    """Write example. Create a folder and upload a local file."""
    sp.create_folder(folder)
    sp.upload_file(remote_path=remote_path, source=local_path)
    print(f"Uploaded {local_path} -> {remote_path}")


if __name__ == "__main__":
    print("No SharePoint operation was run. Edit this file and call a helper with real paths.")

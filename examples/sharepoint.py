"""SharePoint examples: download a file, create a folder, upload a file."""

from examples._setup import build_clients


def download(sp, remote_path, local_dir):
    path = sp.download_file(remote_path, local_dir=local_dir)
    print(f"Saved to: {path}")
    return path


def upload(sp, folder, remote_path, local_path):
    sp.create_folder(folder)
    sp.upload_file(remote_path=remote_path, source=local_path)
    print(f"Uploaded {local_path} -> {remote_path}")


if __name__ == "__main__":
    clients = build_clients()
    sp = clients["sharepoint"]

    download(sp, "/Shared Documents/report.xlsx", local_dir="./data")
    upload(
        sp,
        folder="/Shared Documents/Reports/2026",
        remote_path="/Shared Documents/Reports/2026/output.csv",
        local_path="./data/output.csv",
    )

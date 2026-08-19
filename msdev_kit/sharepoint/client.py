import mimetypes
import os
from datetime import datetime
from typing import Optional, Union
from urllib.parse import quote

import requests

from msdev_kit.auth import Auth


class SharePointClient:
    _GRAPH_BASE = "https://graph.microsoft.com/v1.0"

    def __init__(self, auth: Auth, sp_hostname: str, sp_site_path: str):
        self._auth = auth

        hostname = (
            sp_hostname.replace("https://", "").replace("http://", "").rstrip("/")
        )
        if not hostname.endswith(".sharepoint.com"):
            hostname = f"{hostname}.sharepoint.com"
        self._sp_hostname = hostname

        site_path = sp_site_path.strip("/")
        if not site_path.startswith("sites/"):
            site_path = f"sites/{site_path}"
        self._sp_site_path = site_path

        self._site_id: Optional[str] = None

    def _headers(self) -> dict:
        return {
            "Authorization": f'Bearer {self._auth.get_token("graph")}',
            "Content-Type": "application/json",
        }

    def _ts(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _get_site_id(self) -> str:
        """Resolve and cache the Graph site ID for this SharePoint site."""
        if self._site_id:
            return self._site_id

        resp = requests.get(
            f"{self._GRAPH_BASE}/sites/{self._sp_hostname}:/{self._sp_site_path}",
            headers=self._headers(),
            params={"$select": "id"},
            timeout=30,
        )
        resp.raise_for_status()
        self._site_id = resp.json()["id"]
        return self._site_id

    def download_file(self, file_path: str, local_dir: str) -> str:
        """Download a file from the site's default document library."""
        site_id = self._get_site_id()
        local_path = os.path.join(local_dir, os.path.basename(file_path))

        resp = requests.get(
            f"{self._GRAPH_BASE}/sites/{site_id}/drive/root:{file_path}:/content",
            headers={"Authorization": f'Bearer {self._auth.get_token("graph")}'},
            timeout=120,
        )
        resp.raise_for_status()

        with open(local_path, "wb") as file:
            file.write(resp.content)
        return local_path

    def create_folder(self, folder_path: str) -> None:
        """Create a folder and any missing intermediate folders."""
        parts = [part for part in folder_path.strip("/").split("/") if part]
        if not parts:
            raise ValueError("folder_path must contain at least one folder name")

        site_id = self._get_site_id()
        for index, name in enumerate(parts):
            parent_path = "/".join(parts[:index])
            parent_ref = (
                "root/children"
                if not parent_path
                else f"root:/{quote(parent_path, safe='/')}:/children"
            )
            resp = requests.post(
                f"{self._GRAPH_BASE}/sites/{site_id}/drive/{parent_ref}",
                headers=self._headers(),
                json={
                    "name": name,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "replace",
                },
                timeout=30,
            )
            resp.raise_for_status()

    def upload_file(
        self,
        remote_path: str,
        source: Union[str, bytes, os.PathLike],
        content_type: Optional[str] = None,
    ) -> None:
        """Upload or overwrite a file in the default document library.

        ``remote_path`` is relative to the document library. ``source`` may be a
        local path or raw bytes. The simple upload endpoint supports files up to
        250 MB.
        """
        path = remote_path.strip("/")
        if not path:
            raise ValueError("remote_path must include a file name")

        parent_dir = path.rpartition("/")[0]
        if parent_dir:
            self.create_folder(parent_dir)

        mime_type = (
            content_type or mimetypes.guess_type(path)[0] or "application/octet-stream"
        )
        if isinstance(source, (str, os.PathLike)):
            with open(source, "rb") as file:
                content = file.read()
        elif isinstance(source, bytes):
            content = source
        else:
            raise TypeError("source must be a local path or bytes")

        site_id = self._get_site_id()
        encoded_path = quote(path, safe="/")
        resp = requests.put(
            f"{self._GRAPH_BASE}/sites/{site_id}/drive/root:/{encoded_path}:/content",
            headers={
                "Authorization": f'Bearer {self._auth.get_token("graph")}',
                "Content-Type": mime_type,
            },
            data=content,
            timeout=60,
        )
        resp.raise_for_status()

import os
import mimetypes
import requests
from datetime import datetime
from typing import Optional, Union
from msdev_kit.auth import Auth


class SharePointClient:
    _GRAPH_BASE = 'https://graph.microsoft.com/v1.0'

    def __init__(self, auth: Auth, sp_hostname: str, sp_site_path: str):
        self._auth = auth

        hostname = sp_hostname.replace('https://', '').replace('http://', '').rstrip('/')
        if not hostname.endswith('.sharepoint.com'):
            hostname = f'{hostname}.sharepoint.com'
        self._sp_hostname = hostname

        site_path = sp_site_path.strip('/')
        if not site_path.startswith('sites/'):
            site_path = f'sites/{site_path}'
        self._sp_site_path = site_path

        self._site_id: Optional[str] = None

    def _headers(self) -> dict:
        return {
            'Authorization': f'Bearer {self._auth.get_token("graph")}',
            'Content-Type': 'application/json',
        }

    def _ts(self) -> str:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _get_site_id(self) -> str:
        """Resolve and cache the Graph site ID for this SharePoint site."""
        if self._site_id:
            return self._site_id

        resp = requests.get(
            f'{self._GRAPH_BASE}/sites/{self._sp_hostname}:/{self._sp_site_path}',
            headers=self._headers(),
            params={'$select': 'id'},
            timeout=30,
        )
        resp.raise_for_status()
        self._site_id = resp.json()['id']
        return self._site_id

    def download_file(self, file_path: str, local_dir: str) -> str:
        """Download a file from the site's default document library. Returns local file path."""
        site_id = self._get_site_id()
        local_path = os.path.join(local_dir, os.path.basename(file_path))

        resp = requests.get(
            f'{self._GRAPH_BASE}/sites/{site_id}/drive/root:{file_path}:/content',
            headers={'Authorization': f'Bearer {self._auth.get_token("graph")}'},
            timeout=120,
        )
        resp.raise_for_status()
        print(f'[{self._ts()}] Downloaded: {file_path}')

        with open(local_path, 'wb') as f:
            f.write(resp.content)
        return local_path

    def create_folder(self, folder_path: str):
        """Create a folder and all intermediate folders in the default document library."""
        site_id = self._get_site_id()
        parts = [p for p in folder_path.strip('/').split('/') if p]
        current_path = ''

        for part in parts:
            parent_ref = f'root:{current_path}' if current_path else 'root'
            resp = requests.post(
                f'{self._GRAPH_BASE}/sites/{site_id}/drive/{parent_ref}:/children',
                headers=self._headers(),
                json={
                    'name': part,
                    'folder': {},
                    '@microsoft.graph.conflictBehavior': 'replace',
                },
                timeout=30,
            )
            resp.raise_for_status()
            current_path = f'{current_path}/{part}'

    def upload_file(self, remote_path: str, source: Union[str, bytes], content_type: Optional[str] = None):
        """Upload (or overwrite) a file to the default document library.
        source: local file path (str) or raw bytes."""
        mime_type = content_type or mimetypes.guess_type(remote_path)[0] or 'application/octet-stream'
        site_id = self._get_site_id()
        content = open(source, 'rb').read() if isinstance(source, str) else source

        resp = requests.put(
            f'{self._GRAPH_BASE}/sites/{site_id}/drive/root:{remote_path}:/content',
            headers={
                'Authorization': f'Bearer {self._auth.get_token("graph")}',
                'Content-Type': mime_type,
            },
            data=content,
            timeout=60,
        )
        resp.raise_for_status()

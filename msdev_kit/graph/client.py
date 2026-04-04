import requests
from typing import Optional
from msdev_kit.auth import Auth


class GraphClient:
    _GRAPH_BASE = 'https://graph.microsoft.com/v1.0'

    def __init__(self, auth: Auth):
        self._auth = auth

    def _headers(self) -> dict:
        return {
            'Authorization': f'Bearer {self._auth.get_token("graph")}',
            'Content-Type': 'application/json',
        }

    def get_user_id(self, email: str) -> Optional[str]:
        """Return the Entra object ID of a user by UPN/email, or None if not found.
        Falls back to filtering by the mail field when the UPN lookup returns 404."""
        resp = requests.get(
            f'{self._GRAPH_BASE}/users/{email}',
            headers=self._headers(),
            params={'$select': 'id'},
            timeout=30,
        )
        if resp.status_code != 404:
            resp.raise_for_status()
            return resp.json().get('id')

        resp = requests.get(
            f'{self._GRAPH_BASE}/users',
            headers=self._headers(),
            params={'$filter': f"mail eq '{email}'", '$select': 'id'},
            timeout=30,
        )
        resp.raise_for_status()
        users = resp.json().get('value', [])
        return users[0]['id'] if users else None

    def get_group_id(self, group_name: str) -> Optional[str]:
        """Return the Entra object ID of a security group by displayName, or None."""
        resp = requests.get(
            f'{self._GRAPH_BASE}/groups',
            headers=self._headers(),
            params={'$filter': f"displayName eq '{group_name}'", '$select': 'id,displayName'},
            timeout=30,
        )
        resp.raise_for_status()
        groups = resp.json().get('value', [])
        return groups[0]['id'] if groups else None

    def list_group_members(self, group_id: str) -> list[dict]:
        """List all members of a group with pagination.
        Returns user dicts with id, displayName, mail, userPrincipalName."""
        members = []
        url = f'{self._GRAPH_BASE}/groups/{group_id}/members/microsoft.graph.user'
        params = {'$select': 'id,displayName,mail,userPrincipalName', '$top': '999'}

        while url:
            resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            members.extend(data.get('value', []))
            url = data.get('@odata.nextLink')
            params = None

        return members

    def add_group_member(self, group_id: str, user_id: str):
        """Add user to group. Silently ignores 'already a member' errors."""
        resp = requests.post(
            f'{self._GRAPH_BASE}/groups/{group_id}/members/$ref',
            headers=self._headers(),
            json={'@odata.id': f'{self._GRAPH_BASE}/directoryObjects/{user_id}'},
            timeout=30,
        )
        if resp.status_code == 400 and 'One or more added object references already exist' in resp.text:
            return
        resp.raise_for_status()

    def remove_group_member(self, group_id: str, user_id: str):
        """Remove user from group. Silently ignores 404 (not a member) and 403 (insufficient privileges)."""
        resp = requests.delete(
            f'{self._GRAPH_BASE}/groups/{group_id}/members/{user_id}/$ref',
            headers=self._headers(),
            timeout=30,
        )
        if resp.status_code in (403, 404):
            return
        resp.raise_for_status()

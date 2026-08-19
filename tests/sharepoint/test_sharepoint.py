"""
Unit tests for SharePointClient — all HTTP calls are mocked.
"""

import pytest
from unittest.mock import patch, MagicMock
from msdev_kit.auth import Auth
from msdev_kit.sharepoint import SharePointClient


@pytest.fixture
def auth():
    mock_auth = MagicMock(spec=Auth)
    mock_auth.get_token.return_value = "fake-graph-token"
    return mock_auth


@pytest.fixture
def sp(auth):
    return SharePointClient(auth, sp_hostname="mycompany", sp_site_path="DataTeam")


# ---------------------------------------------------------------------------
# __init__ — hostname and site path normalization
# ---------------------------------------------------------------------------


class TestInit:

    def test_short_hostname_normalized(self, auth):
        sp = SharePointClient(auth, sp_hostname="mycompany", sp_site_path="DataTeam")
        assert sp._sp_hostname == "mycompany.sharepoint.com"

    def test_https_hostname_normalized(self, auth):
        sp = SharePointClient(
            auth,
            sp_hostname="https://mycompany.sharepoint.com",
            sp_site_path="DataTeam",
        )
        assert sp._sp_hostname == "mycompany.sharepoint.com"

    def test_fqdn_hostname_unchanged(self, auth):
        sp = SharePointClient(
            auth, sp_hostname="mycompany.sharepoint.com", sp_site_path="DataTeam"
        )
        assert sp._sp_hostname == "mycompany.sharepoint.com"

    def test_site_path_without_prefix_normalized(self, auth):
        sp = SharePointClient(auth, sp_hostname="mycompany", sp_site_path="DataTeam")
        assert sp._sp_site_path == "sites/DataTeam"

    def test_site_path_with_prefix_unchanged(self, auth):
        sp = SharePointClient(
            auth, sp_hostname="mycompany", sp_site_path="sites/DataTeam"
        )
        assert sp._sp_site_path == "sites/DataTeam"

    def test_site_path_with_leading_slash_normalized(self, auth):
        sp = SharePointClient(
            auth, sp_hostname="mycompany", sp_site_path="/sites/DataTeam"
        )
        assert sp._sp_site_path == "sites/DataTeam"


# ---------------------------------------------------------------------------
# _get_site_id
# ---------------------------------------------------------------------------


class TestGetSiteId:

    @patch("msdev_kit.sharepoint.client.requests.get")
    def test_resolves_site_id(self, mock_get, sp):
        mock_get.return_value = MagicMock(json=lambda: {"id": "site-xyz"})
        assert sp._get_site_id() == "site-xyz"

    @patch("msdev_kit.sharepoint.client.requests.get")
    def test_caches_site_id(self, mock_get, sp):
        mock_get.return_value = MagicMock(json=lambda: {"id": "site-xyz"})
        sp._get_site_id()
        sp._get_site_id()
        assert mock_get.call_count == 1


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------


class TestDownloadFile:

    @patch("msdev_kit.sharepoint.client.requests.get")
    def test_returns_local_path(self, mock_get, sp, tmp_path):
        site_resp = MagicMock()
        site_resp.json.return_value = {"id": "site-xyz"}
        content_resp = MagicMock()
        content_resp.content = b"file content"
        mock_get.side_effect = [site_resp, content_resp]

        result = sp.download_file("/Reports/report.xlsx", str(tmp_path))

        expected = str(tmp_path / "report.xlsx")
        assert result == expected

    @patch("msdev_kit.sharepoint.client.requests.get")
    def test_writes_file_content(self, mock_get, sp, tmp_path):
        site_resp = MagicMock()
        site_resp.json.return_value = {"id": "site-xyz"}
        content_resp = MagicMock()
        content_resp.content = b"file content"
        mock_get.side_effect = [site_resp, content_resp]

        sp.download_file("/Reports/report.xlsx", str(tmp_path))

        assert (tmp_path / "report.xlsx").read_bytes() == b"file content"


# ---------------------------------------------------------------------------
# create_folder
# ---------------------------------------------------------------------------


class TestCreateFolder:

    @patch("msdev_kit.sharepoint.client.requests.get")
    @patch("msdev_kit.sharepoint.client.requests.post")
    def test_single_part(self, mock_post, mock_get, sp):
        mock_get.return_value = MagicMock(json=lambda: {"id": "site-xyz"})
        mock_post.return_value = MagicMock()

        sp.create_folder("/Reports")

        mock_post.assert_called_once_with(
            "https://graph.microsoft.com/v1.0/sites/site-xyz/drive/root/children",
            headers=sp._headers(),
            json={
                "name": "Reports",
                "folder": {},
                "@microsoft.graph.conflictBehavior": "replace",
            },
            timeout=30,
        )

    @patch("msdev_kit.sharepoint.client.requests.get")
    @patch("msdev_kit.sharepoint.client.requests.post")
    def test_multi_part(self, mock_post, mock_get, sp):
        mock_get.return_value = MagicMock(json=lambda: {"id": "site-xyz"})
        mock_post.return_value = MagicMock()

        sp.create_folder("/Reports/2026/Q1")

        assert mock_post.call_args_list == [
            (
                (
                    "https://graph.microsoft.com/v1.0/sites/site-xyz/drive/root/children",
                ),
                {
                    "headers": sp._headers(),
                    "json": {
                        "name": "Reports",
                        "folder": {},
                        "@microsoft.graph.conflictBehavior": "replace",
                    },
                    "timeout": 30,
                },
            ),
            (
                (
                    "https://graph.microsoft.com/v1.0/sites/site-xyz/drive/root:/Reports:/children",
                ),
                {
                    "headers": sp._headers(),
                    "json": {
                        "name": "2026",
                        "folder": {},
                        "@microsoft.graph.conflictBehavior": "replace",
                    },
                    "timeout": 30,
                },
            ),
            (
                (
                    "https://graph.microsoft.com/v1.0/sites/site-xyz/drive/root:/Reports/2026:/children",
                ),
                {
                    "headers": sp._headers(),
                    "json": {
                        "name": "Q1",
                        "folder": {},
                        "@microsoft.graph.conflictBehavior": "replace",
                    },
                    "timeout": 30,
                },
            ),
        ]


# ---------------------------------------------------------------------------
# upload_file
# ---------------------------------------------------------------------------


class TestUploadFile:

    @patch("msdev_kit.sharepoint.client.requests.get")
    @patch("msdev_kit.sharepoint.client.requests.put")
    def test_bytes_source(self, mock_put, mock_get, sp):
        mock_get.return_value = MagicMock(json=lambda: {"id": "site-xyz"})
        mock_put.return_value = MagicMock()

        with patch.object(sp, "create_folder") as mock_create_folder:
            sp.upload_file("/Reports/data.xlsx", b"binary content")
            mock_create_folder.assert_called_once_with("Reports")

        _, kwargs = mock_put.call_args
        assert kwargs["data"] == b"binary content"

    @patch("msdev_kit.sharepoint.client.requests.get")
    @patch("msdev_kit.sharepoint.client.requests.put")
    def test_file_path_source(self, mock_put, mock_get, sp, tmp_path):
        mock_get.return_value = MagicMock(json=lambda: {"id": "site-xyz"})
        mock_put.return_value = MagicMock()

        local_file = tmp_path / "data.xlsx"
        local_file.write_bytes(b"file bytes")

        with patch.object(sp, "create_folder") as mock_create_folder:
            sp.upload_file("/Reports/data.xlsx", str(local_file))
            mock_create_folder.assert_called_once_with("Reports")

        _, kwargs = mock_put.call_args
        assert kwargs["data"] == b"file bytes"

    @patch("msdev_kit.sharepoint.client.requests.get")
    @patch("msdev_kit.sharepoint.client.requests.put")
    def test_content_type_inferred(self, mock_put, mock_get, sp):
        mock_get.return_value = MagicMock(json=lambda: {"id": "site-xyz"})
        mock_put.return_value = MagicMock()

        with patch.object(sp, "create_folder") as mock_create_folder:
            sp.upload_file("/Reports/data.xlsx", b"data")
            mock_create_folder.assert_called_once_with("Reports")

        _, kwargs = mock_put.call_args
        assert "spreadsheet" in kwargs["headers"]["Content-Type"]

    @patch("msdev_kit.sharepoint.client.requests.get")
    @patch("msdev_kit.sharepoint.client.requests.put")
    def test_content_type_explicit(self, mock_put, mock_get, sp):
        mock_get.return_value = MagicMock(json=lambda: {"id": "site-xyz"})
        mock_put.return_value = MagicMock()

        with patch.object(sp, "create_folder") as mock_create_folder:
            sp.upload_file(
                "/Reports/data.xlsx", b"data", content_type="application/octet-stream"
            )
            mock_create_folder.assert_called_once_with("Reports")

        _, kwargs = mock_put.call_args
        assert kwargs["headers"]["Content-Type"] == "application/octet-stream"

    @patch("msdev_kit.sharepoint.client.requests.get")
    @patch("msdev_kit.sharepoint.client.requests.put")
    def test_upload_file_encodes_path(self, mock_put, mock_get, sp):
        mock_get.return_value = MagicMock(json=lambda: {"id": "site-xyz"})
        mock_put.return_value = MagicMock()

        with patch.object(sp, "create_folder") as mock_create_folder:
            sp.upload_file("/General/Usage Metrics/report.pbix", b"data")
            mock_create_folder.assert_called_once_with("General/Usage Metrics")

        mock_put.assert_called_once()
        called_url = mock_put.call_args[0][0]
        assert "General/Usage%20Metrics/report.pbix" in called_url

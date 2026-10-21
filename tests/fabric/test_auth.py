"""Unit tests for scope selection and credential choice."""

import hashlib
import os
import stat
from unittest.mock import MagicMock, patch

import pytest

from msdev_kit.auth import Auth


class TestGetToken:
    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_pbi_scope(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="fake-pbi-token")
        mock_cred_cls.return_value = mock_cred

        auth = Auth("tenant", "client", "secret")

        assert auth.get_token("pbi") == "fake-pbi-token"
        mock_cred.get_token.assert_called_once_with(
            "https://analysis.windows.net/powerbi/api/.default"
        )

    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_fabric_scope(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="fake-fabric-token")
        mock_cred_cls.return_value = mock_cred

        auth = Auth("tenant", "client", "secret")

        assert auth.get_token("fabric") == "fake-fabric-token"
        mock_cred.get_token.assert_called_once_with(
            "https://api.fabric.microsoft.com/.default"
        )

    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_azure_scope(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="fake-azure-token")
        mock_cred_cls.return_value = mock_cred

        auth = Auth("tenant", "client", "secret")

        assert auth.get_token("azure") == "fake-azure-token"
        mock_cred.get_token.assert_called_once_with(
            "https://management.azure.com/.default"
        )

    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_graph_scope(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="fake-graph-token")
        mock_cred_cls.return_value = mock_cred

        auth = Auth("tenant", "client", "secret")

        assert auth.get_token("graph") == "fake-graph-token"
        mock_cred.get_token.assert_called_once_with(
            "https://graph.microsoft.com/.default"
        )

    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_credential_created_once(self, mock_cred_cls):
        mock_cred_cls.return_value.get_token.return_value = MagicMock(token="t")

        auth = Auth("tenant", "client", "secret")
        auth.get_token("pbi")
        auth.get_token("fabric")

        mock_cred_cls.assert_called_once()

    def test_invalid_service_raises(self):
        with patch("msdev_kit.auth.ClientSecretCredential"):
            auth = Auth("tenant", "client", "secret")
            with pytest.raises(ValueError, match="Invalid service"):
                auth.get_token("invalid")


class TestInteractiveAuth:
    @patch("msdev_kit.auth.InteractiveBrowserCredential")
    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_missing_client_secret_configuration_uses_interactive_credential(
        self, mock_client_secret_cls, mock_interactive_cls
    ):
        mock_interactive_cls.return_value.get_token.return_value = MagicMock(
            token="fake-user-token"
        )

        auth = Auth()

        assert auth.get_token("fabric") == "fake-user-token"
        mock_client_secret_cls.assert_not_called()
        mock_interactive_cls.assert_called_once()

    @patch("msdev_kit.auth.TokenCachePersistenceOptions")
    @patch("msdev_kit.auth.InteractiveBrowserCredential")
    def test_get_token_for_user_uses_a_fresh_interactive_credential(
        self, mock_cred_cls, mock_cache_options_cls
    ):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="fake-azure-token")
        mock_cred_cls.return_value = mock_cred
        mock_cache_options = mock_cache_options_cls.return_value

        with patch("msdev_kit.auth.ClientSecretCredential"):
            auth = Auth("tenant", "client", "secret")
            assert auth.get_token_for_user("azure") == "fake-azure-token"

        mock_cred.get_token.assert_called_once_with(
            "https://management.azure.com/.default"
        )
        mock_cache_options_cls.assert_called_once_with(allow_unencrypted_storage=False)
        mock_cred_cls.assert_called_once_with(
            cache_persistence_options=mock_cache_options,
            tenant_id="tenant",
        )

    @patch("msdev_kit.auth.TokenCachePersistenceOptions")
    @patch("msdev_kit.auth.InteractiveBrowserCredential")
    def test_get_token_for_user_can_allow_unencrypted_storage(
        self, mock_cred_cls, mock_cache_options_cls
    ):
        mock_cred_cls.return_value.get_token.return_value = MagicMock(token="token")

        with patch("msdev_kit.auth.ClientSecretCredential"):
            auth = Auth("tenant", "client", "secret")
            assert (
                auth.get_token_for_user("fabric", allow_unencrypted_storage=True)
                == "token"
            )

        mock_cache_options_cls.assert_called_once_with(allow_unencrypted_storage=True)

    def test_get_token_for_user_rejects_unknown_service(self):
        with patch("msdev_kit.auth.InteractiveBrowserCredential"):
            auth = Auth()
            with pytest.raises(ValueError, match="Invalid service"):
                auth.get_token_for_user("invalid")

    @patch("msdev_kit.auth.InteractiveBrowserCredential")
    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_cached_user_first_sign_in_and_reuse(
        self, mock_spn_cls, mock_browser_cls, tmp_path
    ):
        credential = mock_browser_cls.return_value
        credential.get_token.return_value = MagicMock(token="user-token")
        record = credential.authenticate.return_value
        record.username = "person@example.com"
        record.serialize.return_value = "serialized record"
        auth = Auth("tenant-one", "spn-client", "secret")
        auth._user_cache_dir = tmp_path / "auth"

        assert (
            auth.get_token_for_user("fabric", account="person@example.com")
            == "user-token"
        )
        (record_path,) = auth._user_cache_dir.glob("*.json")
        assert record_path.read_text() == "serialized record"
        assert mock_browser_cls.call_args.kwargs["tenant_id"] == "tenant-one"
        assert mock_browser_cls.call_args.kwargs["login_hint"] == "person@example.com"
        assert "client_id" not in mock_browser_cls.call_args.kwargs
        credential.authenticate.assert_called_once_with(
            scopes=["https://api.fabric.microsoft.com/.default"]
        )
        if os.name != "nt":
            assert stat.S_IMODE(record_path.stat().st_mode) == 0o600
            assert stat.S_IMODE(auth._user_cache_dir.stat().st_mode) == 0o700

        credential.authenticate.reset_mock()
        with patch(
            "msdev_kit.auth.AuthenticationRecord.deserialize", return_value=record
        ) as deserialize:
            assert (
                auth.get_token_for_user("pbi", account="person@example.com")
                == "user-token"
            )
        deserialize.assert_called_once_with("serialized record")
        assert mock_browser_cls.call_args.kwargs["authentication_record"] is record
        assert mock_browser_cls.call_args.kwargs["tenant_id"] == "tenant-one"
        credential.authenticate.assert_not_called()
        mock_spn_cls.assert_called_once()

    @patch("msdev_kit.auth.InteractiveBrowserCredential")
    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_cached_user_separates_tenants_and_accounts(
        self, mock_spn_cls, mock_browser_cls, tmp_path
    ):
        credential = mock_browser_cls.return_value
        credential.get_token.return_value = MagicMock(token="token")
        record = credential.authenticate.return_value
        record.serialize.return_value = "record"
        auth_one = Auth("tenant-one", "spn-client", "secret")
        auth_two = Auth("tenant-two", "spn-client", "secret")
        auth_one._user_cache_dir = auth_two._user_cache_dir = tmp_path

        record.username = "person@example.com"
        auth_one.get_token_for_user(account="person@example.com")
        first_cache_name = mock_browser_cls.call_args.kwargs[
            "cache_persistence_options"
        ].name
        record.username = "other@example.com"
        auth_one.get_token_for_user(account="other@example.com")
        second_cache_name = mock_browser_cls.call_args.kwargs[
            "cache_persistence_options"
        ].name
        record.username = "person@example.com"
        auth_two.get_token_for_user(account="person@example.com")
        third_cache_name = mock_browser_cls.call_args.kwargs[
            "cache_persistence_options"
        ].name

        assert len(set((first_cache_name, second_cache_name, third_cache_name))) == 3
        assert len(list(tmp_path.glob("*.json"))) == 3
        assert [
            call.kwargs["tenant_id"] for call in mock_browser_cls.call_args_list
        ] == ["tenant-one", "tenant-one", "tenant-two"]

    @patch("msdev_kit.auth.InteractiveBrowserCredential")
    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_cached_user_rejects_a_different_signed_in_account(
        self, mock_spn_cls, mock_browser_cls, tmp_path
    ):
        record = mock_browser_cls.return_value.authenticate.return_value
        record.username = "other@example.com"
        auth = Auth("tenant-one", "spn-client", "secret")
        auth._user_cache_dir = tmp_path

        with pytest.raises(ValueError, match="does not match"):
            auth.get_token_for_user(account="person@example.com")

        assert list(tmp_path.glob("*.json")) == []

    @patch("msdev_kit.auth.AuthenticationRecord.deserialize")
    @patch("msdev_kit.auth.InteractiveBrowserCredential")
    @patch("msdev_kit.auth.ClientSecretCredential")
    def test_cached_user_rejects_a_different_stored_account(
        self, mock_spn_cls, mock_browser_cls, mock_deserialize, tmp_path
    ):
        auth = Auth("tenant-one", "spn-client", "secret")
        auth._user_cache_dir = tmp_path
        mock_browser_cls.reset_mock()
        mock_deserialize.return_value.username = "other@example.com"
        digest = hashlib.sha256(b"tenant-one\0person@example.com").hexdigest()
        (auth._user_cache_dir / f"{digest}.json").write_text("serialized record")

        with pytest.raises(ValueError, match="belongs to another account"):
            auth.get_token_for_user(account="person@example.com")

        mock_browser_cls.assert_not_called()

    def test_cached_user_requires_explicit_tenant_and_account(self, tmp_path):
        with patch("msdev_kit.auth.InteractiveBrowserCredential"):
            auth = Auth()
            auth._user_cache_dir = tmp_path
            with pytest.raises(ValueError, match="tenant_id"):
                auth.get_token_for_user(account="person@example.com")
            with pytest.raises(ValueError, match="account"):
                auth.get_token_for_user(account=" ")
        assert list(tmp_path.iterdir()) == []

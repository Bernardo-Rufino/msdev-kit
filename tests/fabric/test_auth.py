"""Unit tests for scope selection and credential choice."""

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

    @patch("msdev_kit.auth.InteractiveBrowserCredential")
    def test_get_token_for_user_uses_a_fresh_interactive_credential(
        self, mock_cred_cls
    ):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="fake-azure-token")
        mock_cred_cls.return_value = mock_cred

        with patch("msdev_kit.auth.ClientSecretCredential"):
            auth = Auth("tenant", "client", "secret")
            assert auth.get_token_for_user("azure") == "fake-azure-token"

        mock_cred.get_token.assert_called_once_with(
            "https://management.azure.com/.default"
        )

    def test_get_token_for_user_rejects_unknown_service(self):
        with patch("msdev_kit.auth.InteractiveBrowserCredential"):
            auth = Auth()
            with pytest.raises(ValueError, match="Invalid service"):
                auth.get_token_for_user("invalid")

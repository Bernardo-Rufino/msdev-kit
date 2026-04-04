"""
Unit tests for msdev_kit Auth — scope selection, graph scope, and validation.
These tests mock Azure credentials and do not perform real authentication.
"""

import pytest
from unittest.mock import patch, MagicMock
from msdev_kit.auth import Auth


class TestGetToken:

    @patch('msdev_kit.auth.ClientSecretCredential')
    def test_pbi_scope(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token='fake-pbi-token')
        mock_cred_cls.return_value = mock_cred

        auth = Auth('tenant', 'client', 'secret')
        token = auth.get_token('pbi')

        assert token == 'fake-pbi-token'
        mock_cred.get_token.assert_called_once_with(
            'https://analysis.windows.net/powerbi/api/.default'
        )

    @patch('msdev_kit.auth.ClientSecretCredential')
    def test_fabric_scope(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token='fake-fabric-token')
        mock_cred_cls.return_value = mock_cred

        auth = Auth('tenant', 'client', 'secret')
        token = auth.get_token('fabric')

        assert token == 'fake-fabric-token'
        mock_cred.get_token.assert_called_once_with(
            'https://api.fabric.microsoft.com/.default'
        )

    @patch('msdev_kit.auth.ClientSecretCredential')
    def test_azure_scope(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token='fake-azure-token')
        mock_cred_cls.return_value = mock_cred

        auth = Auth('tenant', 'client', 'secret')
        token = auth.get_token('azure')

        assert token == 'fake-azure-token'
        mock_cred.get_token.assert_called_once_with(
            'https://management.azure.com/.default'
        )

    @patch('msdev_kit.auth.ClientSecretCredential')
    def test_graph_scope(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token='fake-graph-token')
        mock_cred_cls.return_value = mock_cred

        auth = Auth('tenant', 'client', 'secret')
        token = auth.get_token('graph')

        assert token == 'fake-graph-token'
        mock_cred.get_token.assert_called_once_with(
            'https://graph.microsoft.com/.default'
        )

    @patch('msdev_kit.auth.ClientSecretCredential')
    def test_credential_created_once(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token='t')
        mock_cred_cls.return_value = mock_cred

        auth = Auth('tenant', 'client', 'secret')
        auth.get_token('pbi')
        auth.get_token('fabric')

        mock_cred_cls.assert_called_once()

    def test_invalid_service_raises(self):
        with patch('msdev_kit.auth.ClientSecretCredential'):
            auth = Auth('tenant', 'client', 'secret')
            with pytest.raises(ValueError, match="Invalid service"):
                auth.get_token('invalid')


class TestGetTokenForUser:

    @patch('msdev_kit.auth.InteractiveBrowserCredential')
    def test_azure_scope(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token='fake-azure-token')
        mock_cred_cls.return_value = mock_cred

        with patch('msdev_kit.auth.ClientSecretCredential'):
            auth = Auth('tenant', 'client', 'secret')
            token = auth.get_token_for_user('azure')

        assert token == 'fake-azure-token'
        mock_cred.get_token.assert_called_once_with(
            'https://management.azure.com/.default'
        )

    def test_invalid_service_raises(self):
        with patch('msdev_kit.auth.ClientSecretCredential'):
            auth = Auth('tenant', 'client', 'secret')
            with pytest.raises(ValueError, match="Invalid service"):
                auth.get_token_for_user('invalid')

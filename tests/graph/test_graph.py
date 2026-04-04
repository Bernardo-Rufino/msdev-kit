"""
Unit tests for GraphClient — all HTTP calls are mocked.
"""

import pytest
from unittest.mock import patch, MagicMock
from msdev_kit.auth import Auth
from msdev_kit.graph import GraphClient


@pytest.fixture
def auth():
    mock_auth = MagicMock(spec=Auth)
    mock_auth.get_token.return_value = 'fake-graph-token'
    return mock_auth


@pytest.fixture
def graph(auth):
    return GraphClient(auth)


# ---------------------------------------------------------------------------
# get_user_id
# ---------------------------------------------------------------------------

class TestGetUserId:

    @patch('msdev_kit.graph.client.requests.get')
    def test_found_by_upn(self, mock_get, graph):
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = {'id': 'user-123'}

        assert graph.get_user_id('user@company.com') == 'user-123'

    @patch('msdev_kit.graph.client.requests.get')
    def test_404_falls_back_to_mail_found(self, mock_get, graph):
        not_found = MagicMock(status_code=404)
        found = MagicMock(status_code=200)
        found.json.return_value = {'value': [{'id': 'user-456'}]}
        mock_get.side_effect = [not_found, found]

        assert graph.get_user_id('user@company.com') == 'user-456'

    @patch('msdev_kit.graph.client.requests.get')
    def test_not_found_returns_none(self, mock_get, graph):
        not_found = MagicMock(status_code=404)
        empty = MagicMock(status_code=200)
        empty.json.return_value = {'value': []}
        mock_get.side_effect = [not_found, empty]

        assert graph.get_user_id('missing@company.com') is None


# ---------------------------------------------------------------------------
# get_group_id
# ---------------------------------------------------------------------------

class TestGetGroupId:

    @patch('msdev_kit.graph.client.requests.get')
    def test_found(self, mock_get, graph):
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = {'value': [{'id': 'grp-abc'}]}

        assert graph.get_group_id('Data Team') == 'grp-abc'

    @patch('msdev_kit.graph.client.requests.get')
    def test_not_found_returns_none(self, mock_get, graph):
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = {'value': []}

        assert graph.get_group_id('Ghost Group') is None


# ---------------------------------------------------------------------------
# list_group_members
# ---------------------------------------------------------------------------

class TestListGroupMembers:

    @patch('msdev_kit.graph.client.requests.get')
    def test_single_page(self, mock_get, graph):
        page = MagicMock(status_code=200)
        page.json.return_value = {
            'value': [{'id': 'u1', 'displayName': 'Alice', 'mail': 'a@c.com', 'userPrincipalName': 'a@c.com'}],
        }
        mock_get.return_value = page

        members = graph.list_group_members('grp-abc')

        assert len(members) == 1
        assert members[0]['id'] == 'u1'

    @patch('msdev_kit.graph.client.requests.get')
    def test_pagination(self, mock_get, graph):
        page1 = MagicMock(status_code=200)
        page1.json.return_value = {
            'value': [{'id': 'u1'}],
            '@odata.nextLink': 'https://graph.microsoft.com/v1.0/next',
        }
        page2 = MagicMock(status_code=200)
        page2.json.return_value = {'value': [{'id': 'u2'}]}
        mock_get.side_effect = [page1, page2]

        members = graph.list_group_members('grp-abc')

        assert len(members) == 2
        assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# add_group_member
# ---------------------------------------------------------------------------

class TestAddGroupMember:

    @patch('msdev_kit.graph.client.requests.post')
    def test_success(self, mock_post, graph):
        mock_post.return_value = MagicMock(status_code=204)
        graph.add_group_member('grp-abc', 'user-123')
        mock_post.assert_called_once()

    @patch('msdev_kit.graph.client.requests.post')
    def test_already_member_silently_ignored(self, mock_post, graph):
        mock_post.return_value = MagicMock(
            status_code=400,
            text='One or more added object references already exist',
        )
        graph.add_group_member('grp-abc', 'user-123')  # must not raise
        mock_post.assert_called_once()


# ---------------------------------------------------------------------------
# remove_group_member
# ---------------------------------------------------------------------------

class TestRemoveGroupMember:

    @patch('msdev_kit.graph.client.requests.delete')
    def test_success(self, mock_delete, graph):
        mock_delete.return_value = MagicMock(status_code=204)
        graph.remove_group_member('grp-abc', 'user-123')
        mock_delete.assert_called_once()

    @patch('msdev_kit.graph.client.requests.delete')
    def test_404_silently_ignored(self, mock_delete, graph):
        mock_delete.return_value = MagicMock(status_code=404)
        graph.remove_group_member('grp-abc', 'user-123')  # must not raise

    @patch('msdev_kit.graph.client.requests.delete')
    def test_403_silently_ignored(self, mock_delete, graph):
        mock_delete.return_value = MagicMock(status_code=403)
        graph.remove_group_member('grp-abc', 'user-123')  # must not raise

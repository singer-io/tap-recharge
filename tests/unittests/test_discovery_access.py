"""
Unit tests for discovery access check logic.
Tests that streams returning 403 are excluded from the catalog.
"""

import unittest
from unittest.mock import patch, MagicMock

from tap_recharge.client import RechargeForbiddenError
from tap_recharge.discover import discover, _apply_access_checks
from tap_recharge.streams import STREAMS


class TestCheckAccess(unittest.TestCase):
    """Tests for BaseStream.check_access()"""

    def test_check_access_returns_true_on_success(self):
        """check_access returns True when the API call succeeds."""
        mock_client = MagicMock()
        mock_client.get.return_value = {'addresses': []}

        stream_cls = STREAMS['addresses']
        stream = stream_cls(client=mock_client)
        self.assertTrue(stream.check_access())
        mock_client.get.assert_called_once()

    def test_check_access_returns_false_on_forbidden(self):
        """check_access returns False when a 403 error is raised."""
        mock_client = MagicMock()
        mock_client.get.side_effect = RechargeForbiddenError("HTTP-error-code: 403, Error: Forbidden")

        stream_cls = STREAMS['addresses']
        stream = stream_cls(client=mock_client)
        self.assertFalse(stream.check_access())

    def test_check_access_logs_warning_on_forbidden(self):
        """check_access logs the stream id and error detail when a 403 is raised."""
        mock_client = MagicMock()
        error_message = "HTTP-error-code: 403, Error: Forbidden"
        mock_client.get.side_effect = RechargeForbiddenError(error_message)

        stream_cls = STREAMS['addresses']
        stream = stream_cls(client=mock_client)

        with self.assertLogs(level='WARNING') as log:
            stream.check_access()

        self.assertEqual(len(log.output), 1)
        warning = log.output[0]
        self.assertIn('addresses', warning)
        self.assertIn(error_message, warning)


class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks()"""

    @patch('tap_recharge.discover.STREAMS')
    def test_all_streams_accessible(self, mock_streams):
        """When all streams are accessible, no streams are removed."""
        mock_client = MagicMock()

        # Create mock stream classes that pass access checks
        mock_stream_cls = MagicMock()
        mock_stream_cls.parent = None
        mock_stream_instance = MagicMock()
        mock_stream_instance.check_access.return_value = True
        mock_stream_cls.return_value = mock_stream_instance

        mock_streams.items.return_value = [('stream_a', mock_stream_cls), ('stream_b', mock_stream_cls)]

        schemas = {'stream_a': {}, 'stream_b': {}}
        field_metadata = {'stream_a': [], 'stream_b': []}

        _apply_access_checks(mock_client, schemas, field_metadata)

        self.assertIn('stream_a', schemas)
        self.assertIn('stream_b', schemas)

    @patch('tap_recharge.discover.STREAMS')
    def test_partial_access(self, mock_streams):
        """When some streams are inaccessible, they are excluded."""
        mock_client = MagicMock()

        # stream_a is accessible
        mock_stream_a_cls = MagicMock()
        mock_stream_a_cls.parent = None
        mock_stream_a_instance = MagicMock()
        mock_stream_a_instance.check_access.return_value = True
        mock_stream_a_cls.return_value = mock_stream_a_instance

        # stream_b is NOT accessible
        mock_stream_b_cls = MagicMock()
        mock_stream_b_cls.parent = None
        mock_stream_b_instance = MagicMock()
        mock_stream_b_instance.check_access.return_value = False
        mock_stream_b_cls.return_value = mock_stream_b_instance

        mock_streams.items.return_value = [('stream_a', mock_stream_a_cls), ('stream_b', mock_stream_b_cls)]

        schemas = {'stream_a': {}, 'stream_b': {}}
        field_metadata = {'stream_a': [], 'stream_b': []}

        _apply_access_checks(mock_client, schemas, field_metadata)

        self.assertIn('stream_a', schemas)
        self.assertNotIn('stream_b', schemas)
        self.assertIn('stream_a', field_metadata)
        self.assertNotIn('stream_b', field_metadata)

    @patch('tap_recharge.discover.STREAMS')
    def test_no_streams_accessible_raises_error(self, mock_streams):
        """When no parent streams are accessible, raise RechargeForbiddenError."""
        mock_client = MagicMock()

        mock_stream_cls = MagicMock()
        mock_stream_cls.parent = None
        mock_stream_instance = MagicMock()
        mock_stream_instance.check_access.return_value = False
        mock_stream_cls.return_value = mock_stream_instance

        mock_streams.items.return_value = [('stream_a', mock_stream_cls), ('stream_b', mock_stream_cls)]

        schemas = {'stream_a': {}, 'stream_b': {}}
        field_metadata = {'stream_a': [], 'stream_b': []}

        with self.assertRaises(RechargeForbiddenError):
            _apply_access_checks(mock_client, schemas, field_metadata)



class TestDiscoverWithClient(unittest.TestCase):
    """Integration test for discover() with access checks."""

    @patch('tap_recharge.discover._apply_access_checks')
    @patch('tap_recharge.discover.get_schemas')
    def test_discover_calls_access_checks_with_client(self, mock_get_schemas, mock_access_checks):
        """discover() calls _apply_access_checks when client is provided."""
        mock_get_schemas.return_value = ({'addresses': {'type': 'object', 'properties': {}}},
                                          {'addresses': []})
        mock_client = MagicMock()

        catalog = discover(mock_client)
        mock_access_checks.assert_called_once()

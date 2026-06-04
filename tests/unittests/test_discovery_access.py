"""
Unit tests for discovery access check logic.
Tests that streams returning 403 are excluded from the catalog.
"""

import unittest
from unittest.mock import patch, MagicMock

from tap_recharge.client import RechargeForbiddenError
from tap_recharge.discover import discover, _apply_access_checks, _prune_inaccessible_children
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

    def test_check_access_child_stream_always_true(self):
        """check_access returns True for child streams without making API call."""
        mock_client = MagicMock()

        # Create a mock child stream class with a parent
        from tap_recharge.streams import BaseStream
        class ChildStream(BaseStream):
            tap_stream_id = 'child_test'
            parent = 'addresses'
            path = 'child'

        stream = ChildStream(client=mock_client)
        self.assertTrue(stream.check_access())
        mock_client.get.assert_not_called()

    def test_check_access_raises_valueerror_without_client(self):
        """check_access raises ValueError when called on a parent stream without a client."""
        stream_cls = STREAMS['addresses']
        stream = stream_cls(client=None)
        with self.assertRaises(ValueError) as ctx:
            stream.check_access()
        self.assertIn("Recharge client is required", str(ctx.exception))


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
        mock_streams.values.return_value = [mock_stream_cls, mock_stream_cls]

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
        mock_streams.values.return_value = [mock_stream_a_cls, mock_stream_b_cls]

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
        mock_streams.values.return_value = [mock_stream_cls, mock_stream_cls]

        schemas = {'stream_a': {}, 'stream_b': {}}
        field_metadata = {'stream_a': [], 'stream_b': []}

        with self.assertRaises(RechargeForbiddenError):
            _apply_access_checks(mock_client, schemas, field_metadata)


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Tests for _prune_inaccessible_children()"""

    @patch('tap_recharge.discover.STREAMS')
    def test_child_excluded_when_parent_missing(self, mock_streams):
        """Child stream is removed when its parent is not in schemas."""
        mock_child_cls = MagicMock()
        mock_child_cls.parent = 'parent_stream'

        mock_streams.items.return_value = [('child_stream', mock_child_cls)]

        schemas = {'child_stream': {}}
        field_metadata = {'child_stream': []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn('child_stream', schemas)
        self.assertNotIn('child_stream', field_metadata)

    @patch('tap_recharge.discover.STREAMS')
    def test_child_kept_when_parent_present(self, mock_streams):
        """Child stream is kept when its parent is in schemas."""
        mock_child_cls = MagicMock()
        mock_child_cls.parent = 'parent_stream'

        mock_streams.items.return_value = [('child_stream', mock_child_cls)]

        schemas = {'parent_stream': {}, 'child_stream': {}}
        field_metadata = {'parent_stream': [], 'child_stream': []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn('child_stream', schemas)
        self.assertIn('child_stream', field_metadata)

    @patch('tap_recharge.discover.STREAMS')
    def test_child_excluded_when_parent_is_class_reference(self, mock_streams):
        """Child stream is removed when parent is a class with tap_stream_id not in schemas."""
        mock_parent_cls = MagicMock()
        mock_parent_cls.tap_stream_id = 'parent_stream'

        mock_child_cls = MagicMock()
        mock_child_cls.parent = mock_parent_cls

        mock_streams.items.return_value = [('child_stream', mock_child_cls)]

        schemas = {'child_stream': {}}
        field_metadata = {'child_stream': []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn('child_stream', schemas)
        self.assertNotIn('child_stream', field_metadata)


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

    @patch('tap_recharge.discover._apply_access_checks')
    @patch('tap_recharge.discover.get_schemas')
    def test_discover_skips_access_checks_without_client(self, mock_get_schemas, mock_access_checks):
        """discover() skips _apply_access_checks when client is None."""
        mock_get_schemas.return_value = ({'addresses': {'type': 'object', 'properties': {}}},
                                          {'addresses': []})

        catalog = discover(client=None)
        mock_access_checks.assert_not_called()


if __name__ == '__main__':
    unittest.main()

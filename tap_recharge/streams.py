"""
This module defines the stream classes and their individual sync logic.
"""

import datetime

from typing import Iterator

import singer
from singer import Transformer, utils, metrics, bookmarks

from tap_recharge.client import RechargeClient


LOGGER = singer.get_logger()

MAX_PAGE_LIMIT = 250


def get_recharge_bookmark(
        state: dict,
        tap_stream_id: str,
        default: str = None) -> str:
    """
    Retrieves the bookmarks for a stream from the state dict.

    :param state: The dict of the current state.
    :param tap_stream_id: The stream for which to get the bookmark.
    :return: Bookmark datetime string for stream.
    """
    return state.get('bookmarks', {}).get(tap_stream_id, default)

def write_recharge_bookmark(
        state: dict,
        tap_stream_id: str,
        value: str) -> dict:
    """
    Writes bookmark for a stream in the taps original structure:
        { "bookmarks": { "tap_stream_id": "value"} }

    :param state: The dict of the current state.
    :param tap_stream_id: The stream for which to write the bookmark.
    :param value: The bookmark datetime string.
    :return: New state dict.
    """
    state = bookmarks.ensure_bookmark_path(state, ['bookmarks'])
    state['bookmarks'][tap_stream_id] = value

    return state

class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used to extract records from external source
    """
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    path = None
    params = {}
    parent = None
    data_key = None

    def __init__(self, client: RechargeClient):
        self.client = client

    def get_records(
            self,
            bookmark_datetime: datetime = None,
            is_parent: bool = False) -> list:
        """
        Returns a list of records for that stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require "
                                  "`get_records` implementation")

    def get_parent_data(self, bookmark_datetime: datetime = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :return: A list of records
        """
        # pylint: disable=not-callable
        parent = self.parent(self.client)
        return parent.get_records(bookmark_datetime, is_parent=True)


# pylint: disable=abstract-method
class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'

    # pylint: disable=too-many-arguments
    def sync(
            self,
            state: dict,
            stream_schema: dict,
            stream_metadata: dict,
            config: dict,
            transformer: Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        start_date = get_recharge_bookmark(
            state,
            self.tap_stream_id,
            config['start_date'])
        bookmark_datetime = utils.strptime_to_utc(start_date)
        max_datetime = bookmark_datetime

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config, bookmark_datetime):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)

                record_datetime = utils.strptime_to_utc(transformed_record[self.replication_key])

                if record_datetime >= bookmark_datetime:
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_datetime = max(record_datetime, bookmark_datetime)

            bookmark_date = utils.strftime(max_datetime)

        state = write_recharge_bookmark(
            state,
            self.tap_stream_id,
            bookmark_date)

        singer.write_state(state)

        return state


class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'

    # pylint: disable=too-many-arguments
    def sync(
            self,
            state: dict,
            stream_schema: dict,
            stream_metadata: dict,
            config: dict,
            transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config):
                transformed_record = transformer.transform(
                    record,
                    stream_schema,
                    stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        singer.write_state(state)

        return state


class PageBasedPagingStream(IncrementalStream):
    """
    A generic page based pagination implementation for the Recharge API.

    Docs: https://developer.rechargepayments.com/?python#page-based-pagination
    """

    def get_records(
            self,
            bookmark_datetime: datetime = None,
            is_parent: bool = False) -> Iterator[list]:
        page = 1
        self.params.update({
            'limit': MAX_PAGE_LIMIT,
            'page': page
            })
        result_size = MAX_PAGE_LIMIT

        while result_size == MAX_PAGE_LIMIT:
            records, _ = self.client.get(self.path, params=self.params)

            result_size = len(records.get(self.data_key))
            page += 1
            self.params.update({'page': page})

            yield from records.get(self.data_key)


class CursorPagingStream(IncrementalStream):
    """
    A generic cursor pagination implemantation for the Recharge API.

    Docs: https://developer.rechargepayments.com/?python#cursor-pagination
    """

    def get_records(
            self,
            bookmark_datetime: datetime = None,
            is_parent: bool = False) -> Iterator[list]:
        self.params.update({'limit': MAX_PAGE_LIMIT})
        paging = True
        path = self.path
        url = None

        while paging:
            records, links = self.client.get(path, url=url, params=self.params)

            if links.get('next'):
                path = None
                self.params = None
                url = links.get('next', {}).get('url')
            else:
                paging = False

            yield from records.get(self.data_key)


class Addresses(CursorPagingStream):
    """
    Retrieves addresses from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-addresses
    """
    tap_stream_id = 'addresses'
    key_properties = ['id']
    path = 'addresses'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'addresses'


class Charges(CursorPagingStream):
    """
    Retrieves charges from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-charges
    """
    tap_stream_id = 'charges'
    key_properties = ['id']
    path = 'charges'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'charges'


class Collections(CursorPagingStream):
    """
    Retrieves collections from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-collections
    """
    tap_stream_id = 'collections'
    key_properties = ['id']
    path = 'collections'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'collections'


class Customers(PageBasedPagingStream):
    """
    Retrieves customers from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-customers
    """
    tap_stream_id = 'customers'
    key_properties = ['id']
    path = 'customers'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'customers'


class Discounts(CursorPagingStream):
    """
    Retrieves discounts from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-discounts
    """
    tap_stream_id = 'discounts'
    key_properties = ['id']
    path = 'discounts'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'discounts'


class MetafieldsStore(PageBasedPagingStream):
    """
    Retrieves store metafields from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-metafields
    """
    tap_stream_id = 'metafields_store'
    key_properties = ['id']
    path = 'metafields'
    replication_key = 'updated_at' # pseudo-incremental; doesn't support `updated_at_min` param
    valid_replication_keys = ['updated_at']
    params = {
        'sort_by': f'{replication_key}-asc',
        'owner_resource': 'store'
        }
    data_key = 'metafields'


class MetafieldsCustomer(PageBasedPagingStream):
    """
    Retrieves customer metafields from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-metafields
    """
    tap_stream_id = 'metafields_customer'
    key_properties = ['id']
    path = 'metafields'
    replication_key = 'updated_at' # pseudo-incremental; doesn't support `updated_at_min` param
    valid_replication_keys = ['updated_at']
    params = {
        'sort_by': f'{replication_key}-asc',
        'owner_resource': 'customer'
        }
    data_key = 'metafields'


class MetafieldsSubscription(PageBasedPagingStream):
    """
    Retrieves subscription metafields from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-metafields
    """
    tap_stream_id = 'metafields_subscription'
    key_properties = ['id']
    path = 'metafields'
    replication_key = 'updated_at' # pseudo-incremental; doesn't support `updated_at_min` param
    valid_replication_keys = ['updated_at']
    params = {
        'sort_by': f'{replication_key}-asc',
        'owner_resource': 'subscription'
        }
    data_key = 'metafields'


class Onetimes(PageBasedPagingStream):
    """
    Retrieves non-recurring line items on queued orders from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-onetimes
    """
    tap_stream_id = 'onetimes'
    key_properties = ['id']
    path = 'onetimes'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'onetimes'


class Orders(CursorPagingStream):
    """
    Retrieves orders from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-orders
    """
    tap_stream_id = 'orders'
    key_properties = ['id']
    path = 'orders'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'orders'


class Products(CursorPagingStream):
    """
    Retrieves products from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-products
    """
    tap_stream_id = 'products'
    key_properties = ['id']
    path = 'products'
    replication_key = 'updated_at' # pseudo-incremental; doesn't support `updated_at_min` param
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'products'


class Shop(FullTableStream):
    """
    Retrieves basic info about your store setup from the Recharge API.

    Docs: https://developer.rechargepayments.com/#shop
    """
    tap_stream_id = 'shop'
    key_properties = ['id']
    path = 'shop'
    data_key = 'shop'

    def get_records(
            self,
            bookmark_datetime: datetime = None,
            is_parent: bool = False) -> Iterator[list]:
        records, _ = self.client.get(self.path)

        return [records.get(self.data_key)]


class Subscriptions(CursorPagingStream):
    """
    Retrieves subscriptions from the Recharge API.

    Docs: https://developer.rechargepayments.com/#list-subscriptions
    """
    tap_stream_id = 'subscriptions'
    key_properties = ['id']
    path = 'subscriptions'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'subscriptions'


STREAMS = {
    'addresses': Addresses,
    'charges': Charges,
    'collections': Collections,
    'customers': Customers,
    'discounts': Discounts,
    'metafields_store': MetafieldsStore,
    'metafields_customer': MetafieldsCustomer,
    'metafields_subscription': MetafieldsSubscription,
    'onetimes': Onetimes,
    'orders': Orders,
    'products': Products,
    'shop': Shop,
    'subscriptions': Subscriptions
}

"""
This module defines the stream classes and their individual sync logic.
"""

import datetime

from typing import Iterator

import singer
from singer import Transformer, utils, metrics

from tap_recharge.client import RechargeClient


LOGGER = singer.get_logger()


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

    def get_records(self,
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
    def sync(self,
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
        start_date = singer.get_bookmark(state,
                                        self.tap_stream_id,
                                        self.replication_key,
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

        state = singer.write_bookmark(state,
                                    self.tap_stream_id,
                                    self.replication_key,
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
    def sync(self,
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
                transformed_record = transformer.transform(record,
                                                            stream_schema,
                                                            stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        singer.write_state(state)

        return state


class Addresses(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class Charges(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class Collections(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class Customers(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class Discounts(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class MetafieldsStore(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class MetafieldsCustomer(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class MetafieldsSubscription(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class Onetimes(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class Orders(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class Products(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


class Shop(FullTableStream):
    """
    Retrieves basic info about your store setup from the Recharge API.

    Docs: https://developer.rechargepayments.com/#shop
    """
    tap_stream_id = 'shop'
    key_properties = ['id']
    path = 'shop'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'shop'

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        return [records.get(self.data_key)]


class Subscriptions(IncrementalStream):
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

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path, params=self.params)

        yield from records.get(self.data_key)


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

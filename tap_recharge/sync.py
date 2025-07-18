import singer
from singer import Transformer, Catalog, metadata

from tap_recharge.client import RechargeClient
from tap_recharge.streams import STREAMS

LOGGER = singer.get_logger()

def sync(
        client: RechargeClient,
        config: dict,
        state: dict,
        catalog: Catalog) -> dict:
    """Sync data from tap source"""

    is_product_selected = False
    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client)
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info('Starting sync for stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            state = stream_obj.sync(
                state,
                stream_schema,
                stream_metadata,
                config,
                transformer)
            singer.write_state(state)

            if "products" == tap_stream_id:
                is_product_selected = True
    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)

    if is_product_selected:
        raise Exception(
            "Recharge plans to deprecate `products` stream by June 30, 2025. " \
            "It is recommended to use the `plans` stream instead to achieve equivalent functionality."
        )

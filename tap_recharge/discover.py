import singer
from singer.catalog import Catalog
from tap_recharge.client import RechargeForbiddenError
from tap_recharge.schema import get_schemas
from tap_recharge.streams import STREAMS

LOGGER = singer.get_logger()


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """
    Probe each parent stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.
    Raises RechargeForbiddenError if no parent streams are accessible.
    """
    inaccessible_streams = [
        stream_name
        for stream_name, stream_cls in STREAMS.items()
        if stream_name in schemas
        and not stream_cls.parent
        and not stream_cls(client=client).check_access()
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    if not schemas:
        raise RechargeForbiddenError(
            "No streams are accessible. Ensure the credentials have read permission for at least one stream."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "These streams have been excluded due to HTTP-Error-Code:403 Forbidden: %s",
            ", ".join(inaccessible_streams),
        )


def discover(client):
    """
    Constructs a singer Catalog object based on the schemas and metadata.
    Access to each stream is verified using the provided client and streams
    that the credentials cannot read are excluded from the returned catalog.
    """
    schemas, field_metadata = get_schemas()

    _apply_access_checks(client, schemas, field_metadata)

    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = field_metadata[schema_name]

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': schema_meta
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({'streams': streams})

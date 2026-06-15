import singer
from singer.catalog import Catalog
from tap_recharge.client import RechargeForbiddenError
from tap_recharge.schema import get_schemas
from tap_recharge.streams import STREAMS

LOGGER = singer.get_logger()


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas and field_metadata in place.
    """
    for name, stream_cls in list(STREAMS.items()):
        if name in schemas and stream_cls.parent and stream_cls.parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name, stream_cls.parent,
            )
            schemas.pop(name, None)
            field_metadata.pop(name, None)


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

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise RechargeForbiddenError(
            "HTTP-error-code: 403, Error: Credentials lack read access to all supported streams."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "These streams have been excluded due to 403 Forbidden: %s",
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

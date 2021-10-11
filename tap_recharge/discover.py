from singer.catalog import Catalog
from tap_recharge.schema import get_schemas


def discover():
    """
    Constructs a singer Catalog object based on the schemas and metadata.
    """
    schemas, field_metadata = get_schemas()
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

import os
import json

from singer import metadata
from tap_recharge.streams import STREAMS


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    """
    Loads the schemas defined for the tap.

    This function iterates through the STREAMS dictionary which contains
    a mapping of the stream name and its corresponding class and loads
    the matching schema file from the schemas directory.
    """
    schemas = {}
    field_metadata = {}

    for stream_name, stream_object in STREAMS.items():
        schema_path = get_abs_path(f'schemas/{stream_name}.json')
        with open(schema_path, encoding='utf-8') as file:
            schema = json.load(file)
        schemas[stream_name] = schema

        if stream_object.replication_method == 'INCREMENTAL':
            replication_keys = stream_object.valid_replication_keys
        else:
            replication_keys = None

        # pylint: disable=line-too-long
        # Documentation: https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference: https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_object.key_properties,
            replication_method=stream_object.replication_method,
            valid_replication_keys=replication_keys,
        )

        mdata = metadata.to_map(mdata)

        if replication_keys:
            for replication_key in replication_keys:
                mdata = metadata.write(mdata, ('properties', replication_key), 'inclusion', 'automatic')

        mdata = metadata.to_list(mdata)

        field_metadata[stream_name] = mdata

    return schemas, field_metadata

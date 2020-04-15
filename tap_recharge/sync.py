import singer
from singer import metrics, metadata, Transformer, utils

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    bookmark_type=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    last_integer=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer() as transformer:
                transformed_record = transformer.transform(record,
                                               schema,
                                               stream_metadata)

                 # Reset max_bookmark_value to new value if higher
                if bookmark_field and (bookmark_field in transformed_record):
                    if (max_bookmark_value is None) or \
                        (transformed_record[bookmark_field] > max_bookmark_value):
                        max_bookmark_value = transformed_record[bookmark_field]

                if bookmark_field and (bookmark_field in transformed_record):
                    if bookmark_type == 'integer':
                        # Keep only records whose bookmark is after the last_integer
                        if transformed_record[bookmark_field] >= last_integer:
                            write_record(stream_name, transformed_record, time_extracted=time_extracted)
                            counter.increment()
                    elif bookmark_type == 'datetime':
                        last_dttm = transformer._transform_datetime(last_datetime)
                        bookmark_dttm = transformer._transform_datetime(record[bookmark_field])
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            write_record(stream_name, transformed_record, time_extracted=time_extracted)
                            counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, counter.value


# Sync a specific parent or child endpoint.
def sync_endpoint(client, #pylint: disable=too-many-branches
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  endpoint_config,
                  data_key,
                  static_params,
                  bookmark_query_field=None,
                  bookmark_field=None,
                  bookmark_type=None,
                  id_fields=None,
                  parent=None,
                  parent_id=None):

    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = None
    last_integer = None
    max_bookmark_value = None
    if bookmark_type == 'integer':
        last_integer = get_bookmark(state, stream_name, 0)
        max_bookmark_value = last_integer
    else:
        last_datetime = get_bookmark(state, stream_name, start_date)
        max_bookmark_value = last_datetime

    write_schema(catalog, stream_name)

    # pagination: loop thru all pages of data
    page = 1
    pg_size = 250
    from_rec = 1
    record_count = pg_size # initial value, set with first API call
    total_records = 0
    while record_count == pg_size:
        params = {
            'page': page,
            'limit': pg_size,
            **static_params # adds in endpoint specific, sort, filter params
        }

        if bookmark_query_field:
            if bookmark_type == 'datetime':
                params[bookmark_query_field] = last_datetime[0:10] # last_datetime date
            elif bookmark_type == 'integer':
                params[bookmark_query_field] = last_integer

        LOGGER.info('{} - Sync start {}'.format(
            stream_name,
            'since: {}, '.format(last_datetime) if bookmark_query_field else ''))

        # Squash params to query-string params
        querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
        LOGGER.info('URL for {}: https://api.rechargeapps.com/{}?{}'\
            .format(stream_name, path, querystring))

        # Get data, API request
        data = client.get(
            path,
            params=querystring,
            endpoint=stream_name)
        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()

        # Transform data: currently NO transformations on data
        # The data_key identifies the collection of records below the <root> element
        # LOGGER.info('data = {}'.format(data)) # TESTING, comment out
        transformed_data = [] # initialize the record list
        if data_key in data:
            transformed_data = data[data_key]
        # LOGGER.info('transformed_data = {}'.format(transformed_data))  # TESTING, comment out
        if not transformed_data or transformed_data is None:
            break # No data results

        # If transformed_data is a single-record dict (like shop endpoint), add it to a list
        if isinstance(transformed_data, dict):
            tdata = []
            tdata.append(transformed_data)
            transformed_data = tdata

        # Process records and get the max_bookmark_value and record_count for the set of records
        max_bookmark_value, record_count = process_records(
            catalog=catalog,
            stream_name=stream_name,
            records=transformed_data,
            time_extracted=time_extracted,
            bookmark_field=bookmark_field,
            bookmark_type=bookmark_type,
            max_bookmark_value=max_bookmark_value,
            last_datetime=last_datetime,
            last_integer=last_integer,
            parent=parent,
            parent_id=parent_id)

        # Set total_records
        total_records = total_records + record_count

        # Loop thru parent batch records for each children objects (if should stream)
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                should_stream, last_stream_child = should_sync_stream(get_selected_streams(catalog),
                                                            None,
                                                            child_stream_name)
                if should_stream:
                    # For each parent record
                    for record in transformed_data:
                        i = 0
                        # Set parent_id
                        for id_field in id_fields:
                            if i == 0:
                                parent_id_field = id_field
                            if id_field == 'id':
                                parent_id_field = id_field
                            i = i + 1
                        parent_id = record.get(parent_id_field)

                        LOGGER.info('Syncing: {}, parent_stream: {}, parent_id: {}'.format(
                            child_stream_name,
                            stream_name,
                            parent_id))
                        child_path = child_endpoint_config.get('path').format(parent_id)
                        child_total_records = sync_endpoint(
                            client=client,
                            catalog=catalog,
                            state=state,
                            start_date=start_date,
                            stream_name=child_stream_name,
                            path=child_path,
                            endpoint_config=child_endpoint_config,
                            data_key=child_endpoint_config.get('data_key', child_stream_name),
                            static_params=child_endpoint_config.get('params', {}),
                            bookmark_query_field=child_endpoint_config.get('bookmark_query_field'),
                            bookmark_field=child_endpoint_config.get('bookmark_field'),
                            bookmark_type=child_endpoint_config.get('bookmark_type'),
                            id_fields=child_endpoint_config.get('id_fields'),
                            parent=child_endpoint_config.get('parent'),
                            parent_id=parent_id)
                        LOGGER.info('Synced: {}, parent_id: {}, total_records: {}'.format(
                            child_stream_name, 
                            parent_id,
                            child_total_records))

        # Set to_rec: to record; ending record for the batch
        to_rec = from_rec + record_count - 1

        LOGGER.info('{} - Synced Page {} - Records {} to {}'.format(
            stream_name,
            page,
            from_rec,
            to_rec))
        # Pagination: increment the page by 1
        page = page + 1
        from_rec = to_rec + 1

    # Update the state with the max_bookmark_value for the stream after ALL pages
    #   because the API requested data is NOT sorted
    if bookmark_field:
        write_bookmark(state,
                       stream_name,
                       max_bookmark_value)

    # Return the list of ids to the stream, in case this is a parent stream with children.
    return total_records


# Review catalog and make a list of selected streams
def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# Review last_stream (last currently syncing stream), if any,
#  and continue where it left off in the selected streams.
# Or begin from the beginning, if no last_stream, and sync
#  all selected steams.
# Returns should_sync_stream (true/false) and last_stream.
def should_sync_stream(selected_streams, last_stream, stream_name):
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            last_stream = None
        if stream_name in selected_streams:
            return True, last_stream
    return False, last_stream


def sync(client, catalog, state, start_date):
    selected_streams = get_selected_streams(catalog)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))

    # endpoints: API URL endpoints to be called
    # properties:
    #   <root node>: Plural stream name for the endpoint
    #   path: API endpoint relative path, when added to the base URL, creates the full path
    #   params: Query, sort, and other endpoint specific parameters
    #   data_key: JSON element containing the records for the endpoint
    #   bookmark_query_field: Typically a date-time field used for filtering the query
    #   bookmark_field: Replication key field, typically a date-time, used for filtering the results
    #        and setting the state
    #   bookmark_type: Data type for bookmark, integer or datetime
    #   id_fields: Primary key (and other IDs) from the Parent stored when store_ids is true.
    #   children: A collection of child endpoints (where the endpoint path includes the parent id)
    #   parent: On each of the children, the singular stream name for parent element
    #       NOT NEEDED FOR THIS INTEGRATION (The Children all include a reference to the Parent)
    endpoints = {
        'addresses': {
            'path': 'addresses',
            'params': {},
            'data_key': 'addresses',
            'bookmark_query_field': 'updated_at_min',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'charges': {
            'path': 'charges',
            'params': {},
            'data_key': 'charges',
            'bookmark_query_field': 'updated_at_min',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'collections': {
            'path': 'collections',
            'params': {},
            'data_key': 'collections',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'customers': {
            'path': 'customers',
            'params': {},
            'data_key': 'customers',
            'bookmark_query_field': 'updated_at_min',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'discounts': {
            'path': 'discounts',
            'params': {},
            'data_key': 'discounts',
            'bookmark_query_field': 'updated_at_min',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'metafields_store': {
            'path': 'metafields',
            'params': {
                'owner_resource': 'store'
            },
            'data_key': 'metafields',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'metafields_customer': {
            'path': 'metafields',
            'params': {
                'owner_resource': 'customer'
            },
            'data_key': 'metafields',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'metafields_subscription': {
            'path': 'metafields',
            'params': {
                'owner_resource': 'subscription'
            },
            'data_key': 'metafields',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'onetimes': {
            'path': 'onetimes',
            'params': {},
            'data_key': 'onetimes',
            'bookmark_query_field': 'updated_at_min',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'orders': {
            'path': 'orders',
            'params': {},
            'data_key': 'orders',
            'bookmark_query_field': 'updated_at_min',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'products': {
            'path': 'products',
            'params': {},
            'data_key': 'products',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'shop': {
            'path': 'shop',
            'params': {},
            'data_key': 'shop',
            'id_fields': ['id']
        },
        'subscriptions': {
            'path': 'subscriptions',
            'params': {},
            'data_key': 'subscriptions',
            'bookmark_query_field': 'updated_at_min',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        }
    }

    # For each endpoint (above), determine if the stream should be streamed
    #   (based on the catalog and last_stream), then sync those streams.
    for stream_name, endpoint_config in endpoints.items():
        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        stream_name)
        if should_stream:
            LOGGER.info('START Syncing: {}'.format(stream_name))
            update_currently_syncing(state, stream_name)
            path = endpoint_config.get('path')
            total_records = sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                start_date=start_date,
                stream_name=stream_name,
                path=path,
                endpoint_config=endpoint_config,
                data_key=endpoint_config.get('data_key', stream_name),
                static_params=endpoint_config.get('params', {}),
                bookmark_query_field=endpoint_config.get('bookmark_query_field'),
                bookmark_field=endpoint_config.get('bookmark_field'),
                bookmark_type=endpoint_config.get('bookmark_type'),
                id_fields=endpoint_config.get('id_fields'))

            update_currently_syncing(state, None)
            LOGGER.info('Synced: {}, total_records: {}'.format(
                            stream_name, 
                            total_records))
            LOGGER.info('FINISHED Syncing: {}'.format(stream_name))

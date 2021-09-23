#!/usr/bin/env python3

from singer import get_logger, utils

from tap_recharge.client import RechargeClient
from tap_recharge.discover import discover
from tap_recharge.sync import sync

LOGGER = get_logger()

REQUIRED_CONFIG_KEYS = [
    'access_token',
    'start_date',
    'user_agent'
]

def do_discover():

    LOGGER.info('Starting discover')
    catalog = discover()
    catalog.dump()
    LOGGER.info('Finished discover')


@utils.handle_top_exception(LOGGER)
def main():
    """Entrypoint function for tap."""

    parsed_args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    with RechargeClient(
        parsed_args.config['access_token'],
        parsed_args.config['user_agent']) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(
                client=client,
                catalog=parsed_args.catalog,
                state=state,
                config=parsed_args.config)

if __name__ == '__main__':
    main()

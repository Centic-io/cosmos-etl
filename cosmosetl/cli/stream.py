# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
import os
import random

from attr.converters import to_bool
from blockchainetl_common.streaming.streaming_utils import configure_logging, configure_signals
from dotenv import load_dotenv
import click
from query_state_lib.base.providers.auto import get_provider_from_uri

from config import EntityType, ItemExporterType
from cosmosetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from cosmosetl.streaming.streamer import Streamer
from cosmosetl.streaming.streaming_exporter_creator import determine_item_exporter_type, create_streaming_exporter
from cosmosetl.thread_local_proxy import ThreadLocalProxy

load_dotenv()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-l', '--last-synced-block-file', default='last_synced_block.txt', show_default=True, type=str, help='')
@click.option('--lag', default=0, show_default=True, type=int, help='The number of blocks to lag behind the network.')
@click.option('-p', '--provider-uri', default=None, show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-o', '--output', type=str, default=None,
              help='Either Google PubSub topic path e.g. projects/your-project/topics/crypto_ethereum; '
                   'or Postgres connection url e.g. postgresql+pg8000://postgres:admin@127.0.0.1:5432/ethereum. '
                   'or mongodb://just_for_dev:password_for_dev@localhost:27017')
@click.option('--db-prefix', default="", show_default=True, type=str,
              help='prefix of db')
@click.option('--add-wallets', default=False, show_default=True, type=bool,
              help='add wallet to exporter or not : True or False')
@click.option('-s', '--start-block', default=None, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', default=None, show_default=True, type=int, help='End block')
@click.option('--entity-types', default=','.join(EntityType.ALL_FOR_STREAMING), show_default=True, type=str,
              help='The list of entity types to export.')
@click.option('--period-seconds', default=3, show_default=True, type=int,
              help='How many seconds to sleep between syncs')
@click.option('-b', '--batch-size', default=4, show_default=True, type=int,
              help='How many blocks to batch in single request')
@click.option('-B', '--block-batch-size', default=32, show_default=True, type=int,
              help='How many blocks to batch in single sync round')
@click.option('-w', '--max-workers', default=8, show_default=True, type=int, help='The number of workers')
@click.option('--log-file', default=None, show_default=True, type=str, help='Log file')
@click.option('--pid-file', default=None, show_default=True, type=str, help='pid file')
@click.option('--stream-id', default=None, show_default=True, type=str, help='streamer id')
@click.option('--monitor', default=False, show_default=True, type=bool, help='True is turn on monitor')
@click.option('--stream-name', default='streaming_collector', show_default=True, type=str,
              help='Name of the collector')
@click.option('--chain-id', default=None, show_default=True, type=str, help='chain id')
def stream(last_synced_block_file, lag, provider_uri, output, db_prefix, add_wallets, start_block, end_block,
           entity_types, period_seconds, batch_size, block_batch_size, max_workers, log_file,
           pid_file, stream_id, monitor, stream_name, chain_id):
    """Streams all data types to console or Google Pub/Sub."""
    configure_logging(log_file)
    configure_signals()
    if not provider_uri:
        provider_uri = os.getenv("PROVIDER_URI")
    if not output:
        output = os.getenv("OUTPUT_DATABASE")
    entity_types = parse_entity_types(entity_types)
    validate_entity_types(entity_types, output)

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly
    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info('Using ' + provider_uri)

    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        item_exporter=create_streaming_exporter(output, collector_id=stream_id, db_prefix=db_prefix),
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types,
        stream_id=stream_id,
        stream_name=stream_name
    )
    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
        pid_file=pid_file,
        stream_id=stream_id,
        output=output,
        db_prefix=db_prefix,
        chain_id=chain_id,
        monitor=monitor
    )
    streamer.stream()


def parse_entity_types(entity_types):
    entity_types = [c.strip() for c in entity_types.split(',')]

    # validate passed types
    for entity_type in entity_types:
        if entity_type not in EntityType.ALL_FOR_STREAMING:
            raise click.BadOptionUsage(
                '--entity-type', '{} is not an available entity type. Supply a comma separated list of types from {}'
                .format(entity_type, ','.join(EntityType.ALL_FOR_STREAMING)))

    return entity_types


def validate_entity_types(entity_types, output):
    item_exporter_type = determine_item_exporter_type(output)
    if item_exporter_type == ItemExporterType.POSTGRES \
            and (EntityType.CONTRACT in entity_types or EntityType.TOKEN in entity_types):
        raise ValueError('contract and token are not yet supported entity types for postgres item exporter.')


def pick_random_provider_uri(provider_uri):
    provider_uris = [uri.strip() for uri in provider_uri.split(',')]
    return random.choice(provider_uris)

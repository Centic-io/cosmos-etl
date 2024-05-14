import json
import logging

from blockchainetl_common.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl_common.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from web3 import Web3
from web3.middleware import geth_poa_middleware

from config import EntityType
from cosmosetl.database.contract_filter import ContractFilterMemoryStorage
from cosmosetl.jobs.export_blocks_job import ExportBlocksJob
from cosmosetl.jobs.export_transactions_job import ExportTransactionsJob
from cosmosetl.json_rpc_requests import generate_get_latest_block_json_rpc
from cosmosetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from cosmosetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator


class EthStreamerAdapter:
    def __init__(
            self,
            batch_web3_provider,
            item_exporter=ConsoleItemExporter(),
            batch_size=100,
            block_batch_size=96,
            max_workers=5,
            entity_types=tuple(EntityType.ALL_FOR_STREAMING),
            stream_id='default-collector',
            stream_name='streaming_collector',
            chain_id='cosmos'
    ):
        self.chain_id = chain_id
        self.batch_web3_provider = batch_web3_provider
        self.w3 = Web3(batch_web3_provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.batch_size_block = min(batch_size, int(block_batch_size / max_workers))
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()
        self.contract_filter = ContractFilterMemoryStorage.getInstance()
        self.stream_id = stream_id
        self.stream_name = stream_name

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        rpc = generate_get_latest_block_json_rpc()
        data = self.batch_web3_provider.make_batch_request(json.dumps([rpc]))
        block_number = data.get("result", {}).get("sync_info", {}).get("latest_block_height")
        return int(block_number)

    def export_all(self, start_block, end_block):
        # reset temp wallet
        self.contract_filter.clear_temp()

        # Export blocks and transactions
        blocks, transactions, events = [], [], []
        if self._should_export(EntityType.BLOCK):
            blocks = self._export_blocks(start_block, end_block)

        if self._should_export(EntityType.TRANSACTION):
            transactions, events = self._export_transactions(start_block, end_block)

        self.item_exporter.upsert_transactions(transactions)
        self.item_exporter.upsert_blocks(blocks)
        self.item_exporter.upsert_logs(events)

    def _export_blocks(self, start_block, end_block):
        blocks_and_transactions_item_exporter = InMemoryItemExporter(item_types=['block'])
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size_block,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,

        )
        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items('block')
        return blocks

    def _export_transactions(self, start_block, end_block):
        blocks_and_transactions_item_exporter = InMemoryItemExporter(item_types=['transaction', 'event'])
        blocks_and_transactions_job = ExportTransactionsJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size_block,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            export_transactions=EntityType.TRANSACTION in self.entity_types,
            export_events=EntityType.LOG in self.entity_types,
            chain_id=self.chain_id
        )
        blocks_and_transactions_job.run()
        transactions = blocks_and_transactions_item_exporter.get_items('transaction')
        events = blocks_and_transactions_item_exporter.get_items('event')
        return transactions, events

    def _should_export(self, entity_type):
        if entity_type == EntityType.BLOCK:
            return True

        if entity_type == EntityType.TRANSACTION:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(EntityType.LOG)

        if entity_type == EntityType.LOG:
            return True

        raise ValueError('Unexpected entity type ' + entity_type)

    def calculate_item_ids(self, items):
        for item in items:
            item['item_id'] = self.item_id_calculator.calculate(item)

    def calculate_item_timestamps(self, items):
        for item in items:
            item['item_timestamp'] = self.item_timestamp_calculator.calculate(item)

    def close(self):
        self.item_exporter.close()

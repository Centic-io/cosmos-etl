import os
from dotenv import load_dotenv

load_dotenv()


class MongoDBConfig:
    NAME = os.environ.get("MONGO_USERNAME") or "just_for_dev"
    PASSWORD = os.environ.get("MONGO_PASSWORD") or "password_for_dev"
    HOST = os.environ.get("MONGO_HOST") or "localhost"
    PORT = os.environ.get("MONGO_PORT") or "27027"
    DATABASE = "blockchain_etl"
    BLOCKS = "blocks"
    TRANSACTIONS = "transactions"
    TOKEN_TRANSFERS = "token_transfers"
    CONTRACTS = "contracts"
    TOKENS = "tokens"
    RECEIPTS = "receipts"
    LOGS = "logs"
    COLLECTORS = "collectors"
    WALLETS = "wallets"
    LENDING_EVENTS = 'lending_events'
    TRAVADAO_EVENTS = "travadao_events"
    EVENTS = ['lending_events', "travadao_events"]


class MonitoringConfig:
    MONITOR_ROOT_PATH = "/home/monitor/.log/"


class EntityType:
    BLOCK = 'block'
    TRANSACTION = 'transaction'
    RECEIPT = 'receipt'
    LOG = 'log'
    TOKEN_TRANSFER = 'token_transfer'
    TRACE = 'trace'
    CONTRACT = 'contract'
    TOKEN = 'token'
    ENRICH_TXS = 'enrich_txs'

    ALL_FOR_STREAMING = [BLOCK, TRANSACTION, LOG, TOKEN_TRANSFER, TRACE, CONTRACT, TOKEN, ENRICH_TXS]
    ALL_FOR_INFURA = [BLOCK, TRANSACTION, LOG, TOKEN_TRANSFER]


class ItemExporterType:
    PUBSUB = 'pubsub'
    POSTGRES = 'postgres'
    MONGODB = 'mongodb'
    CONSOLE = 'console'
    UNKNOWN = 'unknown'

from datetime import datetime

from cosmosetl.domain.block import CosmBlock
from cosmosetl.utils import str_to_dec

class CosmBlockMapper:
    def json_dict_to_block(self, json_dict):
        block = CosmBlock()
        block.height = str_to_dec(json_dict['block']['header'].get('height'))
        block.hash = json_dict['block_id'].get('hash')
        block.last_block_hash = json_dict['block']['header'].get('last_block_id', {}).get('hash')
        block.data_hash = json_dict['block']['header'].get('data_hash')
        block.proposer = json_dict['block']['header'].get('proposer_address')
        block.num_txs = len(json_dict['block']['data'].get('txs', []))
        block.time = json_dict['block']['header'].get('time')
        block.timestamp = block.time

        return block
    
    def block_to_dict(self, block):
        return {
            'type': 'block',
            'number': block.height,
            'hash': block.hash.lower(),
            'last_block_hash': block.last_block_hash.lower(),
            'data_hash': block.data_hash.lower(),
            'proposer': block.proposer.lower(),
            'num_txs': block.num_txs,
            'datetime': block.time,
            'timestamp': datetime.strptime(block.time.split(".")[0]+"Z", "%Y-%m-%dT%H:%M:%SZ").timestamp()
        }
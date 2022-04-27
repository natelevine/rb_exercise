from queue import Queue
from contextlib import contextmanager

from web3 import Web3
from web3.contract import Contract

from constants import Pair


class QuickSwapPairClient:
    def __init__(self, node_uri: str, abi: dict, pair: Pair, address: str) -> None:
        self.pair = pair
        self.address = address
        self.node_uri = node_uri
        self.abi = abi
        self._contract_pool: Queue[Contract] = Queue(maxsize=6)
        self._build_contract_pool()
        self._swap_contract = Web3(Web3.WebsocketProvider(self.node_uri)).eth.contract(address=Web3.toChecksumAddress(self.address), abi=self.abi)

    def _build_contract_pool(self):
        for _ in range(6):
            self._contract_pool.put_nowait(Web3(Web3.WebsocketProvider(self.node_uri)).eth.contract(address=Web3.toChecksumAddress(self.address), abi=self.abi))
    
    @contextmanager
    def _pooled_contract(self):
        try:
            contract = self._contract_pool.get(timeout=3)
            yield contract
        finally:
            self._contract_pool.put(contract)
    
    def fetch_current_pool_reserves(self) -> list[int]:
        with self._pooled_contract() as contract:
            return contract.functions.getReserves().call()

    def fetch_lp_token_supply(self) -> int:
        with self._pooled_contract() as contract:
            return contract.functions.totalSupply().call()
    
    def get_latest_swap_filter(self):
        # with self._pooled_contract() as contract:
        return self._swap_contract.events.Swap.createFilter(fromBlock='latest')

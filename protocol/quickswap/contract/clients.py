from queue import Queue
from contextlib import contextmanager

from web3 import Web3
from web3.contract import Contract

from constants import Pair


class QuickSwapPairClient:
    """
    Provides access to per-pair contract functions and connection pooling so contract functions can easily
    be called across multiple threads. The pool is threadsafe and accessed automatically by client methods.

    Note: I put this in because I was seeing errors using async await across multiple threads (two threads cannot call recv on the same socket)
        so apparently the Websocket connections provided by Web3 are _not_ threadsafe :<
    """

    def __init__(self, node_uri: str, abi: dict, pair: Pair, address: str, pool_size: int = 6) -> None:
        self.pair = pair
        self.address = address
        self.node_uri = node_uri
        self.abi = abi
        self._contract_pool: Queue[Contract] = Queue(maxsize=pool_size)
        self._build_contract_pool(pool_size)
        # This is a janky way to ensure the swap filter always gets its own underlying connection
        # since the pool manager would check the connection back into the pool and allow it to be
        # re-used in another context.
        #
        # A better approach would be to manage connections being used by the event loop
        # vs. multi-threaded workloads separately, but it didn't seem worth it given the scope of this so far.
        self._swap_contract = self._new_websocket_conn()

    def _build_contract_pool(self, pool_size: int):
        for _ in range(pool_size):
            self._contract_pool.put_nowait(self._new_websocket_conn())
    
    @contextmanager
    def _pooled_contract(self):
        try:
            contract = self._contract_pool.get(timeout=3)
            yield contract
        finally:
            self._contract_pool.put(contract)
    
    def _new_websocket_conn(self):
        return Web3(Web3.WebsocketProvider(self.node_uri)).eth.contract(address=Web3.toChecksumAddress(self.address), abi=self.abi)
    
    def fetch_current_pool_reserves(self) -> list[int]:
        with self._pooled_contract() as contract:
            return contract.functions.getReserves().call()

    def fetch_lp_token_supply(self) -> int:
        with self._pooled_contract() as contract:
            return contract.functions.totalSupply().call()
    
    def get_latest_swap_filter(self):
        # with self._pooled_contract() as contract:
        return self._swap_contract.events.Swap.createFilter(fromBlock='latest')

import asyncio
from time import time_ns
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3
from web3.middleware import geth_poa_middleware
from abis import QUICKSWAP_PAIR
from clients.contract.quick_swap import QuickSwapPairClient
from constants import Pair

# external node provider for this exercise
MATIC_WSS = 'wss://ws-matic-mainnet.chainstacklabs.com'

# Connect to the remote node.
web3_websocket_conn = Web3(Web3.WebsocketProvider(MATIC_WSS))

# Fixes a fieldLength error calling getBlock since we're using a POA chain
web3_websocket_conn.middleware_onion.inject(geth_poa_middleware, layer=0)

# Do these ever change? Maybe should be dynamic config? Reliability vs. flexibility trade-off
WMATIC_USDC_ADDRESS = '0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827'
USDC_WETH_ADDRESS = '0x853ee4b2a13f8a742d64c8f088be7ba2131f670d'
WMATIC_WETH_ADDRESS = '0xadbf1854e5883eb8aa7baf50705338739e558e5b'

MONITORED_PAIRS = {
    (Pair.WMATIC_USDC, WMATIC_USDC_ADDRESS),
    (Pair.USDC_WETH, USDC_WETH_ADDRESS),
    (Pair.WMATIC_WETH, WMATIC_WETH_ADDRESS),
}

QUICKSWAP_PAIR_ABI = QUICKSWAP_PAIR['abi']

clients = [QuickSwapPairClient(MATIC_WSS, QUICKSWAP_PAIR_ABI, mp[0], mp[1]) for mp in MONITORED_PAIRS]

@dataclass(frozen=True)
class PairStatusEvent:
    """
    Describes point-in-time state of a liquidity pool in the QuickSwap DEX
    pair      - QuickSwap pair that this status represents
    pair_id	  - Contract address of the pool's pair
    reserve_0 - The amount of Token0 in reserve in the pool
    reserve_1 - The amount of Token1 in reserve in the pool
    lp_shares - The amount of LP tokens outstanding for the pool
    block	  - The block number as of which this position was calculated
    timestamp - Unix time (in milliseconds) when this event was created
    """

    pair: Pair
    pair_id: str
    reserve_0: int
    reserve_1: int
    lp_shares: int
    block: int
    timestamp: int

@dataclass(frozen=True)
class SwapEvent:
    """
    Describes per-block swaps for a given liquidity pool in the QuickSwap DEX

    pair         - QuickSwap pair that this swap event represents
    pair_id	     - Contract address of the pool's pair
    amount_0_in  - Amount of token0 swapped in
    amount_1_in  - Amount of token1 swapped in
    amount_0_out - Amount of token0 swapped out
    amount_1_out - Amount of token1 swapped out
    tx           - Transaction hash of the swap
    block	     - The block number containing this swap
    timestamp    - Unix time (in milliseconds) when this event was created
    """

    pair: Pair
    pair_id: str
    amount_0_in: int
    amount_1_in: int
    amount_0_out: int
    amount_1_out: int
    tx: str
    block: int
    timestamp: int

@dataclass(frozen=True)
class SwapEvents:
    swaps: list[SwapEvent]

async def main(executor: ThreadPoolExecutor) -> None:
    print("***** Started main function *****")
    latest_block_filter = web3_websocket_conn.eth.filter('latest')
    await asyncio.gather(
        poll_for_blocks(latest_block_filter, executor, 0.3),
        poll_for_swaps(0.3))

class BlockTracker:
    def __init__(self) -> None:
        self.prev_block = None
    
    def check_continuous_block_lineage(self, curr_block: asyncio.Future) -> bool:
        # In case this is the first block we've seen, initialize our tracker so we can pass this check
        curr_block_parent = curr_block.result().parentHash.hex()
        self.prev_block = self.prev_block or curr_block_parent
        is_direct_link = curr_block_parent == self.prev_block

        if not is_direct_link:
            print(f"!!!!!!!!!!! **************** Missing at least one block: {curr_block_parent} ***************** !!!!!!!!!!!!!!!!")

        # update our status regardless of whether we passed or not
        self.prev_block = curr_block.result().hash.hex()


async def poll_for_blocks(latest_block_filter, executor: ThreadPoolExecutor, poll_interval: int) -> None:
    print("***** Beginning poll for new blocks *****")
    block_tracker = BlockTracker()

    while True:
        for block in latest_block_filter.get_new_entries():
            block_addr = block.hex()
            print("New block!!! - ", block_addr)

            event_loop = asyncio.get_running_loop()
            block_data = event_loop.run_in_executor(executor, web3_websocket_conn.eth.getBlock, block_addr)
            block_data.add_done_callback(block_tracker.check_continuous_block_lineage)

            for client in clients:
                lp_shares = event_loop.run_in_executor(executor, client.fetch_lp_token_supply)
                position = event_loop.run_in_executor(executor, client.fetch_current_pool_reserves)

                await asyncio.gather(block_data, lp_shares, position)

                block_number = block_data.result().number
                pair_status_event = build_latest_pair_status_event(block_number, client, lp_shares.result(), position.result())

                print(pair_status_event.__dict__)

        await asyncio.sleep(poll_interval)


async def poll_for_swaps(poll_interval: int) -> None:
    print("***** Beginning poll for swaps *****")
    for client in clients:
        swap_filter = client.get_latest_swap_filter()

        while True:
            if (new_swaps := swap_filter.get_new_entries()):
                # TODO collate swaps from the same block together
                print(build_latest_pair_swaps_event(client, new_swaps).__dict__)
            await asyncio.sleep(poll_interval)

def build_latest_pair_status_event(block: int, client: QuickSwapPairClient, lp_shares: int, position: list[int]) -> PairStatusEvent:
    return PairStatusEvent(
        pair=client.pair,
        pair_id=client.address,
        reserve_0=position[0],
        reserve_1=position[1],
        lp_shares=lp_shares,
        block=block,
        timestamp=int(time_ns() / 1000))

def build_latest_pair_swaps_event(client: QuickSwapPairClient, swaps: list) -> SwapEvents:
    if len(swaps) > 1:
        print("****** Got multiple swaps!!!! - ", Web3.toJSON(swaps))
        curr_block = swaps[0].blockNumber
        for swap in swaps:
            if curr_block != swap.blockNumber:
                # This would probably be an alert
                print("********** !!!!!!!!!!!!!!!!! Multiple swaps in one batch, with mismatched blocks ************** !!!!!!!!!!!!!!!!")
    
    swap_events = []
    for swap in swaps:
        args = swap.args

        swap_events.append(
            SwapEvent(
                pair=client.pair,
                pair_id=client.address,
                amount_0_in=args.amount0In,
                amount_1_in=args.amount1In,
                amount_0_out=args.amount0Out,
                amount_1_out=args.amount1Out,
                tx=swap.transactionHash,
                block=swap.blockNumber,
                timestamp=int(time_ns() / 1000)))

    return SwapEvents(swap_events)


if __name__ == "__main__":
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            asyncio.run(main(executor))
    except KeyboardInterrupt:
        pass
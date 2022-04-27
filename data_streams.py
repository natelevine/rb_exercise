import asyncio
from time import time_ns
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3
from web3.middleware import geth_poa_middleware
from protocol.quickswap.contract.abis import QUICKSWAP_PAIR
from protocol.quickswap.config import MATIC_WSS, MONITORED_PAIRS
from protocol.quickswap.contract.clients import QuickSwapPairClient
from protocol.quickswap.events.liquidity_pool import PairStatusEvent, SwapEvent, SwapEvents

# Connect to the remote node.
web3_websocket_conn = Web3(Web3.WebsocketProvider(MATIC_WSS))

# Fixes a fieldLength error calling getBlock since we're using a POA chain
web3_websocket_conn.middleware_onion.inject(geth_poa_middleware, layer=0)

clients = [QuickSwapPairClient(MATIC_WSS, QUICKSWAP_PAIR['abi'], mp[0], mp[1]) for mp in MONITORED_PAIRS]

STATS = {
    'BLOCK_PROCESS': {
        'min_process_time_ms': float('inf'),
        'max_process_time_ms': float('-inf'),
    },
    'SWAP_PROCESS': {
        'min_process_time_ms': float('inf'),
        'max_process_time_ms': float('-inf'),
    }
}

async def main(executor: ThreadPoolExecutor) -> None:
    print("***** Started main function *****")
    latest_block_filter = web3_websocket_conn.eth.filter('latest')
    await asyncio.gather(
        poll_for_blocks(latest_block_filter, executor, 0.3),
        poll_for_swaps(0.3),
        report_process_stats(10))

def update_process_stats(start_ns: int, end_ns: int, process_key: str) -> None:
    process_time_ms = (end_ns - start_ns) / 1000000
    process_stats = STATS[process_key]

    if process_time_ms < process_stats['min_process_time_ms']:
        process_stats['min_process_time_ms'] = process_time_ms

    if process_time_ms > process_stats['max_process_time_ms']:
        process_stats['max_process_time_ms'] = process_time_ms

async def report_process_stats(report_interval: int) -> None:
    while(True):
        await asyncio.sleep(report_interval)
        print(f"BLOCK_PROCESS: min_time_ms: {STATS['BLOCK_PROCESS']['min_process_time_ms']}, max_time_ms: {STATS['BLOCK_PROCESS']['max_process_time_ms']}")
        print(f"SWAP_PROCESS: min_time_ms: {STATS['SWAP_PROCESS']['min_process_time_ms']}, max_time_ms: {STATS['SWAP_PROCESS']['max_process_time_ms']}")

class BlockTracker:
    """
    Provides basic block "lineage" monitoring, i.e. that we don't
    skip over any blocks in producing our data stream.

    In a production app, I imagine this would become more robust,
    driving alerting and/or automated recovery.
    """

    def __init__(self) -> None:
        self.prev_block = None
    
    def assert_continuous_block_lineage(self, curr_block: asyncio.Future) -> bool:
        # In case this is the first block we've seen, initialize our tracker so we can pass this check
        curr_block_parent = curr_block.result().parentHash.hex()
        self.prev_block = self.prev_block or curr_block_parent
        is_direct_link = curr_block_parent == self.prev_block

        if not is_direct_link:
            # lol
            print(f"!!!!!!!!!!! ************** Missing at least one block: {curr_block_parent} *************** !!!!!!!!!!!!")

        # update block status regardless of whether we passed or not
        self.prev_block = curr_block.result().hash.hex()


async def poll_for_blocks(latest_block_filter, executor: ThreadPoolExecutor, poll_interval: int) -> None:
    """
    Must be called from an asyncio event loop. This kicks off a poll / async sleep routine looking for
    new blocks generated from the `latest_block_filter` provided.

    If a new block is found, it kicks off a number of concurrent requests to the current node
    to ultimately build a status event describing the liquidity pool as of the new node.

    These requests are run in separate threads because the way Web3 provides these APIs
    (at least with a Websocket connection) is not asyncio friendly. 
    
    Rather than block on synchronous calls, we kick them off at the same time using a ThreadPoolExecutor,
    and release the event loop (`asyncio.gather`) until they all complete.
    """

    print(f"***** Beginning poll loop for new blocks. Poll interval set to: {poll_interval}s *****")
    block_tracker = BlockTracker()

    while True:
        for block in latest_block_filter.get_new_entries():
            start_ns = time_ns()

            block_addr = block.hex()
            print("New block!!! - ", block_addr)

            event_loop = asyncio.get_running_loop()
            block_data = event_loop.run_in_executor(executor, web3_websocket_conn.eth.getBlock, block_addr)
            block_data.add_done_callback(block_tracker.assert_continuous_block_lineage)

            for client in clients:
                lp_shares = event_loop.run_in_executor(executor, client.fetch_lp_token_supply)
                position = event_loop.run_in_executor(executor, client.fetch_current_pool_reserves)

                await asyncio.gather(block_data, lp_shares, position)

                block_number = block_data.result().number
                pair_status_event = build_latest_pair_status_event(block_number, client, lp_shares.result(), position.result())

                print(pair_status_event.__dict__)

                end_ns = time_ns()
                update_process_stats(start_ns, end_ns, 'BLOCK_PROCESS')

        await asyncio.sleep(poll_interval)

async def poll_for_swaps(poll_interval: int) -> None:
    print(f"***** Beginning poll for swaps. Poll interval set to: {poll_interval}s *****")

    for client in clients:
        swap_filter = client.get_latest_swap_filter()

        while True:
            if (new_swaps := swap_filter.get_new_entries()):
                start_ns = time_ns()

                # TODO collate swaps from the same block together
                print(build_latest_pair_swaps_event(client, new_swaps).__dict__)

                end_ns = time_ns()
                update_process_stats(start_ns, end_ns, 'SWAP_PROCESS')

            await asyncio.sleep(poll_interval)

def build_latest_pair_status_event(block: int, client: QuickSwapPairClient, lp_shares: int, position: list[int]) -> PairStatusEvent:
    return PairStatusEvent(
        pair=client.pair,
        pair_id=client.address,
        reserve_0=position[0],
        reserve_1=position[1],
        lp_shares=lp_shares,
        block=block,
        timestamp=int(time_ns() / 1000000))

def build_latest_pair_swaps_event(client: QuickSwapPairClient, swaps: list) -> SwapEvents:
    if len(swaps) > 1:
        print("****** Got multiple swaps!!!! - ", Web3.toJSON(swaps))
        curr_block = swaps[0].blockNumber
        for swap in swaps:
            if curr_block != swap.blockNumber:
                # Another sanity check, that our event blob only contains swap txns from the same block
                # This would likely also be an alert
                print("*********** !!!!!!!!!!!!! Multiple swaps in one batch, with mismatched blocks *********** !!!!!!!!!!!!!")
    
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
                timestamp=int(time_ns() / 1000000)))

    return SwapEvents(swap_events)


if __name__ == "__main__":
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            asyncio.run(main(executor))
    except KeyboardInterrupt:
        pass
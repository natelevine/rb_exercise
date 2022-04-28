import asyncio
import json
from numpy import percentile
from time import time_ns
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3
from web3.middleware import geth_poa_middleware
from protocol.quickswap.contract.abis import QUICKSWAP_PAIR
from protocol.quickswap.config import MATIC_WSS, MONITORED_PAIRS
from protocol.quickswap.contract.clients import QuickSwapPairClient
from protocol.quickswap.events.liquidity_pool import PairStatusEvent, SwapEvent, SwapEvents


class ProcessStats:
    def __init__(self, executor: ThreadPoolExecutor) -> None:
        self._executor = executor
        self._block_process_times = []
        self._swap_process_times = []
    
    def report_block_process(self, start_ns: int, end_ns: int) -> None:
        self._block_process_times.append(int((end_ns - start_ns) / 1000000))

    def report_swap_process(self, start_ns: int, end_ns: int) -> None:
        self._swap_process_times.append(int((end_ns - start_ns) / 1000000))

    async def report_process_stats(self, report_interval: int) -> None:
        while(True):
            await asyncio.sleep(report_interval)
            # copy our runnning lists for calculation, then reset them
            block_process_times = [*self._block_process_times]
            self._block_process_times = []
            swap_process_times = [*self._swap_process_times]
            self._swap_process_times = []

            # Run this part in a separate thread since it could get somewhat CPU heavy,
            # and blocking the event loop on metrics is not ideal
            event_loop = asyncio.get_running_loop()
            stats = await event_loop.run_in_executor(executor, self.calculate_process_stats, block_process_times, swap_process_times)
            # for now just print to stdout
            print(json.dumps(stats, indent=2))

    def calculate_process_stats(self, block_process_times: list[int], swap_process_times: list[int]) -> dict:
        # initialize everything to 0 just in case we haven't recorded any stats yet
        block_process_times = block_process_times or [0]
        swap_process_times = swap_process_times or [0]

        return {
            'BLOCK_PROCESS': {
                'min_process_time_ms': min(block_process_times, default=0),
                'max_process_time_ms': max(block_process_times, default=0),
                'p50_process_time_ms': int(percentile(block_process_times, 50)),
                'p99_process_time_ms': int(percentile(block_process_times, 99)),
            },
            'SWAP_PROCESS': {
                'min_process_time_ms': min(swap_process_times, default=0),
                'max_process_time_ms': max(swap_process_times, default=0),
                'p50_process_time_ms': int(percentile(swap_process_times, 50)),
                'p99_process_time_ms': int(percentile(swap_process_times, 99)),
            },
        }

async def main(executor: ThreadPoolExecutor) -> None:
    # Initial setup

    # This is the main connection used by the event loop, which is safe since
    # it's single-threaded. The client classes provide their own connection pools
    # to handle multi-threaded operations.
    web3_websocket_conn = Web3(Web3.WebsocketProvider(MATIC_WSS))

    # Fixes a fieldLength error when calling getBlock, since we're using a POA chain.
    web3_websocket_conn.middleware_onion.inject(geth_poa_middleware, layer=0)

    # Generate clients for each pair we're setup to monitor.
    # 
    # This abstraction is a little rough, but works fine for now. 
    # The client provides both metadata about the pair + address being monitored, as well as an interface
    # to invoke methods on the contract + manage underlying connections.
    clients = [QuickSwapPairClient(MATIC_WSS, QUICKSWAP_PAIR['abi'], mp[0], mp[1]) for mp in MONITORED_PAIRS]

    print("***** Started main function *****")
    latest_block_filter = web3_websocket_conn.eth.filter('latest')
    stats = ProcessStats(executor)

    await asyncio.gather(
        poll_for_blocks(latest_block_filter, executor, clients, web3_websocket_conn, stats, 0.3),
        poll_for_swaps(clients, stats, 0.3),
        stats.report_process_stats(10))

class BlockTracker:
    """
    Provides basic block "lineage" monitoring, i.e. since this is a polling model on the latest block, 
    we want to ensure that we don't miss any blocks in producing our data stream.

    In a production app, I imagine this would become more robust,
    driving alerting and/or automated recovery.
    """

    def __init__(self) -> None:
        self.prev_block = None
    
    def assert_continuous_block_lineage(self, curr_block: asyncio.Future) -> bool:
        curr_block_parent = curr_block.result().parentHash.hex()
        # In case this is the first block we've seen, initialize our tracker so we can pass this check
        self.prev_block = self.prev_block or curr_block_parent
        is_direct_link = curr_block_parent == self.prev_block

        if not is_direct_link:
            # lol
            print(f"!!!!!!!!!!! ************** Missing at least one block: {curr_block_parent} *************** !!!!!!!!!!!!")

        # update block status regardless of whether we passed or not
        self.prev_block = curr_block.result().hash.hex()


async def poll_for_blocks(latest_block_filter, executor: ThreadPoolExecutor, clients: list[QuickSwapPairClient], conn: Web3, stats: ProcessStats, poll_interval: int) -> None:
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
            block_data = event_loop.run_in_executor(executor, conn.eth.getBlock, block_addr)
            # register a callback to check block linkage as soon as the future resolves
            block_data.add_done_callback(block_tracker.assert_continuous_block_lineage)

            # No need to await here, so let the block polling loop continue
            asyncio.gather(*[collect_pool_status(client, event_loop, block_data, stats, start_ns) for client in clients])

        await asyncio.sleep(poll_interval)

async def collect_pool_status(client: QuickSwapPairClient, event_loop: asyncio.AbstractEventLoop, block_data: asyncio.Future, stats: ProcessStats, start_ns: int):
    lp_shares = event_loop.run_in_executor(executor, client.fetch_lp_token_supply)
    position = event_loop.run_in_executor(executor, client.fetch_current_pool_reserves)

    # We have to await here since we need the results to build the status event
    await asyncio.gather(block_data, lp_shares, position)

    block_number = block_data.result().number
    pair_status_event = build_latest_pair_status_event(block_number, client, lp_shares.result(), position.result())
    # TODO - extract this to a formal publish method
    print(json.dumps(pair_status_event.__dict__, indent=2))

    end_ns = time_ns()
    stats.report_block_process(start_ns, end_ns)

async def poll_for_swaps(clients: list[QuickSwapPairClient], stats: ProcessStats, poll_interval: int) -> None:
    print(f"***** Beginning poll for swaps. Poll interval set to: {poll_interval}s *****")
    await asyncio.gather(*[swap_loop(client, stats, poll_interval) for client in clients])

async def swap_loop(client: QuickSwapPairClient, stats: ProcessStats, poll_interval: int):
    swap_filter = client.get_latest_swap_filter()
    while True:
        if (new_swaps := swap_filter.get_new_entries()):
            start_ns = time_ns()

            # TODO - extract this to a formal publish method
            swap_events = build_latest_pair_swaps_event(client, new_swaps).__dict__
            swaps_json_friendly = [swap.__dict__ for swap in swap_events['swaps']]
            print(json.dumps(swaps_json_friendly, indent=2))

            end_ns = time_ns()
            stats.report_swap_process(start_ns, end_ns)

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
                tx=swap.transactionHash.hex(),
                block=swap.blockNumber,
                timestamp=int(time_ns() / 1000000)))

    return SwapEvents(swap_events)


if __name__ == "__main__":
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            asyncio.run(main(executor))
    except KeyboardInterrupt:
        pass
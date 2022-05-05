import asyncio
from concurrent.futures import ThreadPoolExecutor
import json

from numpy import percentile


class ProcessStats:
    """
    Implements very rudimentary metrics on block event processing latency.
    Written to be compatible with an asyncio event loop, and runs all
    statistical calculations in a separate thread.

    Currently just prints to stdout, but could be updated to report to a
    local metrics collector or 3rd party API.
    """
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
            stats = await event_loop.run_in_executor(self._executor, self.calculate_process_stats, block_process_times, swap_process_times)
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

from dataclasses import dataclass

from constants import Pair


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

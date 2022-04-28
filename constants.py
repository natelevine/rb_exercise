from enum import Enum


class Pair(str, Enum):
    WMATIC_USDC = 'WMATIC_USDC'
    USDC_WETH = 'USDC_WETH'
    WMATIC_WETH = 'WMATIC_WETH'

# external node provider for this exercise
from constants import Pair


MATIC_WSS = 'wss://ws-matic-mainnet.chainstacklabs.com'

# Do these ever change? Maybe should be dynamic config? Reliability vs. flexibility trade-off
WMATIC_USDC_ADDRESS = '0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827'
USDC_WETH_ADDRESS = '0x853ee4b2a13f8a742d64c8f088be7ba2131f670d'
WMATIC_WETH_ADDRESS = '0xadbf1854e5883eb8aa7baf50705338739e558e5b'

MONITORED_PAIRS = {
    (Pair.WMATIC_USDC, WMATIC_USDC_ADDRESS),
    (Pair.USDC_WETH, USDC_WETH_ADDRESS),
    (Pair.WMATIC_WETH, WMATIC_WETH_ADDRESS),
}

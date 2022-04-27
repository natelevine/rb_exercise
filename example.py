"""
This is just scratch / me playing around with the example code. Not part of the actual exercise.
"""
from web3 import Web3
from abis import QUICKSWAP_PAIR

# Add the endpoint of the remote node provider.
MATIC_WSS = 'wss://ws-matic-mainnet.chainstacklabs.com'

# Connect to the remote node.
web3 = Web3(Web3.WebsocketProvider(MATIC_WSS))

# Specify the contract address of the Pair.
# The three pairs in scope for this exercise are listed below.
# In these examples we use the WMATIC-USDC Pair.
WMATIC_USDC_ADDRESS = '0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827'

# Create the Contract object
# https://web3py.readthedocs.io/en/stable/contracts.html
pair = web3.eth.contract(address=web3.toChecksumAddress(WMATIC_USDC_ADDRESS), abi=QUICKSWAP_PAIR['abi'])

# The Contract object can be used to call the contract functions specified in the ABI
# In this example we are calling the totalSupply function to retrieve the amount of LP tokens outstanding.
# The ABI and available functions can also be retrieved from viewing the deployed contract in Polygon's explorer.
# https://polygonscan.com/address/0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827#readContract
lp_tokens = pair.functions.totalSupply().call()

print(lp_tokens)

# The Contract object can also be used to create an Event Filter.
# In these examples we are filtering for the Mint Event.
# https://web3py.readthedocs.io/en/stable/filters.html
mint_filter = pair.events.Mint.createFilter(fromBlock='latest') # Gets the latest block from the node.
for Mint in mint_filter.get_new_entries(): # The get_new_entries() method returns only new entries since the last poll.
    print(Mint)
    print('Ex1:' + Web3.toJSON(Mint))

# You can also construct Event Filters over a range of blocks.
# mint_filter_blocks = pair.events.Mint.createFilter(fromBlock=27080354, toBlock=27080355) # Range of blocks
# for Mint in mint_filter_blocks.get_all_entries():
    # print('Ex2:' + Web3.toJSON(Mint))

# Here's an example using the arguments emited by an Event.
# When a Mint Event occurs, there are also two corresponding Transfer Events
# These are Transfers of the newly minted LP Tokens to the liquidity providers wallet and the protocol
# Since these a new tokens being minted, they are sent from a null address.
# Note that multiple Mints can occur in the same block, so this alone is not sufficient filtering to isolate
# the associated transfer in that case, but nonetheless a helpful example.
# NULL_ADDRESS = '0x0000000000000000000000000000000000000000'
# args = {'from': web3.toChecksumAddress(NULL_ADDRESS)}
# transfer_filter_args = pair.events.Transfer.createFilter(fromBlock=27080354, toBlock=27080354, argument_filters=args)
# for Transfer in transfer_filter_args.get_all_entries():
    # print('Ex3:' + Web3.toJSON(Transfer))

# sync_filter = pair.events.Sync.createFilter(fromBlock='latest')
# for Sync in sync_filter.get_new_entries():
    # print(Sync)
    # print('*** Ex4:' + Web3.toJSON(Sync))

swap_filter = pair.events.Swap.createFilter(fromBlock='latest')
for Swap in swap_filter.get_new_entries():
    print('*** Swaps: ' + Web3.toJSON(Swap))
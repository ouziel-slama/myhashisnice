import time

from mhinstore import MhinStore
from mhinindexes import MhinIndexes
from config import Config
from protocol import process_block
from fetcher import RSFetcher

Config().set_network("mainnet")


def parse():
    mhin_store = MhinStore(base_path=Config()["BALANCES_STORE"])
    current_height = mhin_store.current_block
    
    # If the system has already processed blocks, continue from there
    if current_height > 0:
        print(f"Resuming from saved height: {current_height}")
    # Otherwise, use the configured starting height
    else:
        current_height = Config()["START_HEIGHT"]
        print(f"Starting from configured height: {current_height}")
    
    indexer = MhinIndexes(Config()["BALANCES_STORE"])
    indexer.start()
        
    block_fetcher = RSFetcher(current_height + 1)
    start_time_1000 = time.time()
    ellapsed_1000 = 0
    counter = 0
    try:
        while True:
            start_time = time.time()
            block = block_fetcher.get_next_block()
            if block is None:
                time.sleep(1)
                continue
            process_block(block, mhin_store)

            counter += 1
            if counter == 1000:
                counter = 0
                ellapsed_1000 = time.time() - start_time_1000
                start_time_1000 = time.time()
            ellapsed = time.time() - start_time

            last_indexed_block = indexer.get_last_indexed_block()

            print(f"Block {block['height']} ({ellapsed:.2f}s) ({ellapsed_1000:.2f}s) ({last_indexed_block})", end='\r')

    except KeyboardInterrupt:
        print("\nGraceful shutdown in progress...")
    finally:
        block_fetcher.stop()
        indexer.stop()

if __name__ == "__main__":
    parse()
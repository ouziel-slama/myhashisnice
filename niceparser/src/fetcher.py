import threading
import queue
import json
import time

import requests

from nicefetcher import indexer

from config import Config


class RSFetcher:
    def __init__(self, start_height=0):
        self.fetcher = indexer.Indexer(
            {
                "rpc_address": Config()["BACKEND_URL"],
                "rpc_user": "rpc",
                "rpc_password": "rpc",
                "db_dir": Config()["FETCHER_DB"],
                "start_height": start_height,
                "log_file": "/tmp/fetcher.log",
                "only_write_in_reorg_window": True,
            }
        )
        self.fetcher.start()
        self.prefeteched_block = queue.Queue(maxsize=10)
        self.prefetched_count = 0
        self.stopped_event = threading.Event()
        self.executors = []
        for _i in range(1):
            executor = threading.Thread(
                target=self.prefetch_block, args=(self.stopped_event,)
            )
            executor.daemon = True
            executor.start()
            self.executors.append(executor)

    def get_next_block(self, timeout=1.0):
        """
        Get the next block from the queue
        
        Args:
            timeout (float): Timeout in seconds to wait for a block
            
        Returns:
            dict or None: Block data or None if no block is available or if stopped
        """
        if self.stopped_event.is_set():
            return None  # Return None immediately if stopped
            
        try:
            return self.prefeteched_block.get(timeout=timeout)
        except queue.Empty:
            return None  # Return None if no block is available within timeout

    def prefetch_block(self, stopped_event):
        """
        Thread function that prefetches blocks
        
        Args:
            stopped_event (threading.Event): Event to signal thread to stop
        """
        while not stopped_event.is_set():
            # Check the queue size and skip if full
            if self.prefeteched_block.qsize() >= 8:  # Marge de sécurité par rapport à maxsize=10
                # Add sleep to prevent CPU spinning
                if stopped_event.wait(timeout=0.2):  # Vérifie l'arrêt toutes les 0.2 secondes
                    break  # Exit immediately if stopped
                continue

            # Get a block (non-blocking)
            try:
                block = self.fetcher.get_block_non_blocking()
            except Exception as e:
                print(f"Error fetching block: {e}")
                if stopped_event.wait(timeout=0.2):
                    break
                continue
                
            # If no block available, wait and continue
            if block is None:
                if stopped_event.wait(timeout=0.2):
                    break
                continue

            # Process the block
            block["tx"] = block.pop("transactions")
            
            # Try to add to queue with timeout
            attempt_count = 0
            while not stopped_event.is_set() and attempt_count < 5:  # Limite les tentatives
                try:
                    # Réduire le timeout pour être plus réactif à l'arrêt
                    success = self.prefeteched_block.put(block, timeout=0.5)
                    break
                except queue.Full:
                    attempt_count += 1
                    if stopped_event.wait(timeout=0.2):
                        break

    def stop(self):
        """Stop all threads and resources"""
        print("Arrêt du fetcher en cours...")
        
        # Signal all threads to stop
        self.stopped_event.set()
        
        # Stop the fetcher
        try:
            print("Arrêt de l'indexer sous-jacent...")
            self.fetcher.stop()
        except Exception as e:
            print(f"Erreur lors de l'arrêt de l'indexer: {e}")
        
        # Wait for executor threads to finish (with timeout)
        print(f"Attente de la fin des {len(self.executors)} threads...")
        max_wait = 5  # Attendre au maximum 5 secondes
        start_time = time.time()
        
        for i, thread in enumerate(self.executors):
            remaining_time = max(0, max_wait - (time.time() - start_time))
            if thread.is_alive():
                thread.join(timeout=remaining_time)
                if thread.is_alive():
                    print(f"Le thread {i} ne s'est pas terminé proprement")
        
        # Vider la queue pour éviter tout blocage
        try:
            while True:
                self.prefeteched_block.get_nowait()
        except queue.Empty:
            pass
            
        print("Fetcher arrêté")


class BitcoindRPCError(Exception):
    pass


def rpc_call(method, params):
    try:
        payload = {
            "method": method,
            "params": params,
            "jsonrpc": "2.0",
            "id": 0,
        }
        response = requests.post(
            Config()["BACKEND_URL"],
            data=json.dumps(payload),
            headers={"content-type": "application/json"},
            verify=(not Config()["BACKEND_SSL_NO_VERIFY"]),
            timeout=Config()["REQUESTS_TIMEOUT"],
        ).json()
        if "error" in response and response["error"]:
            print(response)
            raise BitcoindRPCError(f"Error calling {method}: {response['error']}")
        return response["result"]
    except (
        requests.exceptions.RequestException,
        json.decoder.JSONDecodeError,
        KeyError,
    ) as e:
        raise BitcoindRPCError(f"Error calling {method}: {str(e)}") from e


def deserialize_block(block_hex, block_index):
    deserializer = indexer.Deserializer(
        {
            "rpc_address": "",
            "rpc_user": "",
            "rpc_password": "",
            "network": Config().network_name,
            "db_dir": "",
            "log_file": "",
            "prefix": b"prefix",
        }
    )
    decoded_block = deserializer.parse_block(block_hex, block_index)
    decoded_block["tx"] = decoded_block.pop("transactions")
    return decoded_block


def get_block_rpc(block_height):
    block_hash = rpc_call("getblockhash", [block_height])
    raw_block = rpc_call("getblock", [block_hash, 0])
    decoded_block = deserialize_block(raw_block, block_height)
    return decoded_block
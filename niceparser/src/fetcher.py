
import threading
import queue
import json

import requests

from nicefetcher import indexer

from config import Config

class RSFetcher():
    def __init__(self, start_height=0):
        self.fetcher = indexer.Indexer({
            "rpc_address": Config()["BACKEND_URL"],
            "rpc_user": "rpc",
            "rpc_password": "rpc",
            "db_dir": Config()["FETCHER_DB"],
            "start_height": start_height,
            "log_file": "/tmp/fetcher.log",
            "only_write_in_reorg_window": True,
        })
        self.fetcher.start()
        self.prefeteched_block = queue.Queue(maxsize=10)
        self.prefetched_count = 0
        self.stopped_event = threading.Event()
        self.executors = []
        for _i in range(2):
            executor = threading.Thread(target=self.prefetch_block, args=(self.stopped_event,))
            executor.daemon = True
            executor.start()
            self.executors.append(executor)

    def get_next_block(self):
        while not self.stopped_event.is_set():
            try:
                return self.prefeteched_block.get(timeout=1)
            except queue.Empty:
                self.stopped_event.wait(timeout=0.1)
                continue
        return None  # Return None only if stopped
    
    def prefetch_block(self, stopped_event):
        while not stopped_event.is_set():
            if self.prefeteched_block.qsize() > 10:
                # Add sleep to prevent CPU spinning
                stopped_event.wait(timeout=0.1)
                continue
                
            block = self.fetcher.get_block_non_blocking()
            if block is None:
                # Add sleep when no block is available
                stopped_event.wait(timeout=0.1)
                continue
                
            block["tx"] = block.pop("transactions")
            while not stopped_event.is_set():
                try:
                    self.prefeteched_block.put(block, timeout=1)
                    break
                except queue.Full:
                    # Using event.wait instead of time.sleep
                    stopped_event.wait(timeout=0.1)
    
    def stop(self):
        self.stopped_event.set()
        try:
            self.fetcher.stop()
        except Exception as e:
            pass



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




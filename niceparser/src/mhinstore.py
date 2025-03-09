import os
from mhinstorage import MhinStorage
from mhindatabase import MhinDatabase


class MhinStore:
    """
    Unified class that coordinates MhinStorage and MhinDatabase
    for efficient block management and indexing
    """

    def __init__(self, base_path):
        """
        Initializes the MhinStore instance

        Args:
            base_path (str): Base path for files and databases
        """
        self.base_path = base_path

        # Create the base directory if it does not exist
        os.makedirs(base_path, exist_ok=True)

        # Initialize components
        self.storage = MhinStorage(base_path)
        self.database = MhinDatabase(base_path)

        # Information about the current block
        self.current_block = 0
        self.current_block_hash = None
        self.current_transaction = None
        self.current_block_transactions = []
        self.current_transaction_started = False

    def get_last_indexed_block(self):
        """
        Gets the height of the last indexed block

        Returns:
            int: Height of the last indexed block
        """
        return self.database.get_last_indexed_block()

    def start_block(self, height, block_hash):
        """
        Starts processing a new block

        Args:
            height (int): Height of the block
            block_hash (str): Hash of the block
        """
        self.current_block = height
        self.current_block_hash = block_hash
        self.current_block_transactions = []

        # Start the block in storage
        self.storage.start_block(height, block_hash)

    def start_transaction(self, txid, reward=0):
        """
        Starts a new transaction in the current block

        Args:
            txid (bytes): Transaction ID
            reward (int, optional): MHIN reward. Defaults to 0.
        """
        # Create a new transaction
        self.current_transaction = {"txid": txid, "reward": reward, "balance_ops": []}
        self.current_transaction_started = False

    def add_balance(self, utxo_id, balance):
        """
        Adds a balance for a UTXO

        Args:
            utxo_id (bytes): UTXO ID
            balance (int): Amount of the balance
        """
        if balance == 0:
            return
        # Start the transaction in storage
        if not self.current_transaction_started:
            self.storage.start_transaction(self.current_transaction["txid"], self.current_transaction["reward"])
            self.current_transaction_started = True
        
        # Add to storage
        self.storage.add_balance(utxo_id, balance)

        # Record for database processing
        if self.current_transaction is not None:
            self.current_transaction["balance_ops"].append(
                {"type": "add", "utxo_id": utxo_id, "balance": balance}
            )

    def pop_balance(self, utxo_id):
        """
        Removes a balance for a UTXO

        Args:
            utxo_id (bytes): UTXO ID

        Returns:
            int: Amount removed
        """
        # Remove from storage
        balance = self.storage.pop_balance(utxo_id)

        # Record for database processing
        if balance >0:
            if not self.current_transaction_started:
                self.storage.start_transaction(self.current_transaction["txid"], self.current_transaction["reward"])
                self.current_transaction_started = True

            self.current_transaction["balance_ops"].append(
                {"type": "remove", "utxo_id": utxo_id, "balance": balance}
            )

        return balance

    def end_transaction(self):
        """
        Ends the current transaction
        """
        # Start the transaction in storage
        if not self.current_transaction_started:
            self.current_transaction = None
            return

        if self.current_transaction is not None:
            self.current_block_transactions.append(self.current_transaction)
            self.current_transaction = None

    def end_block(self):
        """
        Finalizes the current block

        Returns:
            bool: True if the block was processed successfully, False otherwise
        """
        # End the current transaction if it exists
        self.end_transaction()

        # Finalize the block in storage
        block_info = self.storage.end_block()

        # Process the block in the database
        try:
            self.database.process_block(block_info, self.current_block_transactions)
            self.current_block_transactions = []
            return True
        except Exception as e:
            import traceback

            print(f"Error processing block {self.current_block}: {e}")
            print(traceback.format_exc())
            return False

    def rollback_block(self, height):
        """
        Rolls back a block

        Args:
            height (int): Height of the block to roll back

        Returns:
            bool: True if the rollback succeeded, False otherwise
        """
        # Rollback in storage
        rollback_operations = self.storage.rollback_block(height)
        if isinstance(rollback_operations, bool) and not rollback_operations:
            return False

        # Rollback in the database
        db_success = self.database.rollback_block(height, rollback_operations)
        if not db_success:
            return False

        # Update the current block
        self.current_block = height - 1
        self.current_block_transactions = []

        return True

    def update_stats(self):
        """
        Updates global statistics
        """
        self.database.update_global_stats()

    def close(self):
        """
        Closes the resources
        """
        self.storage.close()
        self.database.close()

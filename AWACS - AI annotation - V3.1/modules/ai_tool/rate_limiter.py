import time
import random
from multiprocessing import Manager

class Yoda:
    def __init__(self, key_list, rpm_limit, manager):
        """
        Yoda: The Master of Rate Limits.
        
        Args:
            key_list: List of API keys.
            rpm_limit: Requests per minute limit.
            manager: The multiprocessing.Manager object from the main process.
        """
        # We use the passed manager to create shared objects
        # BUT we do NOT store 'self.manager = manager' because the Manager object 
        # itself cannot be pickled/sent to workers.
        
        # Shared Dictionary: Stores { key_index: {'count': 0, 'window_start': time.time()} }
        self.ledger = manager.dict()
        self.lock = manager.Lock()
        
        self.rpm_limit = rpm_limit
        self.all_keys = [k['original_index'] for k in key_list]
        
        # Initialize ledger for all keys
        for k in self.all_keys:
            self.ledger[k] = {'count': 0, 'window_start': time.time()}

    def get_usable_key(self, current_key_idx):
        """
        1. Checks if current_key_idx is free.
        2. If busy, SEARCHES for any free key (Smart Swap).
        3. If ALL busy, returns (None, wait_time).
        4. If found, returns (key_index, 0).
        """
        with self.lock:
            now = time.time()
            
            # --- 1. Check Current Key First ---
            if self._check_key_status(current_key_idx, now):
                self._increment_key(current_key_idx)
                return current_key_idx, 0
            
            # --- 2. Smart Swap: Find ANY free key ---
            # Shuffle list to prevent all workers fighting for Key #1
            shuffled_keys = list(self.all_keys)
            random.shuffle(shuffled_keys)
            
            for k_idx in shuffled_keys:
                if k_idx == current_key_idx: continue # Already checked
                
                if self._check_key_status(k_idx, now):
                    self._increment_key(k_idx)
                    return k_idx, 0
            
            # --- 3. Total Saturation: Calculate Wait Time ---
            # If we are here, ALL keys are maxed out.
            # Find the key that resets soonest.
            min_wait = 60.0
            for k_idx in self.all_keys:
                data = self.ledger[k_idx]
                time_passed = now - data['window_start']
                wait = max(0, 60 - time_passed)
                if wait < min_wait:
                    min_wait = wait
            
            return None, min_wait + 1.0 # Add 1s buffer

    def _check_key_status(self, key_idx, now):
        """Internal: Returns True if key has capacity."""
        data = self.ledger[key_idx]
        
        # Reset Window if 60s passed
        if now - data['window_start'] >= 60:
            self.ledger[key_idx] = {'count': 0, 'window_start': now}
            return True
            
        # Check Capacity
        if data['count'] < self.rpm_limit:
            return True
            
        return False

    def _increment_key(self, key_idx):
        """Internal: Increments usage count."""
        data = self.ledger[key_idx]
        data['count'] += 1
        self.ledger[key_idx] = data # Write back to shared dict
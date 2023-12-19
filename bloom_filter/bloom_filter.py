# src/bloom_filter/bloom_filter.py
import hashlib
from typing import List, Dict


class BloomFilter:

    bit_array: List[int]
    data: Dict[str, int]
    size: int

    def __init__(self, size: int) -> None:
        self.size = size
        self.bit_array = size * [0]
        self.data = {}

    def __contains__(self, key: str) -> bool:
        hashes = self._generate_hashes(key)

        for hash_value in hashes:
            index = hash_value % self.size
            if self.bit_array[index] == 0:
                return False

        return True

    @staticmethod
    def _generate_hashes(data: str) -> List:
        hashes = [
            int(hashlib.md5(data.encode()).hexdigest(), 16),
            int(hashlib.sha1(data.encode()).hexdigest(), 16),
            int(hashlib.shake_128(data.encode()).hexdigest(20), 16),
        ]
        return hashes

    def insert(self, key: str, value: int) -> None:
        hashes = self._generate_hashes(key)
        
        for hash_value in hashes:
            index = hash_value % self.size
            self.bit_array[index] = 1
        
        self.data[key] = value

    def retrieve(self, key: str):
        return self.data.get(key, None)

# src/consistent_hashing/consistent_hashing.py
from typing import List, Dict, Tuple

from .utils import h


class HashNode:
    # Attributes
    uid: int | str
    huid: str
    data: Dict[str, int]

    # Limits
    MAX_KEYS: int = 100000
    MIN_KEYS: int = 0

    def __init__(self, uid: int | str) -> None:
        self.id = uid
        self.huid = h(uid)
        self.data = {}

    def __lt__(self, other) -> bool:
        return self.huid < other.huid

    def __gt__(self, other) -> bool:
        return self.huid > other.huid

    def __str__(self) -> str:
        return f"[{self.uid}/{self.huid}[:5]]"

    def __repr__self(self) -> str:
        return self.__str__()

    def get_data_count(self) -> int:
        return len(self.data)

    def get_all_data(self) -> List[Tuple[str, int]]:
        return list(self.data.items())

    def retrieve_data(self, key) -> int | None:
        self._perform_retrieve_validations(key)
        return self.data.get(key)

    def store_data(self, key: str, value: int) -> bool:
        self._perform_store_validations(key)
        self.data[key] = value
        return True

    def delete_data(self, key: str):
        del self.data[key]

    def _perform_retrieve_validations(self, key: str) -> None:
        if key not in self.data:
            error_message = f"Node {self.id} does not contain key: {key}"
            raise KeyError(error_message)

    def _perform_store_validations(self, key: str, override=False) -> None:
        if len(self.data) >= self.MAX_KEYS:
            error_message = (
                f"Node {self.id} reached maximum key count ({self.MAX_KEYS})"
            )
            raise ValueError(error_message)

        if not override and key in self.data:
            error_message = f"Node {self.id} already contains key: {key}"
            raise KeyError(error_message)


class HashRing:
    # Attributes
    node_list: List[HashNode]

    # Limits
    MAX_NODES: int = 100
    MIN_NODES: int = 0

    def __init__(self):
        self.node_list = []

    def get_node_count(self) -> int:
        return len(self.node_list)

    def add_node(self, node: HashNode) -> bool:
        """
        Adds a 'HashNode' to this consistent hashing ring.
        NOTE: First implementation using 'List',
              improve with binary tree.
        """
        self._perform_validations(node)

        self.node_list.append(node)
        self.node_list.sort()

        # Find next sequential clockwise node
        try:
            next_node_index: int = self.node_list.index(node) + 1
            next_node: HashNode = self.node_list[next_node_index]
        except IndexError:
            next_node: HashNode = self.node_list[0]

        # Migrate all corresponding data
        for next_node_key, next_node_value in next_node.get_all_data():
            if h(next_node_key) < node.huid:
                node.store_data(next_node_key, next_node_value)
                next_node.delete_data(next_node_key)

        return True

    def remove_node(self, node: HashNode) -> bool:
        # Find next sequential clockwise node
        try:
            next_node_index = self.node_list.index(node) + 1
            next_node = self.node_list[next_node_index]
        except IndexError:
            next_node = self.node_list[0]

        # Migrate all data and free memory
        [next_node.store_data(k, v) for k, v in node.get_all_data()]
        self.node_list.remove(node)

        return True

    def get_data(self, data_key: str) -> int:
        """
        Get data from appropriate node in ring.
        Raises 'KeyError' if key is not in any node.
        """
        hashed_data_key = h(data_key)
        node = self._find_node(hashed_data_key)
        return node.retrieve_data(hashed_data_key)

    def set_data(self, data_key: str, data_value: int) -> bool:
        """
        Store data in appropriate node in ring.
        Raises 'ValueError' if corresponding node reached its max data limit.
        Raise 'KeyError' if 'override=False' and key already exists in node.
        """
        hashed_data_key = h(data_key)
        node = self._find_node(hashed_data_key)
        return node.store_data(data_key, data_value)

    def _find_node(self, hashed_key: str) -> HashNode:
        for node in self.node_list:
            if hashed_key < node.huid:
                return node
        # Wrap around and return to complete clockwise search
        return self.node_list[0]

    def _perform_validations(self, node: HashNode):
        if len(self.node_list) >= self.MAX_NODES:
            raise ValueError

        if node.huid in self.node_list:
            raise KeyError

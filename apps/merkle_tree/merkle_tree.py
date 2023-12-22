# src/merkle_tree/merkle_tree.py
import json
import math
from datetime import datetime
from typing import List, Dict, Set


class MerkleTree:

    # Attributes
    tree: List[int]
    tree_depth: int
    bucket_index_shift: int  # Shortcut to find leaf with correct bucket
    bucket_list: List[Dict[str, int]]
    last_update: datetime

    def __init__(self, bucket_count: int, key_space: int) -> None:
        self.tree_depth = math.ceil(math.log(bucket_count, 2))
        self.tree = [0] * (2 ** (self.tree_depth + 1))
        self.bucket_index_shift = (2 ** self.tree_depth) - 1
        self.bucket_list = [{} for _ in range(bucket_count)]

    def __eq__(self, other):
        return self.tree[0] == other.tree[0]

    def _touch(self):
        self.last_update = datetime.utcnow()

    def insert(self, bucket_number: int, key: str, value: int) -> None:
        self._perform_insert_validations(bucket_number)
        self.bucket_list[bucket_number][key] = value

        self._update_bucket_hash(bucket_number)
        self._propagate(bucket_number)
        self._touch()

    def retrieve(self, bucket_number: int, key: str) -> int:
        return self.bucket_list[bucket_number][key]

    def remove(self, bucket_number: int, key: str) -> None:
        del self.bucket_list[bucket_number][key]

        self._update_bucket_hash(bucket_number)
        self._propagate(bucket_number)
        self._touch()

    def _update_bucket_hash(self, bucket_number: int):
        bucket_hash: int = hash(json.dumps(self.bucket_list[bucket_number], sort_keys=True))
        tree_index: int = self.bucket_index_shift + bucket_number
        self.tree[tree_index] = bucket_hash

    def _propagate(self, bucket_number: int) -> None:
        """
        Traverses up the parents from the :bucket_number and
        updates along the way, pulling the two children of
        every parent until root.
        """
        tree_index: int = self.bucket_index_shift + bucket_number
        parent_index: int = self._get_parent_index(tree_index)

        while parent_index >= 0:
            left_child_hash: int = self.tree[2 * parent_index + 1]
            right_child_hash: int = self.tree[2 * parent_index + 2]
            self.tree[parent_index] = hash(left_child_hash + right_child_hash)
            parent_index: int = self._get_parent_index(parent_index)

    def replicate_from(self, other):
        if self == other:
            return False

        modified_nodes_list: List[int] = [0]
        self._traverse_and_replicate(0, other, modified_nodes_list)
        self._update_modified_nodes(modified_nodes_list)
        self._touch()

    def _traverse_and_replicate(self, tree_index, other, modified_nodes_list):
        left_child_index: int = 2 * tree_index + 1
        right_child_index: int = 2 * tree_index + 2
        leaf = False

        # Replicate bucket and return if reached leaf node
        for child_index in [left_child_index, right_child_index]:
            if child_index - self.bucket_index_shift >= 0:
                self._replicate_bucket(child_index, other)
                leaf = True
        if leaf:
            return

        # Traverse left and right side if appropriate
        for child_index in [left_child_index, right_child_index]:
            if self.tree[child_index] != other.tree[child_index]:
                modified_nodes_list.append(child_index)
                self._traverse_and_replicate(child_index, other, modified_nodes_list)

    def _replicate_bucket(self, tree_index, other):
        bucket_number: int = tree_index - self.bucket_index_shift
        this_bucket: Dict[str, int] = self.bucket_list[bucket_number]
        other_bucket: Dict[str, int] = other.bucket_list[bucket_number]
        this_bucket.update(other_bucket)
        self._update_bucket_hash(bucket_number)

    def _update_modified_nodes(self, modified_nodes_list: List[int]):
        for index in reversed(modified_nodes_list):
            left_child_hash: int = self.tree[2 * index + 1]
            right_child_hash: int = self.tree[2 * index + 2]
            self.tree[index] = hash(left_child_hash + right_child_hash)

    def _perform_replicate_validations(self, other) -> None:
        if self.tree_depth != other.tree_depth:
            raise ValueError

    def _perform_insert_validations(self, bucket_number: int) -> None:
        if bucket_number > len(self.bucket_list):
            raise KeyError

    @staticmethod
    def _get_parent_index(index) -> int:
        return int((index - 1) / 2 if index % 2 else (index - 2) / 2)

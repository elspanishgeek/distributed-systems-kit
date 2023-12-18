# src/chasing/tests.py
import random
from functools import reduce
from typing import List

import pytest
from django.test import TestCase

from ..schema import HashNode, HashRing


# Test Utils
def gen_word():
    length: int = random.randint(5, 26)
    return 'KEY_' + ''.join(
        [
            random.choice([chr(random.randint(65, 90)), chr(random.randint(97, 123))])
            for _ in range(length)
        ]
    )


def build_hash_ring(node_count: int, data_count: int):
    # Create Nodes, Ring, and populate with data
    node_list: List = [HashNode(gen_word()) for _ in range(node_count)]
    hash_ring: HashRing = HashRing()
    [hash_ring.add_node(node) for node in node_list]
    [hash_ring.set_data(gen_word(), random.randint(0, 999999)) for _ in range(data_count)]
    return hash_ring


class TestSuite(TestCase):
    def test_testsuite_should_populate_hash_ring(self):
        # Build test data
        node_count: int = 10
        data_count: int = 100
        hash_ring: HashRing = build_hash_ring(node_count, data_count)

        assert hash_ring.get_node_count() == node_count

    def test_should_add_nodes_in_order(self):
        # Build test data
        node_count: int = HashRing.MAX_NODES
        data_count: int = 10
        hash_ring: HashRing = build_hash_ring(node_count, data_count)

        # Validate each node's hashed id  is 'less than' the following node's
        for i in range(node_count - 2):
            assert hash_ring.node_list[i].huid < hash_ring.node_list[i + 1].huid
        assert hash_ring.node_list[data_count - 1].huid > hash_ring.node_list[0].huid

    def test_should_add_data_to_node(self):
        # Build test data
        node_count: int = HashRing.MIN_NODES + 1
        data_count: int = 100
        hash_ring: HashRing = build_hash_ring(node_count, data_count)

        # Validate setup
        initial_node: HashNode = hash_ring.node_list[0]
        assert initial_node.get_data_count() == data_count

    def test_should_migrate_data_when_adding_one_node(self):
        # Build test data
        node_count: int = HashRing.MIN_NODES + 1
        data_count: int = 10000
        hash_ring: HashRing = build_hash_ring(node_count, data_count)
        initial_node: HashNode = hash_ring.node_list[0]

        # Do
        test_node: HashNode = HashNode(gen_word())
        hash_ring.add_node(test_node)

        # Assert
        assert initial_node.get_data_count() < data_count

    def test_should_migrate_when_adding_multiple_nodes(self):
        # Build test data
        node_count: int = HashRing.MIN_NODES + 1
        data_count: int = HashNode.MAX_KEYS
        hash_ring: HashRing = build_hash_ring(node_count, data_count)

        # Do
        test_node_count: int = HashRing.MAX_NODES - 1
        [hash_ring.add_node(HashNode(gen_word())) for _ in range(test_node_count)]

        # Assert
        total_data_count_across_all_nodes = reduce(
            lambda acc, node: acc + node.get_data_count(), hash_ring.node_list, 0
        )
        assert total_data_count_across_all_nodes == data_count

    def test_should_not_exceed_node_key_limit(self):
        # Build test data
        data_count: int = HashNode.MAX_KEYS + 1
        test_node: HashNode = HashNode(gen_word())
        hash_ring: HashRing = HashRing()
        hash_ring.add_node(test_node)
        
        print(f' $$$ DATA COUNT: {data_count}')
        # Do
        with pytest.raises(ValueError) as exception:
            [hash_ring.set_data(gen_word(), random.randint(0, 999999)) for _ in range(data_count)]

        # Assert
        assert test_node.id in str(exception.value)

    def test_should_not_exceed_node_count_limit(self):
        # Build test data
        node_count: int = HashRing.MAX_NODES + 1
        node_list: List = [HashNode(gen_word()) for _ in range(node_count)]
        hash_ring: HashRing = HashRing()

        with pytest.raises(ValueError) as exception:
            [hash_ring.add_node(node) for node in node_list]

        assert exception

    def test_should_migrate_when_removing_one_node(self):
        # Build test data
        node_count: int = HashRing.MIN_NODES + 2
        data_count: int = 10000
        hash_ring: HashRing = build_hash_ring(node_count, data_count)

        # Do
        node_to_remove: HashNode = hash_ring.node_list[1]
        hash_ring.remove_node(node_to_remove)

        # Assert
        assert node_to_remove not in hash_ring.node_list

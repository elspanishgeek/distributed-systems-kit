import random
import pytest
from django.test import TestCase

from ..merkle_tree import MerkleTree


# Test Utils
def gen_word():
    length: int = random.randint(5, 26)
    return ''.join(
        [
            random.choice([chr(random.randint(65, 90)), chr(random.randint(97, 123))])
            for _ in range(length)
        ]
    )


def gen_data_tuple():
    return 'KEY_' + gen_word(), random.randint(0, 999999)


class MyTestCase(TestCase):
    def test_should_create_tree(self):
        assert MerkleTree(4, 16)

    def test_should_insert_data(self):
        tree = MerkleTree(4, 16)
        # In all buckets
        tree.insert(0, '0a', 1)
        tree.insert(1, '0b', 2)
        tree.insert(2, '0c', 3)
        tree.insert(3, '0d', 4)

        # Then again
        tree.insert(0, '1a', 1)
        tree.insert(1, '2b', 2)
        tree.insert(2, '3c', 3)
        tree.insert(3, '4d', 4)
        assert True

    def test_should_retrieve_data(self):
        tree = MerkleTree(4, 16)
        # In all buckets
        tree.insert(0, '0a', 1)
        tree.insert(1, '0b', 2)
        tree.insert(2, '0c', 3)
        tree.insert(3, '0d', 4)

        # Then again
        tree.insert(0, '1a', 1)
        tree.insert(1, '1b', 2)
        tree.insert(2, '1c', 3)
        tree.insert(3, '1d', 4)

        assert tree.retrieve(2, '0c') == 3
        assert tree.retrieve(2, '1c') == 3

    def test_should_not_retrieve_data_if_wrong_bucket(self):
        tree = MerkleTree(4, 16)
        tree.insert(0, '0a', 1)
        tree.insert(3, '1d', 4)

        with pytest.raises(IndexError):
            tree.retrieve(10, '1c')

    def test_should_not_retrieve_data_if_wrong_key(self):
        tree = MerkleTree(4, 16)
        tree.insert(0, '0a', 1)
        tree.insert(3, '1d', 4)

        with pytest.raises(KeyError):
            tree.retrieve(3, '1c')

    def test_should_return_equal_if_correct(self):
        tree_one = MerkleTree(4, 16)
        tree_two = MerkleTree(4, 16)
        tree_one.insert(0, '0a', 1)
        tree_two.insert(0, '0a', 1)

        assert tree_one == tree_two

    def test_should_not_return_equal_if_incorrect(self):
        tree_one = MerkleTree(4, 16)
        tree_two = MerkleTree(4, 16)
        tree_one.insert(0, '0a', 1)
        tree_two.insert(0, '0a', 2)

        assert tree_one != tree_two

    def test_should_replicate_correctly(self):
        tree_one = MerkleTree(4, 16)
        tree_two = MerkleTree(4, 16)
        tree_one.insert(0, '0a', 1)
        tree_two.insert(2, '0b', 2)
        tree_two.insert(3, '0c', 3)
        tree_two.insert(3, '0d', 3)

        assert tree_one != tree_two
        tree_one.replicate_from(tree_two)
        tree_two.replicate_from(tree_one)
        assert tree_one == tree_two

    def test_should_insert_big(self):
        bucket_count: int = 1024
        key_space: int = 2**14
        tree = MerkleTree(bucket_count, key_space)

        [tree.insert(random.randint(0, 1023), *gen_data_tuple()) for _ in range(key_space)]
        tree.insert(1000, 'test_key', 7)

        assert tree.retrieve(1000, 'test_key') == 7

    def test_should_replicate_correctly_big(self):
        bucket_count: int = 2**18
        key_space: int = 2**14
        tree_one = MerkleTree(bucket_count, key_space)
        tree_two = MerkleTree(bucket_count, key_space)

        [tree_one.insert(random.randint(0, bucket_count - 1), *gen_data_tuple()) for _ in range(key_space)]
        [tree_two.insert(random.randint(0, bucket_count - 1), *gen_data_tuple()) for _ in range(key_space)]

        assert tree_one != tree_two
        tree_one.replicate_from(tree_two)
        tree_two.replicate_from(tree_one)
        assert tree_one == tree_two


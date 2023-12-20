import pytest
from django.test import TestCase

from ..merkle_tree import MerkleTree


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

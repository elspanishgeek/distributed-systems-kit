import random
from typing import List, Tuple

from django.test import TestCase

from ..bloom_filter import BloomFilter


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


class TestSuite(TestCase):

    def test_should_add_data(self):
        # Build test data
        size: int = 10
        data_count: int = 100
        bfilter: BloomFilter = BloomFilter(size)

        # Do
        [bfilter.insert(*gen_data_tuple()) for _ in range(data_count)]

        # Assert
        assert len(bfilter.data) == data_count

    def test_should_contain_data(self):
        # Build test data
        size: int = 10
        data_count: int = 1000
        bfilter: BloomFilter = BloomFilter(size)
        generated_data: List[Tuple[str, int]] = [gen_data_tuple() for _ in range(data_count)]

        # Do
        for key, value in generated_data:
            bfilter.insert(key, value)

        for idx, (key, value) in enumerate(generated_data):
            assert key in bfilter

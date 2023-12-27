from unittest import TestCase
from django.conf import settings

from ..map_reduce import MapReduce


class TestSuite(TestCase):
    FILE_PATH = settings.APPS_DIR / 'map_reduce/tests/sample.log'

    def test_should_process_sample_log_with_many_threads(self):
        # Build data
        mappers = 3
        reducers = 4
        mapreduce = MapReduce(mappers, reducers)

        # Do
        mapreduce.process_log(self.FILE_PATH)

        # Assert
        assert mapreduce.processed_log['by_code']['422']['paths']['/process'] == 4
        assert mapreduce.processed_log['by_timestamp']['2023-03-14']['codes']['200'] == 2
        assert mapreduce.processed_log['by_timestamp']['2023-03-14']['codes']['503'] == 2
        assert mapreduce.processed_log['by_path']['/login']['timestamps']['2023-02-20'] == 1
        assert mapreduce.processed_log['by_uid']['5xGm3HsW']['paths']['/about'] == 236

    def test_should_process_sample_log_with_one_thread(self):
        # Build data
        mappers = 1
        reducers = 1
        mapreduce = MapReduce(mappers, reducers)

        # Do
        mapreduce.process_log(self.FILE_PATH)

        # Assert
        assert mapreduce.processed_log['by_code']['422']['paths']['/process'] == 4
        assert mapreduce.processed_log['by_timestamp']['2023-03-14']['codes']['200'] == 2
        assert mapreduce.processed_log['by_timestamp']['2023-03-14']['codes']['503'] == 2
        assert mapreduce.processed_log['by_path']['/settings']['codes']['200'] == 244
        assert mapreduce.processed_log['by_uid']['5xGm3HsW']['paths']['/about'] == 236

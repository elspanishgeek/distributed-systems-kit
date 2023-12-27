from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict


class MapReduce:
    # Constants
    DATE_FORMAT_LENGTH = 19
    INPUT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    OUTPUT_DATE_FORMAT = "%Y-%m-%d"
    LOG_DELIMITER = ' | '
    LOG_TIMESTAMP_POSITION = 0
    LOG_UUID_POSITION = 2
    LOG_PATH_POSITION = 3
    LOG_CODE_POSITION = 4

    # Attributes
    mappers: int
    reducers: int
    processed_log: Dict = {
        'by_code': {},
        'by_timestamp': {},
        'by_path': {},
        'by_uid': {},
    }

    def __init__(self, mappers: int, reducers: int) -> None:
        self.mappers = mappers
        self.reducers = reducers

    def process_log(self, file_path: str, lines_per_chunk: int = 200, log_line_limit: int = 5000):
        # MapReduce: Dividing and Mapping Phase
        mapped_by_code = {}
        mapped_by_timestamp = {}
        mapped_by_path = {}
        mapped_by_uid = {}

        with ThreadPoolExecutor(max_workers=self.mappers) as mapper_executor:
            mapping_futures_list = []

            try:
                with open(file_path, 'r') as file:
                    total_lines_count = 0
                    lines_count = 0
                    chunk = []
                    # Read one line at a time in case log is too large
                    for line in file:
                        chunk.append(line)
                        lines_count += 1
                        total_lines_count += 1
                        if lines_count > lines_per_chunk:
                            mapping_futures_list.append(mapper_executor.submit(self._mapping_task, chunk))
                            lines_count = 0
                            chunk = []
                        if total_lines_count > log_line_limit:
                            print(f'Reached file processing limit ({log_line_limit} lines).')
                            break

            except FileNotFoundError:
                print(f'File not found: {file_path}')
            except ValueError as error:
                print(f'Error while processing the file: {error}')

            # MapReduce: Shuffling and Merging Phase
            for mapping_task_future in as_completed(mapping_futures_list):
                mapped_result = mapping_task_future.result()

                for key, val in mapped_result['by_code'].items():
                    mapped_by_code.setdefault(key, []).extend(val)

                for key, val in mapped_result['by_timestamp'].items():
                    mapped_by_timestamp.setdefault(key, []).extend(val)

                for key, val in mapped_result['by_path'].items():
                    mapped_by_path.setdefault(key, []).extend(val)

                for key, val in mapped_result['by_uid'].items():
                    mapped_by_uid.setdefault(key, []).extend(val)

        # MapReduce: Reducing Phase
        with ThreadPoolExecutor(max_workers=self.reducers) as reducer_executor:
            reducers_futures_dict = {
                reducer_executor.submit(self._reducing_task, mapped_by_code): 'by_code',
                reducer_executor.submit(self._reducing_task, mapped_by_timestamp): 'by_timestamp',
                reducer_executor.submit(self._reducing_task, mapped_by_path): 'by_path',
                reducer_executor.submit(self._reducing_task, mapped_by_uid): 'by_uid',
            }

            for reducer_future in as_completed(reducers_futures_dict):
                self.processed_log[reducers_futures_dict[reducer_future]] = reducer_future.result()

        return True

    def _mapping_task(self, log_chunk):
        output = {
            'by_code': {},
            'by_timestamp': {},
            'by_path': {},
            'by_uid': {},
        }

        for log_line in log_chunk:
            try:
                # Clean and extract
                log_line = log_line.replace('\n', '')
                split_line = log_line.split(self.LOG_DELIMITER)
                uid = split_line[self.LOG_UUID_POSITION]
                path = split_line[self.LOG_PATH_POSITION]
                code = split_line[self.LOG_CODE_POSITION]
                timestamp = datetime.strptime(log_line[:self.DATE_FORMAT_LENGTH], self.INPUT_DATE_FORMAT)
                timestamp = timestamp.strftime(self.OUTPUT_DATE_FORMAT)

                # Do mapping
                output['by_code'].setdefault(code, []).append({'timestamp': timestamp, 'path': path})
                output['by_timestamp'].setdefault(timestamp, []).append({'code': code, 'path': path})
                output['by_path'].setdefault(path, []).append({'timestamp': timestamp, 'code': code})
                output['by_uid'].setdefault(uid, []).append({'timestamp': timestamp, 'code': code, 'path': path})
            except Exception as err:
                print(f'Skipping log line: {log_line}. Error: {err}')

        return output

    def _reducing_task(self, mapped_output):
        output = {}
        for key in mapped_output:
            data = {
                'timestamps': {},
                'paths': {},
                'codes': {},
                'uids': {},
            }
            for entry in mapped_output[key]:

                timestamp = entry.get('timestamp')
                if timestamp is not None:
                    data['timestamps'][timestamp] = data['timestamps'].setdefault(timestamp, 0) + 1

                path = entry.get('path')
                if path is not None:
                    data['paths'][path] = data['paths'].setdefault(path, 0) + 1

                code = entry.get('code')
                if code is not None:
                    data['codes'][code] = data['codes'].setdefault(code, 0) + 1

                uid = entry.get('uid')
                if uid is not None:
                    data['uids'][uid] = data['uids'].setdefault(uid, 0) + 1

            output[key] = data

        return output

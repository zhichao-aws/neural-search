import sys
import time
import functools
import logging

import diskcache
from datasets import load_dataset

cache = diskcache.Cache("./cache_dir")


def cached(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        cache_key = f"{func.__name__}_{args}_{kwargs}"
        if cache_key in cache:
            return cache[cache_key]

        result = func(*args, **kwargs)
        cache[cache_key] = result
        return result

    return wrapper


class Timer:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.execution_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.end_time = time.time()
        self.execution_time = self.end_time - self.start_time


def get_doc_number(index_name, client):
    indices = client.cat.indices(index=index_name, format="json")
    for index in indices:
        if index["index"] == index_name:
            return int(index["docs.count"])
    return 0


@cached
def get_beir_qrels(dataset, split="test"):
    dataset = load_dataset(f"BeIR/{dataset}-qrels", split=split)
    result = {}

    for item in dataset:
        query_id = str(item["query-id"])
        corpus_id = str(item["corpus-id"])
        score = item["score"]

        if query_id not in result:
            result[query_id] = {}

        result[query_id][corpus_id] = score

    return result


def set_logging(log_file_name):
    logging.basicConfig(
        level=20,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file_name),
        ],
    )

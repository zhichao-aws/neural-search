import os
import json
import sys
import logging

import yaml
import subprocess

from scripts.ingest import ingest
from scripts.search import search
from scripts.utils import Timer, get_beir_qrels, set_logging

from opensearchpy import OpenSearch
from beir.retrieval.evaluation import EvaluateRetrieval

PIPELINE_NAME = "nlp-ingest-pipeline-sparse"
EMBEDDING_FIELD = "sparse_embedding"
logger = logging.getLogger(__name__)


def prepare_index(client, index_name, fp):
    # 1. try to delete existing index
    # 2. create index with given name and mappings
    # 3. ingest documents
    #     a. refresh & flush

    try:
        client.indices.delete(index_name)
    except:
        pass

    client.indices.create(
        index_name,
        body={
            "settings": {
                "default_pipeline": PIPELINE_NAME,
                "index.number_of_shards": 3,
                "index.number_of_replicas": 0,
            },
            "mappings": {"properties": {EMBEDDING_FIELD: {"type": "rank_features"}}},
        },
    )

    ingest(
        index_name,
        fp,
        auth=None if os.getenv("AUTH") is None else os.getenv("AUTH").split(","),
    )


def prepare_ingest_processor(client, pruning_type, pruning_number):
    client.transport.perform_request(
        "PUT",
        f"/_ingest/pipeline/{PIPELINE_NAME}",
        body={
            "description": "An sparse encoding ingest pipeline",
            "processors": [
                {
                    "sparse_encoding": {
                        "pruning_type": pruning_type,
                        "pruning_number": pruning_number,
                        "model_id": "testid",
                        "field_map": {"id": EMBEDDING_FIELD},
                    }
                }
            ],
        },
    )


if __name__ == "__main__":
    last_ingest = None
    flag = True

    assert len(sys.argv) == 2 and sys.argv[1].endswith(".yaml")
    with open(sys.argv[1]) as f:
        configs = yaml.safe_load(f)
    set_logging("run.log")
    os.makedirs(configs.get("result_dir"), exist_ok=True)

    client = OpenSearch(
        hosts=os.getenv("HOSTS").split(","),
        http_auth=None if os.getenv("AUTH") is None else os.getenv("AUTH").split(","),
        verify_certs=False,
        ssl_show_warn=False,
    )

    for encoding_type in configs.get("encoding_types"):
        for beir_dataset in configs.get("beir_datasets"):
            index_name = beir_dataset
            corpus_fp = os.path.join(
                configs.get("encoding_dir"),
                "corpus",
                encoding_type,
                f"{beir_dataset}.jsonl",
            )
            queries_fp = os.path.join(
                configs.get("encoding_dir"),
                "queries",
                encoding_type,
                f"{beir_dataset}.jsonl",
            )
            with open(queries_fp) as f:
                queries = f.readlines()
            queries = [json.loads(line) for line in queries]
            queries = [(line["id"], line["sparse_embedding"]) for line in queries]
            qrels = get_beir_qrels(beir_dataset)

            for pruning in configs.get("pruning")["ingest"]:
                pruning_type = pruning["pruning_type"]
                for pruning_number in pruning["pruning_number"]:
                    if not flag:
                        if [
                            encoding_type,
                            beir_dataset,
                            pruning_type,
                            pruning_number,
                        ] != last_ingest:
                            continue
                        flag = True
                    else:
                        logger.info(
                            f"start ingest for {beir_dataset}, with {pruning_type} and {pruning_number}"
                        )
                        prepare_ingest_processor(client, pruning_type, pruning_number)
                        prepare_index(client, index_name, corpus_fp)

                        run_res = search(
                            queries=queries,
                            index_name=index_name,
                            batch_size=100,
                            query_lambda=lambda query: {
                                "size": 15,
                                "query": {
                                    "neural_sparse": {
                                        EMBEDDING_FIELD: {
                                            "query_tokens": query
                                        },
                                    }
                                },
                                "_source": ["id"],
                            },
                        )

                        ndcg, map_, recall, p = EvaluateRetrieval.evaluate(
                            qrels, run_res, [10]
                        )
                        ndcg = ndcg["NDCG@10"]
                        try:
                            index_size = client.indices.stats(
                                index=index_name, params={"timeout": 600}
                            )["_all"]["primaries"]["store"]["size_in_bytes"]
                        except:
                            index_size = client.indices.stats(
                                index=index_name, params={"timeout": 600}
                            )["_all"]["primaries"]["store"]["size_in_bytes"]

                        result = {
                            "encoding_type": encoding_type,
                            "dataset": beir_dataset,
                            "pruning_type": pruning_type,
                            "pruning_number": pruning_number,
                            "ndcg": ndcg,
                            "index_size": index_size
                        }
                        file_name = f"""{encoding_type}_{beir_dataset}_{pruning_type}_{pruning_number}"""
                        with open(
                            os.path.join(configs.get("result_dir"), file_name), "w"
                        ) as f:
                            json.dump(result, f, indent=4)

                if flag:
                    logger.info(f"finish {beir_dataset}.")
                    client.indices.delete(index_name, request_timeout=600)

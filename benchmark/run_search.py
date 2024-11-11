import os
import json
import sys
import logging

import yaml

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


def prune_tokens(tokens, ratio):
    max_value = max(tokens.values())
    thresh = max_value * ratio
    return {k: v for k, v in tokens.items() if v >= thresh}, {
        k: v for k, v in tokens.items() if v < thresh
    }


if __name__ == "__main__":
    last_ingest = None
    flag = True

    assert len(sys.argv) == 2 and sys.argv[1].endswith(".yaml")
    with open(sys.argv[1]) as f:
        configs = yaml.safe_load(f)
    set_logging("run.log")
    os.makedirs(configs.get("search_result_dir"), exist_ok=True)

    client = OpenSearch(
        hosts=os.getenv("HOSTS").split(","),
        http_auth=None if os.getenv("AUTH") is None else os.getenv("AUTH").split(","),
        verify_certs=False,
        ssl_show_warn=False,
    )

    ingest_for_search = configs.get("pruning").get("ingest_for_search")
    for encoding_type in ingest_for_search:
        for beir_dataset in configs.get("beir_datasets_search"):
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

            for pruning_type in ingest_for_search[encoding_type]:
                pruning_number = ingest_for_search[encoding_type][pruning_type]
                prepare_ingest_processor(client, pruning_type, pruning_number)
                prepare_index(client, index_name, corpus_fp)

                for pruning_search in configs.get("pruning")["search"]:
                    pruning_type_search = pruning_search["pruning_type"]
                    for pruning_number_search in pruning_search["pruning_number"]:
                        # first search pass to warm up
                        run_res = search(
                            queries=queries,
                            index_name=index_name,
                            batch_size=100,
                            query_lambda=lambda query: {
                                "size": 15,
                                "query": {
                                    "neural_sparse": {
                                        EMBEDDING_FIELD: {
                                            "query_tokens": query,
                                            "prune_type": pruning_type_search,
                                            "prune_number": pruning_number_search,
                                        },
                                    }
                                },
                                "_source": ["id"],
                            },
                        )

                        total_took = 0
                        for q_id, q_vec in queries:
                            query = {
                                "size": 15,
                                "query": {
                                    "neural_sparse": {
                                        EMBEDDING_FIELD: {
                                            "query_tokens": q_vec,
                                            "prune_type": pruning_type_search,
                                            "prune_number": pruning_number_search,
                                        },
                                    }
                                },
                                "_source": ["id"],
                            }
                            res = client.search(index=index_name, body=query)
                            total_took += res["took"]

                        ndcg, map_, recall, p = EvaluateRetrieval.evaluate(
                            qrels, run_res, [10]
                        )
                        ndcg = ndcg["NDCG@10"]

                        result = {
                            "encoding_type": encoding_type,
                            "dataset": beir_dataset,
                            "pruning_type": pruning_type,
                            "pruning_number": pruning_number,
                            "pruning_type_search": pruning_type_search,
                            "pruning_number_search": pruning_number_search,
                            "ndcg": ndcg,
                            "total_took": total_took,
                        }
                        file_name = f"""{encoding_type}_{beir_dataset}_{pruning_type}_{pruning_number}_{pruning_type_search}_{pruning_number_search}"""
                        with open(
                            os.path.join(configs.get("search_result_dir"), file_name),
                            "w",
                        ) as f:
                            json.dump(result, f, indent=4)

                        # run 2-phase search
                        if pruning_type_search != "max_ratio":
                            continue
                        pruned_queries = [
                            (q_id, prune_tokens(tokens, pruning_number_search))
                            for q_id, tokens in queries
                        ]
                        run_res = search(
                            queries=pruned_queries,
                            index_name=index_name,
                            batch_size=100,
                            query_lambda=lambda query: {
                                "size": 15,
                                "query": {
                                    "neural_sparse": {
                                        EMBEDDING_FIELD: {
                                            "query_tokens": query[0],
                                            "prune_type": "empty",
                                            "prune_number": 0,
                                        },
                                    }
                                },
                                "rescore": {
                                    "window_size": 75,
                                    "query": {
                                        "rescore_query": {
                                            "neural_sparse": {
                                                EMBEDDING_FIELD: {
                                                    "query_tokens": query[1],
                                                    "prune_type": "empty",
                                                    "prune_number": 0,
                                                },
                                            }
                                        },
                                        "query_weight": 1.0,
                                        "rescore_query_weight": 1.0,
                                    },
                                },
                                "_source": ["id"],
                            },
                        )

                        total_took = 0
                        for q_id, query in pruned_queries:
                            query = {
                                "size": 15,
                                "query": {
                                    "neural_sparse": {
                                        EMBEDDING_FIELD: {
                                            "query_tokens": query[0],
                                            "prune_type": "empty",
                                            "prune_number": 0,
                                        },
                                    }
                                },
                                "rescore": {
                                    "window_size": 75,
                                    "query": {
                                        "rescore_query": {
                                            "neural_sparse": {
                                                EMBEDDING_FIELD: {
                                                    "query_tokens": query[1],
                                                    "prune_type": "empty",
                                                    "prune_number": 0,
                                                },
                                            }
                                        },
                                        "query_weight": 1.0,
                                        "rescore_query_weight": 1.0,
                                    },
                                },
                                "_source": ["id"],
                            }
                            res = client.search(index=index_name, body=query)
                            total_took += res["took"]

                        ndcg, map_, recall, p = EvaluateRetrieval.evaluate(
                            qrels, run_res, [10]
                        )
                        ndcg = ndcg["NDCG@10"]

                        result = {
                            "encoding_type": encoding_type,
                            "dataset": beir_dataset,
                            "pruning_type": pruning_type,
                            "pruning_number": pruning_number,
                            "pruning_type_search": "2-phase",
                            "pruning_number_search": pruning_number_search,
                            "ndcg": ndcg,
                            "total_took": total_took,
                        }
                        file_name = f"""{encoding_type}_{beir_dataset}_{pruning_type}_{pruning_number}_2-phase_{pruning_number_search}"""
                        with open(
                            os.path.join(configs.get("search_result_dir"), file_name),
                            "w",
                        ) as f:
                            json.dump(result, f, indent=4)

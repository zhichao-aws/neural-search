import os
import sys
import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import yaml
from opensearchpy import OpenSearch

assert len(sys.argv) == 2 and sys.argv[1].endswith(".yaml")
with open(sys.argv[1]) as f:
    configs = yaml.safe_load(f)

client = OpenSearch(
    hosts=os.getenv("HOSTS").split(","),
    http_auth=None if os.getenv("AUTH") is None else os.getenv("AUTH").split(","),
    verify_certs=False,
    ssl_show_warn=False,
)

target_index_size = 0.45
# encoding_types = [x[0] for x in client.transport.perform_request("POST","/_plugins/_ppl",body={
#     "query" : "source = results | fields encoding_type | dedup encoding_type"
# })["datarows"]]
# pruning_types = [x[0] for x in client.transport.perform_request("POST","/_plugins/_ppl",body={
#     "query" : "source = results | fields pruning_type | dedup pruning_type"
# })["datarows"]]
target_point = {}


for encoding_type in ["doc-v2-distill", "v2-distill"]:
    data = client.transport.perform_request(
        "POST",
        "/_plugins/_ppl",
        body={
            "query": f"source = results | where encoding_type = '{encoding_type}' AND pruning_type_search = 'max_ratio' AND pruning_number_search=0 | stats avg(ndcg) as ndcg, avg(index_size) as index_size by pruning_type,pruning_number"
        },
    )
    for data_row in data["datarows"]:
        data_row[-1] = (
            str(data_row[-1]) if data_row[-2] != "top_k" else str(int(data_row[-1]))
        )
    data = pd.DataFrame(
        data["datarows"],
        columns=["ndcg", "index_size", "pruning_type", "pruning_number"],
    )
    pruning_data = {
        pruning_type: data[data["pruning_type"] == pruning_type][
            ["index_size", "ndcg", "pruning_number"]
        ]
        for pruning_type in data["pruning_type"].unique()
    }
    target_point[encoding_type] = {}

    for pruning_type, df in pruning_data.items():
        plt.plot(df["index_size"], df["ndcg"], label=pruning_type, marker="o")
        closest_idx = (np.abs(df["index_size"] - target_index_size)).idxmin()
        closest_point = df.loc[closest_idx]
        target_point[encoding_type][pruning_type] = closest_point["pruning_number"]

    plt.xlabel("Index Size", fontsize=14)
    plt.ylabel("NDCG", fontsize=14)
    plt.legend()
    plt.savefig(f"{encoding_type}_ndcg_index-size.png")
    plt.close()
    # del plt

print(target_point)

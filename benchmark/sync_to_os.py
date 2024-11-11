import os
import sys
import json

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

try:
    client.indices.delete("results")
except:
    pass

client.indices.create(
    index="results",
    body={
        "mappings": {
            "properties": {
                "dataset": {"type": "keyword"},
                "encoding_type": {"type": "keyword"},
                "index_size": {"type": "float"},
                "index_size_bytes": {"type": "long"},
                "ndcg": {"type": "float"},
                "pruning_number": {"type": "float"},
                "pruning_type": {"type": "keyword"}
            }
        }
    },
)

result_dir = configs["result_dir"]
contents = []
for file in os.listdir(result_dir):
    with open(os.path.join(result_dir, file)) as f:
        content = json.load(f)
    contents.append([file, content])

max_index_sizes = {}
encoding_types = set()
datasets = set()

for file, content in contents:
    encoding_type = content["encoding_type"]
    dataset = content["dataset"]
    encoding_types.add(encoding_type)
    datasets.add(dataset)

for encoding_type in encoding_types:
    max_index_sizes[encoding_type] = {}
    for dataset in datasets:
        file_name = f"{encoding_type}_{dataset}_max_ratio_0_max_ratio_0"
        with open(os.path.join(result_dir, file_name)) as f:
            content = json.load(f)
        max_index_sizes[encoding_type][dataset] = content["index_size"]

print(max_index_sizes)

result_dir = configs["result_dir"]
bulk_body = []
for file in os.listdir(result_dir):
    with open(os.path.join(result_dir, file)) as f:
        content = json.load(f)
    bulk_body.append({"index": {"_index": "results", "_id": file}})
    content["index_size_bytes"] = content["index_size"]
    content["index_size"] = (
        content["index_size"]
        / max_index_sizes[content["encoding_type"]][content["dataset"]]
    )
    bulk_body.append(content)

assert client.bulk(bulk_body)["errors"] == False
client.indices.refresh("results")

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
    client.indices.delete("search_results")
except:
    pass

client.indices.create(
    index="search_results",
    body={
        "mappings": {
            "properties": {
                "dataset": {"type": "keyword"},
                "encoding_type": {"type": "keyword"},
                "ndcg": {"type": "float"},
                "pruning_number": {"type": "float"},
                "pruning_number_search": {"type": "float"},
                "pruning_type": {"type": "keyword"},
                "pruning_type_search": {"type": "keyword"},
                "total_took": {"type": "float"},
            }
        }
    },
)

result_dir = configs["search_result_dir"]
contents = []
for file in os.listdir(result_dir):
    with open(os.path.join(result_dir, file)) as f:
        content = json.load(f)
    contents.append([file, content])

max_search_time = {}
encoding_types = set()
datasets = set()

for file, content in contents:
    encoding_type = content["encoding_type"]
    dataset = content["dataset"]

    if encoding_type not in max_search_time:
        max_search_time[encoding_type] = {}
    if dataset not in max_search_time[encoding_type]:
        max_search_time[encoding_type][dataset] = []

    max_search_time[encoding_type][dataset].append(content["total_took"])

for encoding_type in list(max_search_time.keys()):
    for dataset in list(max_search_time[encoding_type].keys()):
        max_search_time[encoding_type][dataset] = max(
            max_search_time[encoding_type][dataset]
        )

print(max_search_time)


bulk_body = []
for file, content in contents:
    bulk_body.append({"index": {"_index": "search_results", "_id": file}})
    content["total_took"] = (
        content["total_took"]
        / max_search_time[content["encoding_type"]][content["dataset"]]
    )
    bulk_body.append(content)

assert client.bulk(bulk_body)["errors"] == False
client.indices.refresh("search_results")

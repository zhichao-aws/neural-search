import argparse
import yaml
from tqdm import tqdm
from opensearchpy import OpenSearch
from beir.retrieval.evaluation import EvaluateRetrieval
from beir.datasets.data_loader import GenericDataLoader

parser = argparse.ArgumentParser(description="Description of your script")
parser.add_argument("config_file", help="input file name",type=str)
args = parser.parse_args()

with open(args.config_file, "r") as file:
    config = yaml.safe_load(file)
    host = config["host"]
    port = config["port"]
    auth = (config["username"],config["password"])
    client = OpenSearch(
        hosts = [{'host': host,"port":port}],
        # http_compress = True, # enables gzip compression for request bodies
        http_auth = auth,
        # client_cert = client_cert_path,
        # client_key = client_key_path,
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
        # ca_certs = ca_certs_path
    )

corpus, queries, qrels = GenericDataLoader(data_folder="nfcorpus/").load(split="test")
client.transport.perform_request("PUT","/_ingest/pipeline/%s"%config["ingest_pipeline"],body={
  "description": "An sparse encoding ingest pipeline",
  "processors": [
    {
      "sparse_encoding": {
        "model_id": config["ingest_model_id"],
        "field_map": {
          "passage_text": "passage_embedding"
        }
      }
    }
  ]
})

try:
    client.indices.delete(index=config["index_name"],request_timeout=60)
except:
    pass

client.indices.create(index=config["index_name"],body={
      "settings": {
        "default_pipeline": config["ingest_pipeline"],
        "index": {
          "number_of_shards": 3
        }
      },
      "mappings": {
        "properties": {
            "passage_embedding": {
                "type": "rank_features"
            },
            "passage_text": {
                "type": "text"
            }
        }
      }
    })

for key,value in tqdm(corpus.items()):
    client.index(index=config["index_name"],id=key,body={"passage_text":value["title"]+" "+value["text"]})
    
client.indices.refresh(index=config["index_name"],request_timeout=60)
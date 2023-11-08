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
run_res=dict()
for _id,text in tqdm(queries.items()):
    response=client.search(index=config["index_name"],body={
        "query":{
            "neural_sparse":{
                "passage_embedding":{
                    "model_id":config["query_model_id"],
                    "query_text":text
                }
            }
        }
    })
    hits=response["hits"]["hits"]
    run_res[_id]={item["_id"]:item["_score"] for item in hits}
    
for query_id, doc_dict in tqdm(run_res.items()):
    if query_id in doc_dict:
        doc_dict.pop(query_id)

res=EvaluateRetrieval.evaluate(qrels, run_res, [10])
print(res)
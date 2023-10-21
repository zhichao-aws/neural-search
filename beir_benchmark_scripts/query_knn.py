import os
import json
import argparse
import yaml
import time

from tqdm import tqdm
from opensearchpy import OpenSearch
from beir.retrieval.evaluation import EvaluateRetrieval
from utils import deploy_model

def analysis_times(times):
    times=sorted(times)
    length=len(times)
    print("avg times:%.4f"%(sum(times)/length))
    print("times at 90:%.4f"%times[int(length*0.9)])
    print("times at 99:%.4f"%times[int(length*0.99)])
    return sum(times)/length,times[int(length*0.9)],times[int(length*0.99)]

def build_res(res,times,dataset):
    body={
        "dataset":dataset,
        "algorithm":"sparse",
        "times_avg":times[0],
        "times_P90":times[1],
        "times_P99":times[2]
    }
    for d in res:
        for k,v in d.items():
            body[k]=v
    return body

parser = argparse.ArgumentParser(description="Description of your script")
parser.add_argument("config_file", help="input file name",type=str)
args = parser.parse_args()

with open(args.config_file, "r") as file:
    config = yaml.safe_load(file)
    print(config)
    client=OpenSearch(hosts = [{'host': config["endpoint"], 'port': config["port"]}],http_compress = True)

all_files = os.listdir(config["bulk_string_dir"])
all_datasets = [file.split(".")[0] for file in all_files]

base_query_body = {
        "_source":False,
        "query":{
            "neural":{
                "text_sparse":{
                    "query_text":"",
                    "model_id": config["model_id"],
                    "k": 20
                }
            }
        }
    }

deploy_model(client, config["model_id"])
for dataset in all_datasets:
    index_name = config["index_prefix"] + dataset
    try:
        client.get(index=config["result_index_name"],id=dataset)
        print("search result exists for dataset "+dataset)
        continue
    except:
        print("search result not exists for dataset "+dataset)
        pass
    try:
        client.indices.open(index=index_name)
    except:
        print("wait another 60s")
        time.sleep(60)
    print("start search")
    times = []
    run_res = {}
    
        
    with open(config["qrels_dir"]+"/%s.json"%dataset) as f:
        qrels=json.load(f)
    with open(config["queries_dir"]+"/%s.json"%dataset) as f:
        queries=json.load(f)
        
    print("start query of index ",index_name)
    for _id,text in tqdm(queries.items()):
        base_query_body["query"]["neural"]["text_sparse"]["query_text"] = text
        print(base_query_body)
        response=client.search(index=index_name,body=base_query_body, request_timeout=600)
        hits=response["hits"]["hits"]
        run_res[_id]={item["_id"]:item["_score"] for item in hits}
        times.append(response["took"])
        
    for query_id, doc_dict in tqdm(run_res.items()):
        if query_id in doc_dict:
            doc_dict.pop(query_id)

    t1,t2,t3=analysis_times(times)
    res=EvaluateRetrieval.evaluate(qrels, run_res, [10])
    body=build_res(res,[t1,t2,t3],dataset)
    print(dataset)
    print(body)
    client.index(index=config["result_index_name"],body=body,id=dataset,refresh=True)
    try:
        client.indices.close(index=index_name)
    except:
        print("wait another 60s")
        time.sleep(60)
    
response = client.search(index=config["result_index_name"], body={"size":50,"query":{"match_all":{}}})
with open(config["result_index_name"]+".json","w") as f:
    json.dump(response,f)
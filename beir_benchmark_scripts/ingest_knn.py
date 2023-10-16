import os
import json
import argparse
import yaml

from tqdm import tqdm
from opensearchpy import OpenSearch

def create_index(index_name):
    try:
        client.indices.delete(index_name)
    except:
        pass

    index_body = {
      'settings': {
        'index': {
          'number_of_shards': 2,
          'knn': True
        }
      },
      "mappings": {
        "properties": {
            "text_sparse": {
                "type": "knn_vector",
                "dimension": 768,
                "method": {
                    "name": "hnsw",
                    "engine": "nmslib",
                    "space_type": "innerproduct",
                    "parameters": {}
                }
            },
            "text": {
                "type": "text"
            },
            "title": {
                "type": "text", "analyzer" : "english"
            },
            "body": {
                "type": "text", "analyzer" : "english"
            }
        }
      }
    }

    response = client.indices.create(index_name, body=index_body)
    return response

def get_line_numbers(filename):
    with open(filename, "r") as file:
        line_count = 0
        for line in tqdm(file):
            line_count += 1
    return line_count


parser = argparse.ArgumentParser(description="Description of your script")
parser.add_argument("config_file", help="input file name",type=str)
args = parser.parse_args()

with open(args.config_file, "r") as file:
    config = yaml.safe_load(file)
    print(config)
    client=OpenSearch(hosts = [{'host': config["endpoint"], 'port': config["port"]}],http_compress = True)

all_files = os.listdir(config["bulk_string_dir"])
all_datasets = [file.split(".")[0] for file in all_files]

for dataset in all_datasets:
# for dataset in ["quora"]:
    file_name = os.path.join(config["bulk_string_dir"], dataset + ".jsonl")
    with open(file_name) as f:
        for line in f:
            break
        index_name=json.loads(line)["index"]["_index"]
    try:
        response = client.cat.indices(index=index_name, format="json")
        print(index_name, " index already exists.")
        bulk_file_doc_num = get_line_numbers(file_name)//2
        index_doc_num = int(response[0]["docs.count"])
        print("current doc number: %d, total doc number: %d"%(index_doc_num, bulk_file_doc_num))
        if index_doc_num == bulk_file_doc_num:
            continue
    except:
        print(index_name, " index not exists")
        
    print("start ingestion of index ",index_name)
    create_index(index_name)
    
    with open(file_name) as f:
        lines = []
        num = 0
        for i,line in enumerate(tqdm(f)):
            lines.append(line)
            num+=1
            if num%(config["bulk_size"]*2)==0:
                num=0
                response=client.bulk("".join(lines))
                lines = []
                assert response["errors"]==False
        if lines != []:
            response=client.bulk("".join(lines))
            assert response["errors"]==False
        client.indices.refresh(index=index_name, request_timeout=60)
        client.indices.flush(index=index_name, request_timeout=60)
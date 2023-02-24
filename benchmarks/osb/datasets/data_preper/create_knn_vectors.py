import sys
import json
import requests
import numpy as np

OS_URL = "http://perftestclusterloadbalancer-1850532049.us-east-1.elb.amazonaws.com:80"
MODEL_ID = "04sDLYQBWeF_lpmA1qwb"

body = {
        "text_docs": ["how does the coronavirus respond to changes in the weather"],
        "target_response": ["sentence_embedding"]
}

query = {
    "query": {
        "knn": {
            "title_and_text_knn": {
                "vector": [],
                "k": 100
            }
        }
    }
}


def main(query_file, output_query_file):
    data_set_file = open(query_file, 'r')
    lines = data_set_file.readlines()
    final_queries = []
    count = 0
    time = []
    for line in lines:
        sentence = json.loads(line.strip())["query"]["match"]["text"]
        path = "/_plugins/_ml/_predict/text_embedding/" + MODEL_ID
        body["text_docs"] = [sentence]
        o = make_request(path, body, 'POST')
        output = json.loads(o["text"])
        # print(output["inference_results"])
        vector = output["inference_results"][0]["output"][0]["data"]
        query["query"]["knn"]["title_and_text_knn"]["vector"] = vector
        count += 1
        final_queries.append(json.dumps(query))
        print("Completed: {} time : {}".format(count, o["time"]))
        time.append(o["time"])
    data_set_file.close()

    output_query = open(output_query_file, 'w+')
    lines_count = 0
    for line in final_queries:
        output_query.write(str(line))
        if lines_count == count - 1:
            break
        output_query.write("\n")
        lines_count += 1
    output_query.close()
    print_ml_response_metrics(time)


def print_ml_response_metrics(time):
    a = np.array(time)
    size = len(time)
    print("\nML response time")
    print("Average Time : {} ms".format(sum(time) / size))
    print("p50 Time : {} ms".format(np.percentile(a, 50)))
    print("p90 : {} ms".format(np.percentile(a, 90)))
    print("p99 : {} ms".format(np.percentile(a, 99)))
    print("p100 : {} ms".format(np.percentile(a, 100)))
    print("Max value : {}".format(max(time)))


def make_request(path, request_body, http_method='GET'):
    if http_method == 'GET':
        return process_response(requests.get(OS_URL+path))
    elif http_method == 'POST':
        return process_response(requests.post(OS_URL + path, None, request_body))
        # print(" {} {} ".format(response.status_code, response.text))
    else:
        raise Exception("HTTP method is not correct. Passed value : {}".format(http_method))
    pass


def process_response(response):
    if response.status_code == 200:
        return {
            "text" : response.text,
            "time" : round(response.elapsed.microseconds / 1000)
        }
    raise Exception("Got error response: {}. text is : {}".format(response.status_code, response.text))


if __name__ == "__main__":
    if len(sys.argv) >= 4:
        OS_URL = "http://" + sys.argv[3]

    if len(sys.argv) >= 5:
        MODEL_ID = sys.argv[4]

    main(sys.argv[1], sys.argv[2])



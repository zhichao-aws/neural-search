import sys
import json

body = {
        "text_docs": [],
        "target_response": ["sentence_embedding"]
}

def main(query_file, output_query_file):
    data_set_file = open(query_file, 'r')
    lines = data_set_file.readlines()
    final_queries = []
    count = 0
    for line in lines:
        sentence = json.loads(line.strip())["query"]["multi_match"]["query"]
        body["text_docs"] = [sentence]
        count += 1
        final_queries.append(json.dumps(body))
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


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])



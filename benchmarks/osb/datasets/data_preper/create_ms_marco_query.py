import json
import sys

query = {
    "query": {
        "neural": {
            "body_knn": {
                "query_text": "",
                "model_id": ""
            }
        }
    }
}


def main(read_file_name, write_file_name, model_id):
    data_set_file = open(read_file_name, 'r')
    lines = data_set_file.readlines()
    current = 0
    final_lines = []
    for line in lines:
        current += 1
        query["query"]["neural"]["body_knn"]["query_text"] = line.strip().split('\t')[1]
        query["query"]["neural"]["body_knn"]["model_id"] = model_id
        final_lines.append(json.dumps(query))
    data_set_file.close()

    transformed_file = open(write_file_name, 'w+')
    lines_count = 0
    for line in final_lines:
        transformed_file.write(line)
        transformed_file.write("\n")
        lines_count += 1

    print("Line added {}".format(lines_count))
    transformed_file.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
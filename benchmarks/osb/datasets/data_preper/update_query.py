import sys
import json


q = {
    "query": {
            "multi_match" : {

            }
    }
}


def main(read_file_name, write_file_name):
    data_set_file = open(read_file_name, 'r')
    lines = data_set_file.readlines()

    final_lines = []
    for line in lines:
        query = json.loads(line)
        q["query"]["multi_match"] = query["query"]["bool"]["should"][0]["multi_match"]
        final_lines.append(json.dumps(q))

    data_set_file.close()

    transformed_file = open(write_file_name, 'w+')
    lines_count = 0
    for line in final_lines:
        transformed_file.write(line)
        if lines_count == len(lines) - 1:
            break
        transformed_file.write("\n")
        lines_count += 1
    transformed_file.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
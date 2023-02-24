import json
import sys

CORPUS = "corpus"
QUERY = "query"

sample_query = {
  "query": {
    "match": {
      "text": ""
    }
  }
}

def main(read_file, write_file, transform_type=''):
    data_set_file = open(read_file, 'r')
    lines = data_set_file.readlines()

    count = 0
    transformed_lines = []
    for line in lines:
        count += 1
        transformed_line = json.loads(line.strip())
        del transformed_line["metadata"]
        if transform_type == CORPUS:
            transformed_line["title_and_text"] = transformed_line["title"] + " " + transformed_line["text"]
            transformed_line["id"] = transformed_line["_id"]
            del transformed_line["_id"]
        elif transform_type == QUERY:
            sample_query["query"]["match"]["text"] = transformed_line["text"]
            del transformed_line["_id"]
            del transformed_line["text"]
            transformed_line = sample_query

        transformed_lines.append(json.dumps(transformed_line))

    data_set_file.close()
    print("Total lines count : {}".format(count))
    transformed_file = open(write_file, 'w+')
    lines_count = 0
    for line in transformed_lines:
        transformed_file.write(line)
        if lines_count == count - 1:
            break
        transformed_file.write("\n")
        lines_count += 1
    transformed_file.close()


if __name__ == "__main__":
    n = len(sys.argv)
    if sys.argv[1] == CORPUS:
        main(sys.argv[2],sys.argv[3], CORPUS)
    elif sys.argv[1] == QUERY:
        main(sys.argv[2],sys.argv[3], QUERY)
    else:
        print("No Args passed")

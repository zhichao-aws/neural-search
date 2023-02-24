import json
import sys


def main(read_file, write_file):
    data_set_file = open(read_file, 'r')
    lines = data_set_file.readlines()

    final_list = []
    count = 0
    for line in lines:
        query = json.loads(line)
        final_query = query["query"]["bool"]["should"][1]["script_score"]
        del final_query["script"]
        final_list.append(json.dumps(final_query))
        count += 1

    data_set_file.close()

    transformed_file = open(write_file, 'w+')
    lines_count = 0
    for line in final_list:
        transformed_file.write(line)
        if lines_count == count - 1:
            break
        transformed_file.write("\n")
        lines_count += 1
    transformed_file.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
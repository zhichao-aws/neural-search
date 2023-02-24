import json
import sys


def main(file_name):
    data_set_file = open(file_name, 'r')
    lines = data_set_file.readlines()
    size = []
    sum = 0.0
    for line in lines:
        text = json.loads(line)["query"]["match"]["text"]
        size.append(len(text.split()))
        sum = sum + len(text.split())

    size.sort()

    print("median : {}".format(size[round(len(lines)/2)]))
    print("avg : {}".format(sum/len(lines)))


if __name__ == "__main__":
    main(sys.argv[2])


import sys
import random


# Generates the count of queries
def main(read_file_name, write_file_name, count):
    data_set_file = open(read_file_name, 'r')
    lines = data_set_file.readlines()
    current = 0
    count = int(count)
    final_lines = []
    while current < count:
        current += 1
        final_lines.append(lines[random.randint(0, len(lines)-1)].strip())
    data_set_file.close()

    transformed_file = open(write_file_name, 'w+')
    lines_count = 0
    for line in final_lines:
        transformed_file.write(line)
        if lines_count == count - 1:
            break
        transformed_file.write("\n")
        lines_count += 1
    transformed_file.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])

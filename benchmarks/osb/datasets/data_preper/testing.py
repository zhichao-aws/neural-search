import linecache
import subprocess
number = 10

line = linecache.getline("../transformed/trec-covid/transformed_queries.json", number)

print(line)


def file_len(fname):
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return int(result.strip().split()[0])


#print(file_len("../transformed/trec-covid/transformed_queries.json"))
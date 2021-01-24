import lithops
from airflow.models import Variable

# Dataset from: https://archive.ics.uci.edu/ml/datasets/bag+of+words
iterdata = ['https://archive.ics.uci.edu/ml/machine-learning-databases/bag-of-words/vocab.enron.txt',
            'https://archive.ics.uci.edu/ml/machine-learning-databases/bag-of-words/vocab.kos.txt',
            'https://archive.ics.uci.edu/ml/machine-learning-databases/bag-of-words/vocab.nips.txt',
            'https://archive.ics.uci.edu/ml/machine-learning-databases/bag-of-words/vocab.nytimes.txt',
            'https://archive.ics.uci.edu/ml/machine-learning-databases/bag-of-words/vocab.pubmed.txt']

def my_map_function(url):
    print('I am processing the object from {}'.format(url.path))
    counter = {}

    data = url.data_stream.read()

    for line in data.splitlines():
        for word in line.decode('utf-8').split():
            if word not in counter:
                counter[word] = 1
            else:
                counter[word] += 1

    return counter


def my_reduce_function(results):
    final_result = {}
    for count in results:
        for word in count:
            if word not in final_result:
                final_result[word] = count[word]
            else:
                final_result[word] += count[word]

    return final_result

def lithops_func(*op_args):
    config = Variable.get("lithops_config", deserialize_json=True)
    fexec = lithops.FunctionExecutor(config=config)
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function)
    result = fexec.get_result()
    print("Done! Result >> ",result)
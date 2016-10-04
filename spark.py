from pyspark import SparkContext, SparkConf

def get_author(entry):
    for line in entry[1].splitlines():
        if line.startswith("Author: "):
            author = line[8:].strip()
            break
    return (author, entry[1])

def word_count(text):
    wordcount={}
    for word in text.split():
        if len(word) < 4:
            continue
        if word not in wordcount:
            wordcount[word] = 1
        else:
            wordcount[word] += 1
    return wordcount

def merge(dict1, dict2):
    # Merge dict2 into dict1
    for key in dict2.keys():
        if key in dict1:
            dict1[key] += dict2[key]
        else:
            dict1[key] = dict2[key]
    return dict1

def sort_dict(dict):
    return sorted(dict.items(), key=lambda x: x[1], reverse=True)

sc = SparkContext()
rdd = sc.wholeTextFiles("dataset")
rdd = rdd.map(get_author).mapValues(word_count).reduceByKey(merge)
rdd = rdd.mapValues(sort_dict).mapValues(lambda x: x[0:5])
print(rdd.saveAsTextFile("output"))

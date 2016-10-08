#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import io
import sys
import csv
import functools
from operator import concat
from pyspark import SparkContext, SparkConf

# Lista de palavras a ser desconsideradas

sw = ['a', 'ao', 'aos', 'aquela', 'aquelas', 'aquele', 'aqueles', 'aquilo', 'as', 'até', 'com', 'como', 'da', 'das', 'de', 'dela', 'delas', 'dele', 'deles', 'depois', 'do', 'dos', 'e', 'ela', 'elas', 'ele', 'eles', 'em', 'entre', 'era', 'eram', 'essa', 'essas', 'esse', 'esses', 'esta', 'estamos', 'estas', 'estava', 'estavam', 'este', 'esteja', 'estejam', 'estejamos', 'estes', 'esteve', 'estive', 'estivemos', 'estiver', 'estivera', 'estiveram', 'estiverem', 'estivermos', 'estivesse', 'estivessem', 'estivéramos', 'estivéssemos', 'estou', 'está', 'estávamos', 'estão', 'eu', 'foi', 'fomos', 'for', 'fora', 'foram', 'forem', 'formos', 'fosse', 'fossem', 'fui', 'fôramos', 'fôssemos', 'haja', 'hajam', 'hajamos', 'havemos', 'hei', 'houve', 'houvemos', 'houver', 'houvera', 'houveram', 'houverei', 'houverem', 'houveremos', 'houveria', 'houveriam', 'houvermos', 'houverá', 'houverão', 'houveríamos', 'houvesse', 'houvessem', 'houvéramos', 'houvéssemos', 'há', 'hão', 'isso', 'isto', 'já', 'lhe', 'lhes', 'mais', 'mas', 'me', 'mesmo', 'meu', 'meus', 'minha', 'minhas', 'muito', 'na', 'nas', 'nem', 'no', 'nos', 'nossa', 'nossas', 'nosso', 'nossos', 'num', 'numa', 'não', 'nós', 'o', 'os', 'ou', 'para', 'pela', 'pelas', 'pelo', 'pelos', 'por', 'qual', 'quando', 'que', 'quem', 'se', 'seja', 'sejam', 'sejamos', 'sem', 'serei', 'seremos', 'seria', 'seriam', 'será', 'serão', 'seríamos', 'seu', 'seus', 'somos', 'sou', 'sua', 'suas', 'são', 'só', 'também', 'te', 'tem', 'temos', 'tenha', 'tenham', 'tenhamos', 'tenho', 'terei', 'teremos', 'teria', 'teriam', 'terá', 'terão', 'teríamos', 'teu', 'teus', 'teve', 'tinha', 'tinham', 'tive', 'tivemos', 'tiver', 'tivera', 'tiveram', 'tiverem', 'tivermos', 'tivesse', 'tivessem', 'tivéramos', 'tivéssemos', 'tu', 'tua', 'tuas', 'tém', 'tínhamos', 'um', 'uma', 'você', 'vocês', 'vos', 'à', 'às', 'éramos', 'des', 'dum', 'duma', 'ella', 'et', 'he', 'hum', 'huma', 'les', 'pera', 'porque', 'pêla', 'pêlo', 'sobre', 'todos', 'vossa', 'vosso', 'é']

DEFAULT_ENCODING = "iso-8859-1"
BOOK_CONTENTS_START_DELIMITER = "*** START OF THIS PROJECT GUTENBERG EBOOK"
BOOK_CONTENTS_END_DELIMITER   = "*** END OF THIS PROJECT GUTENBERG EBOOK"

def get_author(entry):
    bookContents = entry[1].decode(DEFAULT_ENCODING, "replace")
    author = encoding = None
    lines = bookContents.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if author and encoding:
            break;
        if not author and line.startswith("Author: "):
            author = line[8:].strip()
        if not encoding and line.startswith("Character set encoding: "):
            encoding = line[24:].strip().lower()
            if not encoding == DEFAULT_ENCODING:
                bookContents = entry[1].decode(encoding, "replace")
                lines = bookContents.splitlines()
                author = None
                i = -1
        i += 1
    print ("author is %s" % author)
    print ("encoding is %s" % encoding)
    return (author, bookContents)

def word_count(text):
    wordcount={}
    # Cria lista de stop words
    for line in text.splitlines():
        if line.startswith(BOOK_CONTENTS_START_DELIMITER):
            wordcount={}
        elif line.startswith(BOOK_CONTENTS_END_DELIMITER):
            break
        else:
            for word in re.split('\W+', line.lower()):
                # Exclui as palavras mais comuns (stop words)
                if word in sw or len(word) < 2:
                    continue
                if word not in wordcount:
                    wordcount[word] = 1
                else:
                    wordcount[word] += 1
    return wordcount

def my_merge(dict1, dict2):
    # Merge dict2 into dict1
    for key in dict2.keys():
        if key in dict1:
            dict1[key] += dict2[key]
        else:
            dict1[key] = dict2[key]
    return dict1

def sort_dict(dict):
    return sorted(dict.items(), key=lambda x: x[1], reverse=True)

def to_flat_list(value):
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, dict):
        # Turns the dictionary into a list containing all the keys and values
        value = list(functools.reduce(lambda itemsList, item: list(itemsList)+list(item), value.items()))
    if hasattr(value, '__iter__') and not isinstance(value, str):
        # Flatens list, usig recursion
        return list(functools.reduce(concat, map(to_flat_list, value)))
    else:
        return [value]

def format_to_csv_str(entry):
    key   = entry[0]
    value = to_flat_list(entry[1])
    """Returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output, quoting=csv.QUOTE_NONE, escapechar='\\').writerow([key] + value)
    return output.getvalue().strip() # remove extra newline

sc  = SparkContext()

input_path = sys.argv[1] if len(sys.argv) > 1 else "dataset"
output_path = sys.argv[2] if len(sys.argv) > 2 else "output"

rdd = sc.binaryFiles(input_path)
rdd = rdd.map(get_author).mapValues(word_count).reduceByKey(my_merge)
rdd = rdd.mapValues(sort_dict).mapValues(lambda x: x[0:5])
print(rdd.map(format_to_csv_str).saveAsTextFile(output_path))

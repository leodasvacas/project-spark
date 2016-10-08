#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import io
import csv
import argparse
from functools import partial
from pyspark import SparkContext, SparkConf

# Lista de palavras a ser desconsideradas

sw = ['a', 'ao', 'aos', 'aquela', 'aquelas', 'aquele', 'aqueles', 'aquilo', 'as', 'até', 'com', 'como', 'da', 'das', 'de', 'dela', 'delas', 'dele', 'deles', 'depois', 'do', 'dos', 'e', 'ela', 'elas', 'ele', 'eles', 'em', 'entre', 'era', 'eram', 'essa', 'essas', 'esse', 'esses', 'esta', 'estamos', 'estas', 'estava', 'estavam', 'este', 'esteja', 'estejam', 'estejamos', 'estes', 'esteve', 'estive', 'estivemos', 'estiver', 'estivera', 'estiveram', 'estiverem', 'estivermos', 'estivesse', 'estivessem', 'estivéramos', 'estivéssemos', 'estou', 'está', 'estávamos', 'estão', 'eu', 'foi', 'fomos', 'for', 'fora', 'foram', 'forem', 'formos', 'fosse', 'fossem', 'fui', 'fôramos', 'fôssemos', 'haja', 'hajam', 'hajamos', 'havemos', 'hei', 'houve', 'houvemos', 'houver', 'houvera', 'houveram', 'houverei', 'houverem', 'houveremos', 'houveria', 'houveriam', 'houvermos', 'houverá', 'houverão', 'houveríamos', 'houvesse', 'houvessem', 'houvéramos', 'houvéssemos', 'há', 'hão', 'isso', 'isto', 'já', 'lhe', 'lhes', 'mais', 'mas', 'me', 'mesmo', 'meu', 'meus', 'minha', 'minhas', 'muito', 'na', 'nas', 'nem', 'no', 'nos', 'nossa', 'nossas', 'nosso', 'nossos', 'num', 'numa', 'não', 'nós', 'o', 'os', 'ou', 'para', 'pela', 'pelas', 'pelo', 'pelos', 'por', 'qual', 'quando', 'que', 'quem', 'se', 'seja', 'sejam', 'sejamos', 'sem', 'serei', 'seremos', 'seria', 'seriam', 'será', 'serão', 'seríamos', 'seu', 'seus', 'somos', 'sou', 'sua', 'suas', 'são', 'só', 'também', 'te', 'tem', 'temos', 'tenha', 'tenham', 'tenhamos', 'tenho', 'terei', 'teremos', 'teria', 'teriam', 'terá', 'terão', 'teríamos', 'teu', 'teus', 'teve', 'tinha', 'tinham', 'tive', 'tivemos', 'tiver', 'tivera', 'tiveram', 'tiverem', 'tivermos', 'tivesse', 'tivessem', 'tivéramos', 'tivéssemos', 'tu', 'tua', 'tuas', 'tém', 'tínhamos', 'um', 'uma', 'você', 'vocês', 'vos', 'à', 'às', 'éramos', 'des', 'dum', 'duma', 'ella', 'et', 'he', 'hum', 'huma', 'les', 'pera', 'porque', 'pêla', 'pêlo', 'sobre', 'todos', 'vossa', 'vosso', 'é']

BOOK_CONTENTS_START_DELIMITER = "*** START OF THIS PROJECT GUTENBERG EBOOK"
BOOK_CONTENTS_END_DELIMITER   = "*** END OF THIS PROJECT GUTENBERG EBOOK"

def check_negative(value):
    try:
        ivalue = int(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError("%s is an invalid int value" % value)
    if ivalue < 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue

def verbose_print(*args):
    # Print each argument separately so caller doesn't need to
    # stuff everything to be printed into a single string
    for arg in args:
       print (arg),
    print

def get_author(default_encoding, entry):
    bookContents = entry[1].decode(default_encoding, "replace")
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
            if not encoding == default_encoding:
                bookContents = entry[1].decode(encoding, "replace")
                lines = bookContents.splitlines()
                author = None
                i = -1
        i += 1
    verbose_print ("author is %s" % author)
    verbose_print ("encoding is %s" % encoding)
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

def sort_dict(sort_by_word, dictionary):
    keyFun = (lambda x:x[0]) if sort_by_word else (lambda x:x[1])
    return sorted(dictionary.items(), key=keyFun, reverse=not sort_by_word)

def format_to_csv_str(entry):
    key = entry[0]
    results = entry[1]
    """Returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output, quoting=csv.QUOTE_NONE, escapechar='\\', lineterminator='\n').writerow([key] + list(results[0]))
    results = [ [""] + list(result) for result in results[1:]]
    csv.writer(output, quoting=csv.QUOTE_NONE, escapechar='\\', lineterminator='\n').writerows(results)
    return output.getvalue().strip() # remove extra newline

def main():

    # Command-Line Argument Parsing
    parser = argparse.ArgumentParser(
        description='Python implementation of author word count.'
    )

    parser.add_argument(
        '-in', '--input-path',
        action='store',
        required=False,
        default="dataset"
    )
    parser.add_argument(
        '-out', '--output-path',
        action='store',
        required=False,
        default="output"
    )
    parser.add_argument(
       '--sort-by-word',
        action='store_true',
        help='whether to sort the words alphabeticaly instead of frequency'
    )
    parser.add_argument(
        '--default-encoding',
        action='store',
        required=False,
        default='iso-8859-1',
        help='the default encoding to consider when reading the input text files. Defaults to iso-8859-1'
    )
    parser.add_argument(
        '--limit',
        required=False,
        type=check_negative,
        default=None,
        help='limits the number of words per author.'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='whether to print debug messages'
    )

    args = parser.parse_args()

    # If the user does not wish for verbose mode, replace the
    # verbose function with a lambda function that does nothing
    if not args.verbose:
        global verbose_print
        verbose_print = lambda *a: None

    sc  = SparkContext()

    rdd = sc.binaryFiles(args.input_path)
    rdd = rdd.map(partial(get_author, args.default_encoding)).mapValues(word_count).reduceByKey(my_merge)
    rdd = rdd.mapValues(partial(sort_dict, args.sort_by_word))
    rdd = rdd.mapValues(lambda x: x[:args.limit])
    print(rdd.map(format_to_csv_str).saveAsTextFile(args.output_path))

if __name__ == '__main__':
    main()

# MC857-project2
MC851/MC855/MC857 - Projeto 2
Professora Islene

RA024374 - Lucas de Magalhães Araujo
RA103158 - Lucas Lustosa Madureira
RA136034 - Gustavo Taminato Imazaki
RA146898 - Leonardo Yvens

### About

This project implements a variation of word-count over a set of ebooks using Spark RDDs. Given a set of books from Project Gutenberg, the program outputs a list of most used words by author. The main purpose of this project is to test and get acquainted with some of Spark's funcionalities.

### Content

**AuthorWordCount.py** - project's source code in python.

**dataset/** - since the project depends on books in portuguese gathered at the Gutenberg Project, this folder contains a data set of 536 books.

###  Usage

This project depends on python3, so, to tell Spark to use the correct python version, execute:

	export PYSPARK_PYTHON=python3

To run the project with basic settings, execute:

	path/to/spark/bin/spark-submit AuthWordCount.py

After completion, a **output/** folder will be created containing the program's result. For more usage options, there is a "help" flag:

	path/to/spark/bin/spark-submit AuthWordCount.py --help
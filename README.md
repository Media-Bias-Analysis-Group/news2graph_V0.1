# news2graph - Retrieval, Transformation and Analysis of News Articles using Graph Text Representation

`news2graph` provides a framework for retrieving, transforming and analyzing online news articles using graph theoretical approaches. 
I wrote this framework for my Masters Thesis @ University of Konstanz within the Media Bias Group. It is build upon the [corpus2graph](https://github.com/zzcoolj/corpus2graph) package and relies on [news-please](https://github.com/fhamborg/news-please) for retrieval.

Currently, only Neo4j is supported. 

## Features
1. Scraping articles
2. Transformation Pipeline
   1. Generating Graph Representation
   2. Applying NLP
   3. Export to generalizable file format
3. Managing docker images of graph databaes
4. Managing graph databses within docker containers
   1. Batch import
   2. Batch updates
   3. General queries
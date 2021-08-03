from .util import _compose_all_weighted_graphs, compose_all_weighted_graphs, take_from_iter, merge_unique_to_single_file, mkdir_if_not_exits

import networkx

import textacy
import textacy.io
import textacy.spacier
from textacy.preprocessing import normalize as n
from textacy.preprocessing import replace as r

import spacy

import os
import os.path
import shutil

from typing import Union
from spacy.tokens import Doc


CHUNKSIZE = 1_000

class TextToGraph(object):
    def __init__(self, input_path: str, output_path:  str, file_pattern: str, sliding_window_width: int = 4, ignore_sentence_boundaries: bool = True, sep: str = '\t', attributes_to_extract: list = ['title', 'authors', 'date_publish'], extract_ner: bool = True):
        # TODO: Metadata from kwargs or object
        self.input_path = input_path
        self.output_path = output_path
        self.file_pattern = file_pattern
        self.sliding_window_width = sliding_window_width
        self.ignore_sentence_boundaries = ignore_sentence_boundaries
        self.sep = sep
        
        if not os.path.exists(output_path):
            os.mkdir(output_path)

        self.paths = {}

        self.paths['articles'] = os.path.join(output_path, 'articles')
        self.paths['graphs'] = os.path.join(output_path, 'graphs')
        self.paths['headers'] = os.path.join(output_path, 'headers')
        self.paths['nodes'] = os.path.join(output_path, 'nodes')
        self.paths['words'] = os.path.join(output_path, 'words')

        for k, v in self.paths.items():
            mkdir_if_not_exits(v)

        # make sure no duplicate attributes and ensure that url is set as key
        self.attributes_to_extract = list(set(attributes_to_extract))
        if 'url' in self.attributes_to_extract: self.attributes_to_extract.remove('url')
        with open(f'{self.paths["headers"]}/header_article.txt', 'w') as f:
            f.write(f'url:ID(Article){self.sep}')
            f.write(self.sep.join(self.attributes_to_extract))
        self.attributes_to_extract.append('url')
        

        with open(f'{self.paths["headers"]}/header_ai.txt', 'w') as f:
            f.write(f':START_ID(Token){self.sep}:END_ID(Token)')
        

        self.extract_ner = extract_ner
        if not os.path.exists(os.path.join(output_path, 'ne')) and extract_ner:
            os.mkdir(os.path.join(output_path, 'ne'))
            with open(f'{os.path.join(output_path, "headers")}/header_ne.txt', 'w') as f:
                f.write('person:ID(Person)')
        if not os.path.exists(os.path.join(output_path, 'ne_article')) and extract_ner:
            os.mkdir(os.path.join(output_path, 'ne_article'))
            with open(f'{os.path.join(output_path, "headers")}/header_ne_article.txt', 'w') as f:
                f.write(f':START_ID(Person){self.sep}:END_ID(Article)')
        


        
    def _getfiles(self):
        # TODO: check if path exists
        self.files = textacy.io.get_filepaths(
            self.input_path, extension=self.file_pattern)
        self.file_iter = self.files.__iter__()

    def _readfiles(self, files: list) -> None:
        
        assert self.files
        if hasattr(self, 'docs'): del self.docs
        self.docs = []
        _files = iter(files) if files else self.files
        for file in _files:
            records = textacy.io.read_json(file)
            for record in records:
                attr = {k: record.get(k, 'null') for k in self.attributes_to_extract}
                attr['filename'] = os.path.basename(file)
                doc = textacy.make_spacy_doc((
                    #TODO: parameterize
                    # pass tuple with element 0 being the text and element 1 the metadata from json fields
                    self._normalize(record['maintext']),
                    # {'title': record['title'], 'authors': record['authors'],
                    #     'url': record['url'], 'filename': os.path.basename(file)}
                    attr
                ), lang='en')
                # don't add empty docs
                if doc.__len__() > 0:
                    self.docs.append(doc)

    def process(self) -> None:
        # TODO: add logging statements
        #self._readfiles()
        
        self._getfiles()
        self.graphs = []

        self.chunk_nr = 1
        while True:
            # Free memory
            if hasattr(self, 'docs'): del self.docs
            if hasattr(self, 'graphs'): del self.graphs
            if hasattr(self, 'graph'):  del self.graph

            chunk = take_from_iter(CHUNKSIZE, self.file_iter)
            if not chunk:
                break
            else:
                # clears and fills self.docs
                self._readfiles(chunk)

                self.graphs = []
                self.graph = ''
                for doc in self.docs:
                    if self.extract_ner:
                        #TODO: filter named entities by label
                        nes = list(set([d.string.strip() for d in doc.ents if d.label_ == 'PERSON']))
                        with open(f'{self.output_path}/ne/ne_{doc._.meta.get("filename")}.txt', 'w') as f:
                            for ne in nes:
                                f.write(f'{ne}\n')
                        with open(f'{self.output_path}/ne_article/ne_{doc._.meta.get("filename")}.txt', 'w') as f:
                            for ne in nes:
                                f.write(f'{ne}{self.sep}{doc._.meta.get("url")}\n')
                    if self.ignore_sentence_boundaries:
                        graph = textacy.spacier.doc_extensions.to_semantic_network(
                            doc, window_width=self.sliding_window_width)
                    else:
                        doc_graphs = [textacy.spacier.doc_extensions.to_semantic_network(
                            sent, window_width=self.sliding_window_width) for sent in doc.sents]
                        graph = compose_all_weighted_graphs(doc_graphs)
                    # Add article key to edge so we can differentiate later
                    # networkx.set_edge_attributes( 
                    #    graph, doc._.meta.get('url'), 'article_id')
                    self.graphs.append(graph)
                
                self.graph = _compose_all_weighted_graphs(self.graphs)
                self._write()
                self.chunk_nr += 1

        self.graphs = []
        
        #process ner
        merge_unique_to_single_file(f'{self.output_path}/ne/', '.txt', f'{self.output_path}/nodes/ne_merged.txt')

        #TODO: convert token to int?
        #TODO: this condition is not necessary i think
        if self.chunk_nr >= 1:
            path = os.path.join(self.output_path, 'graphs')
            _graphs = [g for g in [os.path.join(self.output_path, 'graphs',f) for f in os.listdir(path) if f.endswith('.pickle')]]
            final_graph = _compose_all_weighted_graphs(_graphs)
            networkx.write_edgelist(final_graph, f'{self.output_path}/final_graph.txt', data=['weight'], delimiter=self.sep)
            with open(f'{self.output_path}/nodes/words.txt', 'w') as f:
                for word in final_graph.nodes():
                    f.write(f'{word}\n')
            with open(f'{self.output_path}/headers/header_words.txt', 'w') as f:
                f.write('token:ID(Token)')
            with open(f'{self.output_path}/headers/header_graph.txt', 'w') as f:
                f.write(f':START_ID(Token){self.sep}:END_ID(Token){self.sep}weight:int')
    
    def merge_graphs(self):
        path = os.path.join(self.output_path, 'graphs')
        _graphs = [g for g in [os.path.join(self.output_path, 'graphs',f) for f in os.listdir(path) if f.endswith('.pickle')]]
        final_graph = _compose_all_weighted_graphs(_graphs)
        networkx.write_edgelist(final_graph, f'{self.output_path}/final_graph_2.txt', data=['weight'], delimiter=self.sep)
        with open(f'{self.output_path}/nodes/words.txt', 'w') as f:
            for word in final_graph.nodes():
                f.write(f'{word}\n')
        with open(f'{self.output_path}/headers/words.txt', 'w') as f:
            f.write('token:ID')



    def _write(self):

        # write headers here
        for doc, graph in zip(self.docs, self.graphs):
            #if self.preserve_article_gow:
            #networkx.write_edgelist(
            #    graph, f'{self.output_path}/gow_{doc._.meta.get("filename")}.txt', data=['weight'], delimiter=self.sep)

            article_node_edges = [node for node, graph in graph.nodes.items()]
            attributes = doc._.meta
            filename = attributes.pop('filename')
            article_id = attributes.pop('url')

            self.write_node_article_edges(
                filename=filename, article=article_id, edges=article_node_edges)
            self._write_article(filename=filename, article=article_id, meta = attributes)


        # Write merged graphs

        networkx.write_edgelist(self.graph, f'{self.output_path}/merged_graph_{self.chunk_nr}.txt', data=['weight'], delimiter=self.sep)
        networkx.write_gpickle(self.graph, f'{self.output_path}/graphs/merged_graph_{self.chunk_nr}.pickle')

    def write_words(self):
        assert self.graph

    def write_node_article_edges(self, filename: str, article: str, edges: list):
        with open(f'{self.output_path}/words/word_article_{filename}.txt', 'w') as file:
            for edge in edges:
                file.write(f'{edge}{self.sep}{article}\n')

    def _write_article(self, filename: str, article: str, meta: dict):
        #if not self.inferred_article_header:
            
        with open(f'{self.output_path}/articles/article_{filename}.txt', 'w') as file:
            if not meta or len(meta) == 0:
                file.write(f'{article}')
            else:
                file.write(f'{article}{self.sep}{self.sep.join([str(v) for k, v in meta.items()])}')

    def _normalize(self, text: str) -> str:
        if text:
            return n.normalize_hyphenated_words(n.normalize_quotation_marks(r.replace_currency_symbols(r.replace_numbers(text, replace_with=''), replace_with='')))
        else:
            return ''

    def _write_headers(self, path: str, headers: list):
        # final_graph_header
        with open(f'{self.output_path}/header_final_graph.txt', 'w') as f:
            f.write(f':START_ID(Token){self.sep}:END_ID(Token){self.sep}weight:int')



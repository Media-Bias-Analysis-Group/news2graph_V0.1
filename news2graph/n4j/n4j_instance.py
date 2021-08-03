from __future__ import annotations

from py2neo import Graph, Cursor
from py2neo.matching import NodeMatcher

from typing import List

import time


NodeList = List[str]


class N4JInstance(object):
    def __init__(self, host: str, password: str, port: int, retries: int = 5):
        # Sometimes the n4j instance is not immeddiately ready after starting
        for _ in range(retries):
            try:
                self.graph = Graph(host=host, password=password, port=port)
                _ = retries
            except:
                print(f'Error on attempt {_}. Retrying in {_*2} second.')
                time.sleep(_*2)

    @classmethod
    def from_config_file(cls, config_file: str) -> self:
        # readconfig here
        return cls()

    @classmethod
    def from_container(cls, container: Container) -> N4JInstance:
        container.reload()
        if not container.status == 'running':
            container.start()
            container.reload()

        return cls(
            host='localhost',
            password=container.attrs['Config']['Env'][0].split('/')[1],
            port=int(container.ports['7687/tcp'][0]['HostPort'])
        )

    def get_node_list(self, node_label: str) -> NodeList:
        matcher = NodeMatcher(self.graph)
        result = [node for node in matcher.match(node_label)]
        return result

    def init_schema(self) -> None:
        # self.create_unique_constraint('Article', 'article_id')
        # self.create_unique_constraint('Token', 'token_id')
        # self.create_index('Author', ['author', 'publisher'])
        raise NotImplementedError 

    def create_unique_constraint(self, label: str, key: str) -> bool:
        try:
            self.graph.schema.create_uniqueness_constraint(label, key)
        except:
            return False    
        return True

    def create_index(self, label: str, keys: List[str]) -> bool:
        try:
            self.graph.schema.create_index(label, keys)
        except:
            return False
        return True

    def create_nodes(self, node_label: str, node_data: list, key: str) -> bool:
        tx = self.graph.begin()
        create = f'UNWIND $batch as row \
                MERGE (n:{node_label} {{{key}: row["{key}"]}}) \
                SET n += row'
        cursor = tx.run(create, batch=node_data)
        tx.commit()
        return cursor

    def create_relationships(self, rel_type: str, from_node: (str, str), to_node: (str, str), rel_data: list) -> Cursor:
        """Creates relationships between indexed nodes

        Args:
            from_node (Node): Give node as tuple with (node_label, key_property)
            to_node (Node): Give node as tuple with (node_label, key_property)
            rel_data (list): List of dicts containing relationships
            rel_type (str): Give the name of the relationships to be created
        """
        tx = self.graph.begin()
        create = f'UNWIND $batch as row \
            MERGE (u:{from_node[0]} {{ {from_node[1]}: row["{from_node[1]}"] }}) \
            MERGE (v:{to_node[0]} {{ {to_node[1]}: row["{to_node[1]}"] }}) \
            MERGE (u)-[r:{rel_type}]->(v)'
        cursor = tx.run(create, batch = rel_data)
        tx.commit()
        return cursor

    def update_relationships(self, rel_type: str, from_node: (str, str), to_node: (str, str), rel_data: list):
        raise NotImplementedError

    def get_viz(self):
        # use http
        # https://neo4j.com/docs/http-api/3.5/actions/return-results-in-graph-format/
        # https://py2neo.org/2020.0/client/http.html?highlight=http#module-py2neo.client.http
        raise NotImplementedError


    def pagerank(self, node: str, relationship: str, write_to_property:str, weight:str= 'weight') -> Cursor:
        #TODO: think about concurrency
        projection = f'nodeProjection: "{node}", relationshipProjection: "{relationship}", relationshipProperties: "{weight}"'
        configuration = f' writeProperty: "{write_to_property}", relationshipWeightProperty: "{weight}"'

        query = f'CALL gds.pageRank.write({{ {projection}, {configuration}, maxIterations: 20, dampingFactor: 0.85 }})' \
                f'YIELD nodePropertiesWritten, didConverge, createMillis, computeMillis'

        cursor = self.graph.run(query)
        return cursor
        

    def louvain(self, node: str, relationship: str, write_to_property:str, weight:str= 'weight') -> Cursor:
        projection = f'nodeProjection: "{node}", relationshipProjection: "{relationship}", relationshipProperties: "{weight}"'
        configuration = f'writeProperty: "{write_to_property}", relationshipWeightProperty: "{weight}"'

        query = f'CALL gds.louvain.write({{ {projection}, {configuration} }}) ' \
                f'YIELD communityCount, modularity, modularities, computeMillis'

        cursor = self.graph.run(query)
        return cursor
        

    def jaccard(self, source_label: str, relationship_label: str, target_label: str, cutoff: float, write_to_relationship:str, write_to_property:str):
        # Aggregate words per article
        self.graph.run('CALL gds.graph.drop("jaccard-graph", false) YIELD graphname')
        self.graph.run(f'CALL gds.graph.create("jaccard-graph", ["{source_label}", "{target_label}"], {{ {relationship_label} : {{ type: {relationship_label}, orientation: "REVERSE" }} }})')
        
        #projection = f'MATCH (source:{source_label}-[:{relationship_label}]->(target:{target_label})'
        #aggegration = 'WITH {item: id(target), categories: collect(id(source))} as doc_dict ' \
        #              'WITH collect(doc_dict) as data'
        
        jaccard = f'CALL gds.nodeSimilarity.write("jaccard-graph", {{ topK: 1, similarityCutoff: {cutoff}, writeRelationshipType: {write_to_relationship}, writeProperty: {write_to_property} }}) YIELD nodesCompared, relationshipsWritten, computeMillis)'


    def get_communities_file_count(self):
        query = 'MATCH (a:Article) RETURN a.community, collect(a.url), count(a.url) as file_count ORDER BY file_count DESC '
import unittest
import docker

from news2graph.n4j.n4j_instance import N4JInstance
from news2graph.dockerized.n4j_docker_instance import Neo4jDockerInstance

class TestN4JInstance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        docker_client = docker.from_env()
        ports = {
            '7474': 60_005,
            '7687': 60_006}
        #cls.n4j = docker_client.containers.run('neo4j', remove=True,
        #                                        environment=['NEO4J_AUTH=neo4j/test'],
        #                                         ports=ports, detach = True)
        #cls.n4j_instance = N4JInstance.from_container(cls.n4j)
        cls._nodes = [{'name': 'Ben', 'age': 23}, {'name': 'Jenna', 'age': 42}]

        n4j = Neo4jDockerInstance()

        #cls.n42 = n4j.init_db_from_data(data_folder='/home/jakob/dev/test_db', ports = (60_007, 60_008))
        #cls.n4j_instance2 = N4JInstance.from_container(cls.n42)
    

    def test_from_container(self):
        self.assertIsInstance(self.n4j_instance, N4JInstance)
        #self.assertIn('graph', self.n4j_instance)


    def test_create_nodes(self):
        stats = self.n4j_instance.create_nodes('Author', node_data = self._nodes, key = 'name').stats()
        self.assertTrue(
            2 == stats.nodes_created
        )

    def test_node_list(self):
        nodes = self.n4j_instance.get_node_list('Author')
        for expected, actual in zip(self._nodes, nodes):
            self.assertDictEqual(expected, dict(actual))

    def test_edge_insert(self):
        pass

    
    def test_pagerank(self):
        n4j = Neo4jDockerInstance()
        container = Neo4jDockerInstance.start_db('ab263f09c7c3')
        #n42 = n4j.init_db_from_data(data_folder='/home/jakob/dev/n4j', ports = (60_007, 60_008))
        
        n4j_instance = N4JInstance.from_container(container)
    
        stats = n4j_instance.pagerank('Token', 'COOCURS_WITH', 'pagerank')
        
        print(stats)
        print(stats.stats())



    def test_louvain(self):
        n4j = Neo4jDockerInstance()
        container = Neo4jDockerInstance.start_db('ab263f09c7c3')
        #n42 = n4j.init_db_from_data(data_folder='/home/jakob/dev/n4j', ports = (60_007, 60_008))
        
        n4j_instance = N4JInstance.from_container(container)
    
        stats = n4j_instance.louvain('Token', 'COOCURS_WITH', 'louvain')
        print(next(stats)['computeMillis'])

        stats = n4j_instance.louvain('Article', 'IS_SIMILAR', 'community', weight='')
        print(stats.stats())


    @classmethod
    def tearDownClass(cls):
        #cls.n4j.stop()
        pass
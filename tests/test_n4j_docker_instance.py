import unittest
from news2graph.dockerized.n4j_docker_instance import Neo4jDockerInstance
import shutil

class TestN4JDockerInstance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.container_id = None

    def test_db_creation(self):
        input_path = '/home/jakob/dev/test4'
        output_path = '/home/jakob/dev/n4j4'

        shutil.rmtree(output_path)

        log_path = '/home/jakob/dev/n4j3/logs'

        nr_object = {
            'Article': ('Nodes', 'headers/header_article.txt, articles/article.*'),
            'Token': ('Nodes', 'headers/header_words.txt, nodes/words.txt'),
            'COOCURS_WITH': ('Relationship', 'headers/header_graph.txt, final_graph.txt'),
            'APPEARS_IN': ('Relationship', 'headers/header_ai.txt, words/word.*'),
            'Person': ('Nodes', 'headers/header_ne.txt, nodes/ne_merged.txt'),
            'MENTIONED_ID': ('Relationship', 'headers/header_ne_article.txt, ne_article/ne.*')
        }

        n4j = Neo4jDockerInstance()

        n4j.init_db_from_graph(import_folder_path= input_path, output_folder_path= output_path, node_rel_object = nr_object, log_folder_path =log_path)
        

    def test_start_from_folder(self):
        
        n4j = Neo4jDockerInstance()

        container = n4j.init_db_from_data(data_folder='/home/jakob/dev/n4j4', ports = (60_001, 60_002))

        print(container)
        self.assertIsNotNone(container)
        print(container.id)
        self.container_id = container.id
        


    def test_start_existing_db(self):

        n4j = Neo4jDockerInstance()
        n4j.start_db('8f463dd35a8a')

if __name__ == '__main__':
    unittest.main()
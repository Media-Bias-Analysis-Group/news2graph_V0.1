import unittest
from news2graph.text2graph import TextToGraph


class TestTextToGraph(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input_path = '/home/jakob/dev/master/corpora/1kaxios/news/'
        cls.full_path = '/home/jakob/news-please-repo/data/2020/10/16/axios.com'
        cls.file_pattern = '.json'
        cls.output_path = '/home/jakob/dev/test'
        cls.t2g = TextToGraph(input_path= '/home/jakob/news-please-repo/data/2020/10/16/axios.com',
                                  output_path='/home/jakob/dev/test4/',
                                  file_pattern='.json'
                                  )

    def test_file_list(self):
        self.t2g._getfiles()
        self.assertIsNotNone(self.t2g.files)

    def test_read_files(self):
        self.t2g._readfiles()
        self.assertIsNotNone(self.t2g.docs)

    def test_generate_graphs(self):
        self.t2g.process()
        #self.assertIsNotNone(self.t2g.graph)

    def test_graph_merge(self):
        self.t2g.merge_graphs()

    def test_graph_write(self):
        self.t2g._write()


if __name__ == '__main__':
    unittest.main()
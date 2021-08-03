import unittest
from news2graph.chunked_file_reader import ChunkedFileReader

class TestFileReader(unittest.TestCase):
    def setUp(self):
        self.aute = ChunkedFileReader(
            base_path='/home/jakob/dev/master/corpora/1kaxios/out/dicts_and_encoded_texts/',
            header=['author_id', 'article_id', 'label'],
            startswith='edges_author',
            endswith='')
        self.aut = ChunkedFileReader(
            base_path='/home/jakob/dev/master/corpora/1kaxios/out/',
            header=['author_id', 'publisher', 'label'],
            startswith='nodes_author',
            endswith='')
        self.art = ChunkedFileReader(
            base_path='/home/jakob/dev/master/corpora/1kaxios/out/dicts_and_encoded_texts/',
            header=['article_id', 'date_published', 'title', 'url'],
            startswith='nodes_article',
            endswith='')

    def test_file_list(self):
        self.assertIsNotNone(self.aut.files)
        self.assertEquals(len(self.aut.files), 1)




if __name__ == '__main__':
    unittest.main()
import os
import mmap
import collections
from itertools import islice

class ChunkedFileReader(object):
    def __init__(self, base_path:str, header: list, startswith:str, endswith:str, sep:str = '\t'):
        self.base_path = base_path
        self.startswith = startswith
        self.endswith = endswith
        self.files  = self._generate_file_list()
        self.generator = self._read_csv_list(self.files)
        self.iter = iter(self.generator)
        self.header = header
        self.sep = sep

    def __call__(self, chunksize: int):
        return islice(self.iter, chunksize)


    def _generate_file_list(self):
        return [
            os.path.join(self.base_path, file) for file in os.listdir(self.base_path) if
            file.startswith(self.startswith) and file.endswith(self.endswith)
        ]

    def _read_csv_list(self, file_list: str):
        #TODO: look for efficiency gains
        for file in file_list:
            with open(file, 'r') as f:
                for line in f:
                    yield dict(zip(self.header,line.split(self.sep)))
                #TODO: this code currently bugs out and returns an additional empty dict per file
                #with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
                #    for line in str(mm.read()).split('\\n'):
                #        yield line.split('\\t')

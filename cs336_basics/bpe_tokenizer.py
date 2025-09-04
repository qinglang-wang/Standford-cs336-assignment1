import os
import subprocess
import numpy as np
from typing import BinaryIO
from qinglang.utils.utils import Config

class BPETokenizer:
    def __init__(self):
        self.config = Config(
            num_read_processes=4,
        )
        self.PAT = r"""'(?:[sdmt]|ll|ve|re)| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s+"""

    def read(self, path: str):
        def find_chunk_boundaries(f: BinaryIO) -> list[int]:
            f.seek(0, os.SEEK_END)
            f_length = f.tell()
            chunk_length = f_length // self.config.num_read_processes
            
            f.seek(0, os.SEEK_SET)
            
            ...
        with open (path, 'rb') as f:
            boundaries = ...
            ...

    def pre_tokenize(self):
        ...
import os
import numpy as np
from typing import List, BinaryIO
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from qinglang.utils.utils import Config


class BPETokenizer:
    def __init__(self):
        self.config = Config(
            path = rf"E:\courses\cs336\assignment1\data\TinyStoriesV2-GPT4-train.txt",
            pre_tokenize = Config(
                MAX_WORKERS = 4,
                CHUNK_UNIT_SIZE = 1024 * 1024 * 200,
                READ_UNIT_SIZE = 4 * 1024,
            )
        )
        self.PAT = r"""'(?:[sdmt]|ll|ve|re)| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s+"""

    def pre_tokenize(self):
        def divide_chunks(path: str, delimiter: bytes=b'\r\n') -> List[List[int]]:
            length = os.path.getsize(path)
            num_chunks = length // self.config.pre_tokenize.CHUNK_UNIT_SIZE + 1
            boundaries = [i * self.config.pre_tokenize.CHUNK_UNIT_SIZE for i in range(num_chunks)] + [length]
            with open(path, 'rb') as f:
                for chunk_idx, boundry in list(enumerate(boundaries))[1:]:
                    f.seek(boundry)
                    while True:
                        read_chunk = f.read(self.config.pre_tokenize.READ_UNIT_SIZE)
                        if read_chunk == b'':
                            break

                        if (offset := read_chunk.find(delimiter)) != -1:
                            boundaries[chunk_idx] = boundry + offset
                            break

                        boundry += self.config.pre_tokenize.READ_UNIT_SIZE

            return list(zip(boundaries[:-1], boundaries[1:]))

        def subprocess(path: str, chunk_boundary: List) -> Future:
            with open(path, 'rb') as f:
                f.seek(chunk_boundary[0])
                content = f.read(chunk_boundary[1] - chunk_boundary[0])

                return content

        chunk_boundaries = divide_chunks(self.config.path, delimiter=b'<|endoftext|>')

        results = []
        with ProcessPoolExecutor(self.config.pre_tokenize.MAX_WORKERS) as executor:
            futures = [executor.submit(subprocess, self.config.path, chunk_boundary) for chunk_boundary in chunk_boundaries]

            for future in as_completed(futures):
                results.append(future.result())

        return results
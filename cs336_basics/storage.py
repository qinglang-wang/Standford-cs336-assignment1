import os
import numpy as np
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import List

MAX_WORKERS = 4
CHUNK_UNIT_SIZE = 1024 * 1024 * 200
READ_UNIT_SIZE = 4 * 1024

def divide_chunks(path: str, delimiter: bytes=b'\r\n') -> List[List[int]]:
    length = os.path.getsize(path)
    num_chunks = length // CHUNK_UNIT_SIZE + 1
    boundaries = [i * CHUNK_UNIT_SIZE for i in range(num_chunks)] + [length]
    with open(path, 'rb') as f:
        for chunk_idx, boundry in list(enumerate(boundaries))[1:]:
            f.seek(boundry)
            while True:
                read_chunk = f.read(READ_UNIT_SIZE)
                if read_chunk == b'':
                    break

                if (offset := read_chunk.find(delimiter)) != -1:
                    boundaries[chunk_idx] = boundry + offset
                    break

                boundry += READ_UNIT_SIZE

    return list(zip(boundaries[:-1], boundaries[1:]))

def subprocess(path: str, chunk_boundary: List) -> Future:
    with open(path, 'rb') as f:
        f.seek(chunk_boundary[0])
        content = f.read(chunk_boundary[1] - chunk_boundary[0])

        return content

def parallel_read(path: str):
    chunk_boundaries = divide_chunks(path, delimiter=b'<|endoftext|>')

    results = []
    with ProcessPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(subprocess, path, chunk_boundary) for chunk_boundary in chunk_boundaries]

        for future in as_completed(futures):
            results.append(future.result())

    return results

if __name__ == "__main__":
    # path = r"E:\courses\cs336\assignment1\data\owt_valid.txt"
    path = r"E:\courses\cs336\assignment1\data\TinyStoriesV2-GPT4-train.txt"
    # path = r"E:\courses\cs336\assignment1\data\test.txt"
    # print(os.path.getsize(path))
    # with open(path, "rb") as file:
    #     file.seek(0, os.SEEK_END)
    #     file_size = file.tell()
    #     print(file_size)
    # print(divide_chunks(path, delimiter=b'<|endoftext|>'))
    # print(os.path.getsize(path))
    # with open(path, "rb") as file:
    #     file.seek(0, os.SEEK_END)
    #     file_size = file.tell()
    #     print(file_size)
    data = parallel_read(path)
    print([ls[:20]+ls[-20:] for ls in data])
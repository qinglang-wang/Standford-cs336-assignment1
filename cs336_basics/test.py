import os
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed

MAX_WORKERS = 4

def get_chunks():
    ...

def subprocess():
    ...

def parallel_read():
    chunks = get_chunks()

    results = []
    with ProcessPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(subprocess, chunk) for chunk in chunks]

        for future in as_completed(futures):
            results.extend(future.result())

    return results
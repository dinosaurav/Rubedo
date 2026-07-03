from concurrent.futures import ProcessPoolExecutor
def f(): raise ValueError("foo")
if __name__ == "__main__":
    pool = ProcessPoolExecutor(1)
    future = pool.submit(f)
    print(type(future.exception()), future.exception())

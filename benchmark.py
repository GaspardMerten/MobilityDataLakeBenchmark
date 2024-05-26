import json
import os
import random
import time
import uuid

from stores.apache_parquet import ApacheParquetStore
from stores.motion_lake import MotionLakeStore

stores = [
    (ApacheParquetStore(timestamp_group=1), 1),
    (MotionLakeStore(), 2),
]


MAX_DOCUMENTS = 100
RANDOM_READS = 1000


# Json parser for uuids
class UUIDEncoder:
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return obj


def data_iterator():
    # yield the json of each file in data, sorted by name ascending
    for file in os.walk("data"):
        files = file[2]
        # Sort files by name
        files.sort()
        for i, f in enumerate(files):
            if i >= MAX_DOCUMENTS:
                break

            with open(f"data/{f}", "r") as file:
                yield json.load(file), f.split(".json")[0]


def benchmark():
    benchmark_time = time.time()
    timestamps = [timestamp for _, timestamp in data_iterator()]

    size_stats = {}
    write_stats = {}
    read_stats = {}

    for store, enabled_read_bench in stores:
        name = store.__class__.__name__ if not hasattr(store, "name") else store.name()

        print(f"Running benchmark for {name}")
        store.reset()
        store_start = time.time()
        for data, timestamp in data_iterator():
            store.store_document(data, timestamp)
        print(
            f"{name} took {store.get_total_size() // 1024 / 1024} MB to store {MAX_DOCUMENTS} documents"
        )

        size_stats[name] = store.get_total_size() // 1024 / 1024

        store_end = time.time()
        print(
            f"{name} took {store_end - store_start} seconds to store {MAX_DOCUMENTS} documents"
        )
        write_stats[name] = store_end - store_start

        if not enabled_read_bench:
            continue

        start = time.time()
        for i in random.choices(timestamps, k=RANDOM_READS):
            store.get_document(i)

        end = time.time()

        print(f"{name} took {end - start} seconds to get {RANDOM_READS} documents")
        read_stats[name] = end - start

        # Write all stats to a file
        with open(
            f"results/benchmark_results_store_{benchmark_time}.json", "w"
        ) as file:
            json.dump(
                {
                    "write_stats": write_stats,
                    "size_stats": size_stats,
                    "read_stats": read_stats,
                },
                file,
            )
    import matplotlib.pyplot as plt

    def plot_stats(stats, title):
        plt.figure(figsize=(20, 10))
        # Reverse plot (so X axis is the store name)
        plt.barh(list(stats.keys()), list(stats.values()))
        plt.title(title)
        plt.show()

    plot_stats(write_stats, "Write times")
    plot_stats(size_stats, "Size")
    plot_stats(read_stats, "Read times")


if __name__ == "__main__":
    benchmark()

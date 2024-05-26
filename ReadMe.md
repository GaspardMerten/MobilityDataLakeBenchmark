# Mobility Data Lake Benchmark

This repository contains the code and data utilities 
to benchmark the performance of multiple data management 
systems on mobility data.

## Installation

To install the required dependencies, run the following command:

```bash
pip install -r requirements.txt
```

Then, launch all Docker containers by running the following command:

```bash
docker-compose up
```

## Usage

To run the benchmark, execute the following command:

```bash
python benchmark.py
```

You can modify the file to include or exclude specific data management systems.

It is also possible to create new solutions by implementing the `BaseStore` interface.


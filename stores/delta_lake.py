import os
import shutil
from pathlib import Path

import pyspark
from delta import *

from stores.base_store import BaseStore


class DeltaLakeStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = None

    def reset(self):
        # Create folder delta lake if exists, rm it
        shutil.rmtree("delta_lake", ignore_errors=True)
        os.mkdir("delta_lake")
        builder = (
            pyspark.sql.SparkSession.builder.appName("MyApp")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def store_document(self, data: dict, timestamp: str):
        table = self.spark.createDataFrame([{"timestamp": timestamp, "data": data}])
        table.write.format("delta").mode("append").save("delta_lake")

    def get_document(self, timestamp: str):
        return (
            self.spark.read.format("delta")
            .load("delta_lake")
            .where(f"timestamp = '{timestamp}'")
        )

    def get_total_size(self):
        root_directory = Path("delta_lake")
        return sum([
            os.path.getsize(file) for file in root_directory.rglob("*") if file.is_file()
        ])



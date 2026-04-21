import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

load_dotenv()

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_api_key():
    key = os.getenv("ELECTRICITY_MAPS_API_KEY")
    if not key:
        raise ValueError("ELECTRICITY_MAPS_API_KEY not found")
    return key


def build_spark_session(app_name="ElectricityMapsETL"):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.master", "local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.python.worker.reuse", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_base_paths():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

    return {
        "bronze": os.path.join(base, "bronze"),
        "silver": os.path.join(base, "silver"),
        "gold": os.path.join(base, "gold"),
    }

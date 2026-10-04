"""Shared Spark session factory: MinIO via s3a, Cassandra via spark-cassandra-connector, RocksDB state store."""
import os

from pyspark.sql import SparkSession

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "wiki-edits")
# A window is "contested" with at least this many distinct editors. The surge threshold is a quantile over contested
# windows only, and the stream flags only contested windows, so both jobs must agree on it. One bot iterating
# produces the biggest single-editor windows (30 to 52 edits) and is not a surge.
MIN_EDITORS = int(os.environ.get("MIN_EDITORS", "2"))
ARCHIVE = os.environ.get("ARCHIVE", "s3a://archive/")  # trailing slash: s3a rejects a bucket with no path


def session(name, cores="*", **conf):
    # ponytail: local mode (cores = local[N]) on one host; a standalone cluster only if the evaluation needs executors
    b = (SparkSession.builder.appName(name).master("local[%s]" % cores)
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
         .config("spark.hadoop.fs.s3a.endpoint", "http://%s" % os.environ.get("S3_ENDPOINT", "minio:9000"))
         .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_USER", "minioadmin"))
         .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_PASSWORD", "minioadmin"))
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.streaming.stateStore.providerClass",
                 "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
         .config("spark.cassandra.connection.host", os.environ.get("CASSANDRA_HOST", "cassandra")))
    for k, v in conf.items():
        b = b.config(k.replace("__", "."), v)
    return b.getOrCreate()

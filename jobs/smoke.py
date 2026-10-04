"""Smoke test for the pinned versions: Spark reads Kafka and MinIO Parquet, and round-trips a row through Cassandra."""
from common import ARCHIVE, KAFKA, TOPIC, session

spark = session("smoke", cores="2")

kafka_n = spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest").load().count()
print("SMOKE kafka messages:", kafka_n)
assert kafka_n > 0

arc = spark.read.parquet(ARCHIVE)
print("SMOKE archive rows:", arc.count(), "| partitions:", sorted(r[0] for r in arc.select("date").distinct().collect()))
arc.printSchema()

spark.createDataFrame([("smoke", 1)], ["k", "v"]).write.format("org.apache.spark.sql.cassandra") \
    .mode("append").options(table="smoke", keyspace="wiki").save()
back = spark.read.format("org.apache.spark.sql.cassandra").options(table="smoke", keyspace="wiki").load() \
    .filter("k = 'smoke'").collect()
print("SMOKE cassandra round-trip:", back)
assert len(back) == 1 and back[0]["v"] == 1
spark.stop()
print("SMOKE OK")

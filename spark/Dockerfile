FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu
USER root
# Connector jars are resolved once at build time into /opt/ivy so job runs need no network.
# hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262 must match the Hadoop 3.3.4 bundled with Spark 3.5.
ENV PKGS=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1
RUN echo 'from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().stop()' > /tmp/warm.py \
    && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/opt/ivy --packages $PKGS /tmp/warm.py \
    && chmod -R a+rwX /opt/ivy && rm /tmp/warm.py
RUN mkdir -p /opt/spark/conf \
    && printf 'spark.jars.packages %s\nspark.jars.ivy /opt/ivy\n' "$PKGS" > /opt/spark/conf/spark-defaults.conf
# A named volume copies the ownership of the image directory on first use; without this it is root-owned.
RUN mkdir -p /checkpoints /results && chown spark:spark /checkpoints /results
USER spark
WORKDIR /jobs

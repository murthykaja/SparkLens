FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    python3 \
    python3-pip \
    wget \
    curl \
    procps \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    -O /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark \
    && rm /tmp/spark.tgz

RUN mkdir -p /opt/spark/logs/events && chmod -R 777 /opt/spark/logs

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY jobs/ /opt/spark/jobs/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /opt/spark

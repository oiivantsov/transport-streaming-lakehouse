FROM python:3.10-bullseye

# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    openssh-server \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

 # Set up SSH for Airflow
RUN mkdir /var/run/sshd \
 && echo 'root:root' | chpasswd \
 && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
 && sed -i 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' /etc/pam.d/sshd

RUN useradd -ms /bin/bash sparkuser && echo "sparkuser:sparkpass" | chpasswd

# Python libs
RUN pip install --no-cache-dir prometheus-client

# S3 Bucket
ENV S3_BUCKET=my-hsl-lakehouse-bucket
ENV S3_REGION=eu-north-1

# Environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
ENV SPARK_HOME=/spark
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PATH=$PATH:$SPARK_HOME/bin

# Download and setup Spark
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz \
 && tar -xvf spark-3.5.5-bin-hadoop3.tgz \
 && mv spark-3.5.5-bin-hadoop3 $SPARK_HOME \
 && rm spark-3.5.5-bin-hadoop3.tgz

# Configure Spark
RUN mv $SPARK_HOME/conf/log4j2.properties.template $SPARK_HOME/conf/log4j2.properties \
 && mv $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf \
 && mv $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh

# Spark defaults (Delta + Kafka + Postgres)
RUN echo "spark.jars.packages io.delta:delta-spark_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.sql.warehouse.dir s3a://$S3_BUCKET/warehouse" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.endpoint=s3.$S3_REGION.amazonaws.com" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" >> $SPARK_HOME/conf/spark-defaults.conf

WORKDIR /home/spark

# Copy hive-site.xml into Spark config
COPY hive-site.xml $SPARK_HOME/conf/
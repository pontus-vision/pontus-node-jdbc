#
# Copyright (2023) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# ------------------------------------------------
# Dockerfile for Delta Lake quickstart
# ------------------------------------------------

# This docker image uses the official Docker image of [OSS] Apache Spark v3.5.0 as the base container
# Note: Python version in this image is 3.9.2 and is available as `python3`.
# Note: PySpark v3.5.0 (https://spark.apache.org/docs/latest/api/python/getting_started/install.html#dependencies)
ARG BASE_CONTAINER=spark:3.5.1-scala2.12-java17-python3-ubuntu
FROM $BASE_CONTAINER as spark
#FROM spark as delta

# Authors (add your name when updating the Dockerfile)
LABEL authors="Prashanth Babu, Denny Lee, Andrew Bauman, Scott Haines, Tristen Wentling, Leo Martins"

# Docker image was created and tested with the versions of following packages.
USER root
ARG DELTA_SPARK_VERSION="3.2.0"
ENV DELTA_SPARK_VERSION=${DELTA_SPARK_VERSION}
# Note: for 3.0.0 https://pypi.org/project/deltalake/
ARG DELTALAKE_VERSION="0.17.4"
ENV DELTALAKE_VERSION=${DELTALAKE_VERSION}
ARG JUPYTERLAB_VERSION="4.2.1"
ENV JUPYTERLAB_VERSION=${JUPYTERLAB_VERSION}
# requires pandas >1.0.5, py4j>=0.10.9.7, pyarrow>=4.0.0
ARG PANDAS_VERSION="2.2.2"
ARG ROAPI_VERSION="0.11.1"

# We are explicitly pinning the versions of various libraries which this Docker image runs on.
RUN pip install --quiet --no-cache-dir delta-spark==${DELTA_SPARK_VERSION} pyspark \
deltalake==${DELTALAKE_VERSION} jupyterlab==${JUPYTERLAB_VERSION} pandas==${PANDAS_VERSION} roapi==${ROAPI_VERSION}


# Environment variables
#FROM delta as startup
ARG NBuser=NBuser
ARG GROUP=NBuser
ARG WORKDIR=/opt/spark/work-dir
ENV DELTA_PACKAGE_VERSION=delta-spark_2.12:${DELTA_SPARK_VERSION}
ENV DELTA_SPARK_VERSION=${DELTA_SPARK_VERSION}

# OS Installations Configurations
RUN groupadd -g 1000 -r ${GROUP} && \
    useradd -u 1000 -r -m -s /usr/bin/bash -g ${GROUP} ${NBuser}
RUN apt -qq update
RUN apt -qq -y install vim curl tree less

#FROM deltaio/delta-docker:latest
USER root
RUN apt -y update && \
    apt -y install jq

RUN curl -sL https://deb.nodesource.com/setup_20.x | bash - && \
    apt install nodejs -y && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar --output /opt/spark/jars/hadoop-azure-3.3.1.jar &&\
    curl https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar --output /opt/spark/jars/azure-storage-8.6.6.jar &&\
    curl https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar --output /opt/spark/jars/hadoop-shaded-guava-1.1.1.jar &&\
    curl https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.13/httpclient-4.5.13.jar --output /opt/spark/jars/httpclient-4.5.13.jar &&\
    curl https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.13.1/jackson-databind-2.13.1.jars --output /opt/spark/jars/jackson-databind-2.13.1.jars &&\
    curl https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.13.1/jackson-core-2.13.1.jar --output /opt/spark/jars/jackson-core-2.13.1.jar &&\
    curl https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/2.2.0.Final/wildfly-openssl-2.2.0.Final.jar --output /opt/spark/jars/wildfly-openssl-2.2.0.Final.jar &&\
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.1/hadoop-common-3.3.1.jar --output /opt/spark/jars/hadoop-common-3.3.1.jar &&\
    curl https://repo1.maven.org/maven2/com/microsoft/azure/azure-keyvault-core/1.2.6/azure-keyvault-core-1.2.6.jar --output /opt/spark/jars/azure-keyvault-core-1.2.6.jar &&\
    curl https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.54.v20240208/jetty-util-ajax-9.4.54.v20240208.jar --output /opt/spark/jars/jetty-util-ajax-9.4.54.v20240208.jar &&\
    curl https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.54.v20240208/jetty-util-9.4.54.v20240208.jar --output   /opt/spark/jars/jetty-util.9.4.54.v20240208.jar && \
    curl https://repo1.maven.org/maven2/io/delta/delta-hive_2.12/${DELTA_SPARK_VERSION}/delta-hive_2.12-${DELTA_SPARK_VERSION}.jar --output  /opt/spark/jars/delta-hive_2.12-${DELTA_SPARK_VERSION}.jar && \
    curl https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_SPARK_VERSION}/delta-core_2.12-${DELTA_SPARK_VERSION}.jar --output /opt/spark/jars/delta-core_2.12-${DELTA_SPARK_VERSION}.jar &&\
    curl https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_SPARK_VERSION}/delta-spark_2.12-${DELTA_SPARK_VERSION}.jar --output /opt/spark/jars/delta-spark_2.12-${DELTA_SPARK_VERSION}.jar &&\
    curl https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_SPARK_VERSION}/delta-storage-${DELTA_SPARK_VERSION}.jar --output /opt/spark/jars/delta-storage-${DELTA_SPARK_VERSION}.jar


RUN mkdir /usr/local/share/jupyter/lab/extensions && \
    mkdir /usr/local/share/jupyter/lab/settings && \
    mkdir /usr/local/share/jupyter/lab/staging && \
    mkdir /opt/spark/logs && \
    chmod -R a+w /usr/local/share/jupyter/  && \
    chmod 777 /opt/spark/logs
    

RUN python3 -m pip install --upgrade pip && \
    pip install ipython-sql databricks-mosaic==0.4.2 azure-storage-blob azure-identity behave coverage mock tabulate msgraph-sdk --quiet

RUN pip install sparksql-magic  'jupyterlab-lsp' 'python-lsp-server[all]' parquet-cli --quiet && \
    jupyter labextension install @krassowski/jupyterlab-lsp

RUN pip install faker delta-spark --quiet

COPY defaults.conf /opt/spark/config/defaults.conf
COPY defaults.conf /opt/spark/config/spark-defaults.conf

USER ${NBuser}

VOLUME /data
ENV SPARK_WORKER_DIR=/data
ENV SPARK_LOCAL_DIRS=/data
ENV SPARK_MASTER=spark://localhost:7077
ENV HIVE_SERVER2_THRIFT_PORT=10000
ENV SPARK_SUBMIT_OPTIONS="--conf spark.ssl.enabled=true --conf spark.ssl.protocol=TLS --conf spark.ssl.keyStore=/opt/spark/work-dir/certs/keystore.jks --conf spark.ssl.keyStorePassword=pa55word --conf spark.ssl.keyPassword=pa55word --conf spark.ssl.keyStoreType=JKS"

WORKDIR ${WORKDIR}

ENTRYPOINT ["bash", "startup.sh"]

# First-time build can take upto 10 mins.

FROM apache/airflow:2.3.1-python3.8

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

## jdk files and Variables
# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-get -y install git &&\
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \ 
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    java -version $$ \
    javac -version

# Setup JAVA_HOME 
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin/:${PATH}"
RUN export JAVA_HOME
## Finish jdk files and Variables

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

## Spark files and Variables
ARG SPARK_VERSION=3.2.1
ARG HADOOP_VERSION=3.2
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin/:${PATH}"
RUN export SPARK_HOME
# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
    curl -fL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" --output "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"&& \
    tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mkdir -p "${SPARK_HOME}/bin" && \
    mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
## Finish Spark files and Variables

## Start Python packages
USER airflow
COPY requirements.txt .
ENV PYTHONPATH="/usr/local/bin/python"
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH"
ENV AIRFLOW_VERSION=2.3.1
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.8.txt"
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install "apache-airflow[async,postgres,google,apache-spark]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN pip install apache-airflow-upgrade-check
## Finish Python packages
RUN export PYSPARK_PYTHON=/usr/bin/python3
RUN export PYSPARK_DRIVER_PYTHON=ipython3


WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID
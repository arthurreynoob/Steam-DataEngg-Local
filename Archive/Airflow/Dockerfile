FROM apache/airflow:2.4.1

ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
ENV PATH="${HOME}/.local/bin/:$PATH"

###FOR AIRFLOW
ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install apache-airflow-providers-apache-spark

WORKDIR $AIRFLOW_HOME

USER root
COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID



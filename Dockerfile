FROM prefecthq/prefect:2-python3.10
# FROM python:3.10

RUN apt-get update
RUN apt-get install wget
RUN apt-get install nano

COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir


ENV PREFECT_API_URL=http://127.0.0.1:4200/api


# Install Spark
WORKDIR $HOME/spark
RUN wget -N https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
RUN tar -xvzf spark-3.3.2-bin-hadoop3.tgz

# Set environment variables
ENV SPARK_HOME="$HOME/spark/spark-3.3.2-bin-hadoop3"
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:${PYTHONPATH}"

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /opt/prefect

COPY scripts /scripts
RUN chmod +x /scripts/start.sh

CMD ["/scripts/start.sh"]


# CMD ["jupyter", "notebook", "--allow-root" ,"--ip=0.0.0.0", "--port=8888", "--no-browser"]

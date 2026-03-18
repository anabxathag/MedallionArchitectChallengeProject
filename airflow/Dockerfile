FROM apache/airflow:2.11.2

USER root

# Install Java (JRE)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Spark client
ENV SPARK_VERSION=4.0.2
ENV SPARK_HOME=/opt/spark
RUN apt-get update && apt-get install -y wget \
    && wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && apt-get remove -y wget && apt-get clean

# Add spark-submit to PATH
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Fix Spark 4.0.2 ClassNotFoundException for S3A by symlinking JARs into default jars dir
# Note: Extra jars are mounted at /opt/spark/extra_jars in docker-compose.yaml
RUN mkdir -p /opt/spark/extra_jars
RUN ln -s /opt/spark/extra_jars/hadoop-aws-3.4.1.jar /opt/spark/jars/hadoop-aws-3.4.1.jar \
    && ln -s /opt/spark/extra_jars/bundle-2.23.19.jar /opt/spark/jars/bundle-2.23.19.jar \
    && ln -s /opt/spark/extra_jars/postgresql-42.7.4.jar /opt/spark/jars/postgresql-42.7.4.jar

USER airflow

# Install Airflow Spark provider
RUN pip install apache-airflow-providers-apache-spark==5.6.0 pyspark==${SPARK_VERSION}

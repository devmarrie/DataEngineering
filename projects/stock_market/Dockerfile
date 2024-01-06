FROM ubuntu:22.04

# Install java 11 required for pyspark and kafka, python and all required depndancies
RUN apt-get update && apt-get install -y \
        openjdk-11-jdk \
        wget \
        python3 \
        python3-pip

# Install apache kafka
RUN wget https://downloads.apache.org/kafka/3.5.0/kafka_2.12-3.5.0.tgz \
    && tar -xzf kafka_2.12-3.5.0.tgz \
    && mv kafka_2.12-3.5.0.tgz /opt/kafka

# Install pyspark
RUN pip3 install pyspark

# Set the enviroment variable for kafka and spark
ENV KAFKA_HOME /opt/kafka
ENV PATH $PATH:$KAFKA_HOME/bin
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin

# Expose Kafka ports (optional, for external access)
EXPOSE 9092 2181

# Entrypoint script to start Kafka and Spark services (optional)
COPY entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]

# # Set working directory
# WORKDIR /app

# # Copy the project files to the container
# COPY . .

# # Set the entry point for your application
# CMD ["python3", "your_application_script.py"]


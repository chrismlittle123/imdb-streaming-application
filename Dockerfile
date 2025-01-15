FROM apache/spark:latest

# Set working directory
WORKDIR /app

# Switch to root to install packages
USER root

# Install Python dependencies and Hadoop AWS dependencies
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $SPARK_HOME/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P $SPARK_HOME/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.13/httpclient-4.5.13.jar -P $SPARK_HOME/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.13/httpcore-4.4.13.jar -P $SPARK_HOME/jars/

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Switch back to spark user
USER spark

# Copy application files
COPY src/ src/
COPY tests/ tests/

# Set environment variables
ENV PYTHONPATH=/app
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV AWS_REGION=eu-west-2

# Default command to run tests
CMD ["pytest", "tests/test_config.py", "-v"] 
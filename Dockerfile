FROM apache/spark:latest

# Set working directory
WORKDIR /app

# Switch to root to install packages
USER root

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Switch back to spark user
USER spark

# Copy application files
COPY src/ src/
COPY tests/ tests/

# Set environment variables
ENV PYTHONPATH=/app
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Default command to run tests
CMD ["pytest", "tests/test_config.py", "-v"] 
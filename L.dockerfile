# ==========================================
# Multi-stage Dockerfile for ETL Framework
# ==========================================

# Stage 1: Base Python environment with dependencies
FROM python:3.10-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app" \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN groupadd -r etluser && useradd -r -g etluser etluser

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ==========================================
# Stage 2: Spark-enabled image
# ==========================================
FROM base as spark

# Install Java 11 (required for Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV PATH=${SPARK_HOME}/bin:${PATH}

# Install Delta Lake JARs
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -P ${SPARK_HOME}/jars/ && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar -P ${SPARK_HOME}/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar -P ${SPARK_HOME}/jars/

# ==========================================
# Stage 3: Production application
# ==========================================
FROM spark as production

# Copy application code
COPY --chown=etluser:etluser src/ /app/src/
COPY --chown=etluser:etluser config/ /app/config/
COPY --chown=etluser:etluser scripts/ /app/scripts/
COPY --chown=etluser:etluser cli.py /app/
COPY --chown=etluser:etluser setup.py /app/

# Create required directories
RUN mkdir -p /app/logs /app/data /app/tmp && \
    chown -R etluser:etluser /app

# Install the application
RUN pip install -e .

# Switch to non-root user
USER etluser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from src.unified_engine import create_unified_engine; print('OK')" || exit 1

# Default command
CMD ["python", "cli.py", "--help"]

# ==========================================
# Development stage (optional)
# ==========================================
FROM production as development

USER root

# Install development tools
RUN pip install pytest pytest-cov black flake8 mypy jupyter

# Install additional debugging tools
RUN apt-get update && apt-get install -y \
    vim \
    htop \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER etluser

# Expose Jupyter port for development
EXPOSE 8888

# Development command
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
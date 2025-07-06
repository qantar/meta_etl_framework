# 🚀 ETL Framework - Production-Ready Metadata-Driven Data Pipeline

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

A comprehensive, production-ready ETL framework supporting **SQL**, **Python**, and **PySpark** engines with advanced features like **SCD Type 2**, **data quality validation**, **real-time monitoring**, and **intelligent scheduling**.

## 🌟 Key Features

### 🏗️ **Multi-Engine Architecture**
- **Python Engine**: Pandas + SQLAlchemy for medium datasets
- **PySpark Engine**: Apache Spark + Delta Lake for big data
- **SQL Engine**: Pure SQL stored procedures for database-native processing
- **Intelligent Engine Selection**: Automatic engine selection based on data size and complexity

### 📊 **Advanced Data Management**
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (business-ready) layers
- **SCD Type 2 Support**: Complete slowly changing dimension handling
- **Schema Evolution**: Automatic schema change detection and versioning
- **Data Quality Framework**: 20+ built-in validation rules with custom rule support

### 🔄 **Enterprise Orchestration**
- **Dependency Management**: DAG-based job dependencies with circular detection
- **Cron Scheduling**: Advanced scheduling with retry logic and exponential backoff
- **Real-time Monitoring**: Prometheus metrics + Grafana dashboards
- **Comprehensive Auditing**: Full execution tracking and data lineage

### 🛡️ **Production-Ready Security**
- **Secrets Management**: Encrypted password storage with keyring support
- **Configuration Management**: Environment-specific configs with validation
- **Access Control**: Role-based permissions and audit trails
- **Data Encryption**: End-to-end encryption for sensitive data

## 🏁 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.8+ (for local development)
- 4GB+ RAM recommended

### 1. Clone and Setup
```bash
git clone https://github.com/your-company/etl-framework.git
cd etl-framework

# Copy example environment file
cp .env.example .env

# Edit configuration (update passwords, connections, etc.)
vim .env
```

### 2. Start the Complete Stack
```bash
# Start all services (databases, ETL engines, monitoring)
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f etl-python
```

### 3. Initialize and Run First Pipeline
```bash
# Access the CLI
docker-compose exec etl-python bash

# Initialize configuration
etl-cli config init --name production_pipeline

# Discover and register source tables
etl-cli discover tables --register

# Run your first pipeline
etl-cli pipeline run --parallel

# Check results
etl-cli pipeline status --days 1
```

### 4. Access Web Interfaces
- **Grafana Dashboard**: http://localhost:3000 (admin/etl_grafana_password)
- **Kibana Logs**: http://localhost:5601
- **MinIO Storage**: http://localhost:9001 (etl_minio_user/etl_minio_password)

## 📁 Project Structure

```
etl-framework/
├── 🐳 Docker & Deployment
│   ├── Dockerfile                 # Multi-stage production container
│   ├── docker-compose.yml         # Complete stack definition
│   └── requirements.txt           # Python dependencies
│
├── 🧠 Core Framework
│   ├── src/
│   │   ├── unified_engine.py      # Cross-engine orchestrator
│   │   ├── control_audit_system.py # Metadata & audit management
│   │   ├── python_implementation.py # Pandas-based engine
│   │   ├── pyspark_implementation.py # Spark-based engine
│   │   ├── data_quality_framework.py # Quality validation system
│   │   ├── scheduler_orchestrator.py # Job scheduling & dependencies
│   │   ├── notification_service.py    # Alerts & notifications
│   │   └── config_manager.py      # Configuration management
│   │
├── 🎛️ Configuration
│   ├── config/
│   │   ├── production.yaml        # Production configuration
│   │   ├── development.yaml       # Development settings
│   │   └── quality_rules.yaml     # Data quality rules
│   │
├── 📊 Monitoring & Observability
│   ├── monitoring/
│   │   ├── prometheus.yml         # Metrics collection
│   │   ├── grafana/               # Dashboards & datasources
│   │   └── filebeat.yml          # Log shipping
│   │
├── 🗄️ Database Setup
│   ├── sql/
│   │   ├── metadata_ddl.sql       # Control & audit tables
│   │   ├── sample_source_data.sql # Sample source data
│   │   └── target_init.sql        # DWH initialization
│   │
├── 🔧 Tools & Scripts
│   ├── cli.py                     # Command-line interface
│   ├── setup.py                  # Package installation
│   └── scripts/                  # Utility scripts
│
└── 📚 Documentation
    ├── README.md                  # This file
    ├── docs/                      # Detailed documentation
    └── examples/                  # Usage examples
```

## 💻 Usage Examples

### Basic Pipeline Execution

```bash
# Run specific table
etl-cli --config production.yaml pipeline run --table source_db.public.customers

# Run with specific engine
etl-cli pipeline run --engine pyspark --parallel

# Dry run (show execution plan)
etl-cli pipeline run --dry-run --table source_db.public.orders
```

### Configuration Management

```bash
# Initialize new configuration
etl-cli config init \
  --name my_pipeline \
  --source-host postgres-source \
  --target-host postgres-target

# Validate configuration
etl-cli config validate

# Show current configuration
etl-cli config show --output yaml
```

### Data Discovery & Quality

```bash
# Discover tables from specific schemas
etl-cli discover tables --schemas public sales finance

# Check data quality
etl-cli quality check --table customers --generate-report

# View quality history
etl-cli quality status --table customers --days 30
```

### Monitoring & Maintenance

```bash
# Check pipeline health
etl-cli monitor health

# View recent logs
etl-cli monitor logs --lines 100 --follow

# Run cleanup
etl-cli maintenance cleanup --retention-days 30

# Optimize storage
etl-cli maintenance optimize --engine pyspark
```

## 🔧 Configuration

### Environment Variables

```bash
# Database Connections
ETL_SOURCE_HOST=postgres-source
ETL_SOURCE_DATABASE=source_db
ETL_SOURCE_USERNAME=etl_user
ETL_SOURCE_PASSWORD=secure_password

ETL_TARGET_HOST=postgres-target
ETL_TARGET_DATABASE=target_db
ETL_TARGET_USERNAME=etl_user
ETL_TARGET_PASSWORD=secure_password

# Notification Settings
SMTP_HOST=smtp.company.com
SMTP_USERNAME=etl-notifications@company.com
SMTP_PASSWORD=smtp_password
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK

# Security
JWT_SECRET_KEY=your-jwt-secret-key
ENCRYPTION_KEY=your-encryption-key
```

### Pipeline Configuration (YAML)

```yaml
# config/production.yaml
pipeline_name: "production_etl"
default_engine: "pyspark"
enabled_engines: ["python", "pyspark", "sql"]

databases:
  source:
    host: "${ETL_SOURCE_HOST}"
    database: "${ETL_SOURCE_DATABASE}"
    username: "${ETL_SOURCE_USERNAME}"
    password: "${ETL_SOURCE_PASSWORD}"
  
spark:
  executor_memory: "4g"
  executor_cores: 4
  adaptive_enabled: true

performance:
  max_parallel_tables: 10
  batch_size: 50000
  retry_attempts: 3

notifications:
  enabled: true
  slack_enabled: true
  email_enabled: true
```

## 🎯 Advanced Features

### Custom Data Quality Rules

```python
from src.data_quality_framework import QualityRule, QualityRuleType, Severity

# Define custom business rule
revenue_validation = QualityRule(
    rule_id="revenue_positive",
    name="Revenue Must Be Positive",
    rule_type=QualityRuleType.BUSINESS_RULE,
    severity=Severity.HIGH,
    parameters={
        'expression': 'revenue > 0',
        'max_failure_percentage': 1.0
    }
)

# Add to quality framework
quality_framework.add_rule(revenue_validation)
```

### Custom Transformations

```python
from src.transformations.business_rules import BusinessRuleEngine

class CustomTransformations(BusinessRuleEngine):
    def calculate_customer_lifetime_value(self, df):
        """Calculate CLV using custom business logic"""
        return df.withColumn(
            "lifetime_value",
            F.sum("order_total").over(
                Window.partitionBy("customer_id")
            ) * F.lit(1.2)  # 20% growth factor
        )
```

### Scheduled Jobs

```python
from src.scheduler_orchestrator import JobDefinition, TriggerType

# Define scheduled job
daily_customer_sync = JobDefinition(
    job_id="daily_customer_sync",
    name="Daily Customer Data Sync",
    trigger_type=TriggerType.CRON,
    cron_expression="0 2 * * *",  # Daily at 2 AM
    engine_type="pyspark",
    table_filter="customer",
    notify_on_success=True
)

scheduler.add_job(daily_customer_sync)
```

## 📊 Monitoring & Observability

### Grafana Dashboards
- **Pipeline Overview**: Success rates, execution times, data volumes
- **Data Quality**: Quality scores, validation results, trend analysis
- **System Health**: Resource usage, error rates, performance metrics
- **Business Metrics**: Record counts, processing volumes, SLA tracking

### Prometheus Metrics
```
# Job execution metrics
etl_jobs_completed_total{status="success|failed", engine="python|pyspark|sql"}
etl_job_duration_seconds{job_name="...", engine="..."}
etl_records_processed_total{table="...", layer="bronze|silver|gold"}

# Data quality metrics
etl_data_quality_score{table="...", layer="..."}
etl_quality_rule_violations_total{rule_type="...", severity="..."}

# System metrics
etl_memory_usage_bytes{service="..."}
etl_active_connections{database="..."}
```

### Alerting Rules
- **Critical**: Job failures, data quality below 75%, system errors
- **Warning**: Long-running jobs, quality scores below 85%, resource usage
- **Info**: Job completions, configuration changes

## 🛡️ Security & Compliance

### Data Protection
- **Encryption at Rest**: All sensitive data encrypted using AES-256
- **Encryption in Transit**: TLS 1.3 for all database connections
- **Access Control**: Role-based permissions with audit logging
- **Data Masking**: PII anonymization for non-production environments

### Compliance Features
- **GDPR Support**: Data lineage tracking, right to erasure
- **SOX Compliance**: Complete audit trails, change management
- **Data Classification**: Automatic sensitive data detection
- **Retention Policies**: Configurable data retention and cleanup

## 🧪 Testing

### Run Tests
```bash
# Unit tests
pytest tests/unit/ -v --cov=src

# Integration tests
pytest tests/integration/ -v

# Performance tests
pytest tests/performance/ -v --benchmark-only

# Full test suite with coverage
make test-all
```

### Test Data Generation
```bash
# Generate test datasets
python scripts/generate_test_data.py --tables 10 --rows 100000

# Run data quality tests
python scripts/test_data_quality.py --all-tables
```

## 🚀 Deployment

### Production Deployment

```bash
# 1. Clone repository
git clone https://github.com/your-company/etl-framework.git
cd etl-framework

# 2. Configure environment
cp .env.example .env.production
vim .env.production  # Update with production values

# 3. Deploy with Docker Compose
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# 4. Initialize database schemas
docker-compose exec postgres-metadata psql -U etl_user -d metadata_db -f /sql/metadata_ddl.sql

# 5. Configure monitoring
docker-compose exec grafana grafana-cli admin reset-admin-password newpassword

# 6. Start ETL services
docker-compose exec etl-scheduler python -m src.scheduler_orchestrator
```

### Kubernetes Deployment
```bash
# Generate Kubernetes manifests
helm template etl-framework ./helm/ --values values.prod.yaml > k8s-manifests.yaml

# Deploy to Kubernetes
kubectl apply -f k8s-manifests.yaml

# Check deployment status
kubectl get pods -l app=etl-framework
```

## 📈 Performance Tuning

### Spark Optimization
```yaml
spark:
  executor_memory: "8g"
  executor_cores: 4
  dynamic_allocation: true
  adaptive_enabled: true
  additional_configs:
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.adaptive.skewJoin.enabled: "true"
    spark.serializer: "org.apache.spark.serializer.KryoSerializer"
```

### Database Optimization
```sql
-- Create appropriate indexes
CREATE INDEX CONCURRENTLY idx_orders_date_customer 
ON orders(order_date, customer_id);

-- Partition large tables
CREATE TABLE orders_2024 PARTITION OF orders 
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### Memory Management
```python
# Configure memory limits
performance:
  max_memory_usage_percent: 80
  gc_threshold: 0.8
  batch_size: 50000  # Adjust based on available memory
```

## 🤝 Contributing

### Development Setup
```bash
# 1. Fork and clone
git clone https://github.com/yourusername/etl-framework.git
cd etl-framework

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install development dependencies
pip install -e ".[dev,testing,docs]"

# 4. Install pre-commit hooks
pre-commit install

# 5. Run tests
pytest
```

### Code Standards
- **Python**: Follow PEP 8, use Black for formatting
- **SQL**: Use uppercase keywords, consistent indentation
- **Documentation**: Docstrings for all functions, type hints
- **Testing**: Minimum 90% test coverage for new code

### Pull Request Process
1. Create feature branch: `git checkout -b feature/awesome-feature`
2. Make changes and add tests
3. Run full test suite: `make test-all`
4. Submit pull request with description

## 📚 Documentation

- **[API Reference](docs/api/)**: Complete API documentation
- **[Architecture Guide](docs/architecture/)**: System design and patterns
- **[User Guide](docs/user-guide/)**: Step-by-step tutorials
- **[Operations Guide](docs/operations/)**: Production management
- **[Troubleshooting](docs/troubleshooting/)**: Common issues and solutions

## 🆘 Support

### Getting Help
- **Documentation**: Check the [docs](docs/) directory
- **Issues**: Create an issue on GitHub
- **Discussions**: Use GitHub Discussions for questions
- **Slack**: Join our [Slack workspace](https://etl-framework.slack.com)

### Common Issues
- **Memory Errors**: Reduce batch size or increase memory allocation
- **Connection Timeouts**: Check network connectivity and increase timeouts
- **Data Quality Failures**: Review quality rules and thresholds
- **Performance Issues**: Enable Spark adaptive query execution

## 🎉 Acknowledgments

Built with love by the Data Engineering team using:
- **Apache Spark** for big data processing
- **Delta Lake** for reliable data storage
- **PostgreSQL** for metadata management
- **Prometheus + Grafana** for monitoring
- **Docker** for containerization

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Happy Data Engineering!** 🚀📊✨

For questions or support, reach out to the Data Engineering team at `data-engineering@company.com`
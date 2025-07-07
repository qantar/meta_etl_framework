etl-framework/
│
├── 📦 **Root Configuration & Deployment**
│   ├── Dockerfile                           # Multi-stage container (from L.dockerfile)
│   ├── docker-compose.yml                   # Complete stack deployment
│   ├── requirements.txt                     # Python dependencies (from N.txt)
│   ├── setup.py                            # Package installation
│   ├── .env.example                        # Environment template
│   ├── .gitignore                          # Git exclusions
│   └── README.md                           # Project documentation
│
├── 🧠 **Core Framework** (`src/`)
│   ├── __init__.py                         # Framework version & exports
│   │
│   ├── 🎯 **core/** - Central framework logic
│   │   ├── __init__.py
│   │   ├── unified_engine.py               # Cross-engine orchestrator (from E.py)
│   │   ├── control_audit_system.py         # Metadata & audit mgmt (from A.py)
│   │   ├── engine_selector.py              # **[NEW]** Intelligent engine selection
│   │   └── metadata_manager.py             # **[NEW]** Schema & lineage tracking
│   │
│   ├── ⚙️ **engines/** - Processing engines
│   │   ├── __init__.py
│   │   ├── base_engine.py                  # **[NEW]** Abstract engine interface
│   │   ├── python_implementation.py        # Pandas engine (from B.py)
│   │   ├── pyspark_implementation.py       # Spark engine (from D.py)
│   │   └── sql_implementation.py           # **[NEW]** Pure SQL engine
│   │
│   ├── 🔍 **quality/** - Data quality framework
│   │   ├── __init__.py
│   │   ├── data_quality_framework.py       # **[NEW]** Quality orchestrator
│   │   ├── quality_rules.py                # **[NEW]** Rule definitions
│   │   ├── validators.py                   # **[NEW]** Field validators
│   │   └── profiling.py                    # **[NEW]** Data profiling
│   │
│   ├── 📊 **sources/** - Data source adapters
│   │   ├── __init__.py
│   │   ├── base_source.py                  # **[NEW]** Abstract source interface
│   │   ├── postgresql_source.py            # **[NEW]** PostgreSQL adapter
│   │   ├── mysql_source.py                 # **[NEW]** MySQL adapter
│   │   ├── oracle_source.py                # **[NEW]** Oracle adapter
│   │   ├── file_source.py                  # **[NEW]** File-based sources
│   │   ├── api_source.py                   # **[NEW]** REST API integration
│   │   └── streaming_source.py             # **[NEW]** Kafka/streaming
│   │
│   ├── 🔄 **orchestration/** - Workflow management
│   │   ├── __init__.py
│   │   ├── scheduler.py                    # Job scheduler (from I.py)
│   │   ├── workflow_manager.py             # **[NEW]** DAG execution
│   │   ├── dependency_resolver.py          # **[NEW]** Job dependencies
│   │   └── retry_manager.py                # **[NEW]** Retry & circuit breaker
│   │
│   ├── 💾 **storage/** - Storage & backup
│   │   ├── __init__.py
│   │   ├── backup_manager.py               # **[NEW]** Backup orchestration
│   │   ├── s3_storage.py                   # **[NEW]** AWS S3 operations
│   │   ├── azure_storage.py                # **[NEW]** Azure Blob storage
│   │   └── local_storage.py                # **[NEW]** Local file system
│   │
│   ├── 🛠️ **utils/** - Cross-cutting utilities
│   │   ├── __init__.py
│   │   ├── config_manager.py               # Configuration mgmt (from H.py)
│   │   ├── notification_service.py         # Alerting service (from J.py)
│   │   ├── encryption_utils.py             # **[NEW]** Secret management
│   │   ├── logging_config.py               # **[NEW]** Centralized logging
│   │   └── performance_utils.py            # **[NEW]** Performance monitoring
│   │
│   ├── 📡 **monitoring/** - Observability
│   │   ├── __init__.py
│   │   ├── metrics_collector.py            # **[NEW]** Prometheus metrics
│   │   ├── health_check.py                 # **[NEW]** System health monitoring
│   │   └── performance_monitor.py          # **[NEW]** Performance tracking
│   │
│   ├── 🖥️ **cli/** - Command line interface
│   │   ├── __init__.py
│   │   ├── main.py                         # **[NEW]** Main CLI entry
│   │   ├── monitor_commands.py             # Monitoring commands (from F.py)
│   │   ├── pipeline_commands.py            # **[NEW]** Pipeline operations
│   │   ├── config_commands.py              # **[NEW]** Configuration management
│   │   └── discovery_commands.py           # **[NEW]** Table discovery
│   │
│   └── 🌐 **api/** - REST API server
│       ├── __init__.py
│       ├── rest_server.py                  # **[NEW]** FastAPI application
│       ├── pipeline_endpoints.py           # **[NEW]** Pipeline operations
│       ├── monitoring_endpoints.py         # **[NEW]** Health & metrics
│       └── auth.py                         # **[NEW]** Authentication
│
├── ⚙️ **Configuration** (`config/`)
│   ├── environments/
│   │   ├── development.yaml                # Development settings
│   │   ├── staging.yaml                    # **[NEW]** Staging environment
│   │   └── production.yaml                 # Production config (existing)
│   ├── quality_rules.yaml                  # **[NEW]** Data quality definitions
│   ├── engine_profiles.yaml                # **[NEW]** Engine selection rules
│   └── notification_templates.yaml         # **[NEW]** Alert templates
│
├── 🗄️ **Database Scripts** (`sql/`)
│   ├── metadata_ddl.sql                    # Control tables DDL (from C.sql)
│   ├── sample_source_data.sql              # **[NEW]** Test data generation
│   ├── target_init.sql                     # **[NEW]** DWH initialization
│   ├── maintenance_queries.sql             # **[NEW]** Cleanup procedures
│   └── migrations/                         # **[NEW]** Schema migration scripts
│       ├── 001_initial_schema.sql
│       ├── 002_add_quality_tables.sql
│       └── 003_add_lineage_tracking.sql
│
├── 📊 **Monitoring & Observability** (`monitoring/`)
│   ├── prometheus.yml                      # Metrics collection config
│   ├── grafana/                           # Dashboard definitions
│   │   ├── dashboards/
│   │   │   ├── pipeline_overview.json     # **[NEW]** Main pipeline dashboard
│   │   │   ├── data_quality.json          # **[NEW]** Quality metrics
│   │   │   └── system_health.json         # **[NEW]** Infrastructure health
│   │   └── datasources/
│   │       └── prometheus.yml             # Prometheus datasource config
│   ├── alerting/                          # **[NEW]** Alert rules
│   │   ├── pipeline_alerts.yml            # Pipeline failure alerts
│   │   ├── quality_alerts.yml             # Data quality alerts
│   │   └── system_alerts.yml              # Infrastructure alerts
│   └── filebeat.yml                       # Log shipping configuration
│
├── 🧪 **Testing** (`tests/`)
│   ├── __init__.py
│   ├── conftest.py                        # **[NEW]** Pytest configuration
│   ├── unit/                              # **[NEW]** Unit tests
│   │   ├── test_engines/
│   │   ├── test_quality/
│   │   ├── test_sources/
│   │   └── test_utils/
│   ├── integration/                       # **[NEW]** Integration tests
│   │   ├── test_end_to_end.py
│   │   ├── test_engine_integration.py
│   │   └── test_database_operations.py
│   ├── performance/                       # **[NEW]** Performance tests
│   │   ├── test_large_datasets.py
│   │   └── benchmark_engines.py
│   └── fixtures/                          # **[NEW]** Test data
│       ├── sample_data.csv
│       └── test_configs/
│
├── 📜 **Scripts** (`scripts/`)
│   ├── setup_environment.sh               # **[NEW]** Environment setup
│   ├── generate_test_data.py              # **[NEW]** Test data generation
│   ├── migrate_database.py                # **[NEW]** Database migrations
│   ├── backup_metadata.py                 # **[NEW]** Metadata backup
│   └── performance_benchmark.py           # **[NEW]** Performance testing
│
├── 📚 **Documentation** (`docs/`)
│   ├── README.md                          # Project overview
│   ├── api/                               # **[NEW]** API documentation
│   │   ├── rest_api.md
│   │   └── cli_reference.md
│   ├── architecture/                      # **[NEW]** System design
│   │   ├── overview.md
│   │   ├── engine_selection.md
│   │   └── data_flow.md
│   ├── user_guide/                        # **[NEW]** Usage instructions
│   │   ├── getting_started.md
│   │   ├── configuration.md
│   │   ├── data_quality.md
│   │   └── monitoring.md
│   └── examples/                          # **[NEW]** Usage examples
│       ├── basic_pipeline.py
│       ├── custom_quality_rules.py
│       └── advanced_scheduling.py
│
└── 🚀 **DevOps & CI/CD** (`.github/`)
    └── workflows/
        ├── ci.yml                         # Continuous integration
        ├── security.yml                   # **[NEW]** Security scanning
        └── deployment.yml                 # **[NEW]** Deployment pipeline

# 📋 **File Status Legend**
- **Moved**: File relocated from external codebase
- **New**: Generated production-ready implementation  
- **Existing**: Already in proper location
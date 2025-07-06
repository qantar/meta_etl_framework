#!/usr/bin/env python3
"""
Setup script for ETL Framework
Production-ready metadata-driven ETL pipeline with multi-engine support
"""

import os
import sys
from pathlib import Path
from setuptools import setup, find_packages

# Ensure we're using Python 3.8+
if sys.version_info < (3, 8):
    raise RuntimeError("ETL Framework requires Python 3.8 or higher")

# Get the long description from README
here = Path(__file__).parent.absolute()
long_description = (here / "README.md").read_text(encoding="utf-8") if (here / "README.md").exists() else ""

# Read version from __init__.py
def get_version():
    """Extract version from __init__.py"""
    version_file = here / "src" / "__init__.py"
    if version_file.exists():
        with open(version_file, 'r') as f:
            for line in f:
                if line.startswith('__version__'):
                    return line.split('=')[1].strip().strip('"').strip("'")
    return "1.0.0"

# Read requirements from requirements.txt
def get_requirements():
    """Read requirements from requirements.txt"""
    requirements_file = here / "requirements.txt"
    if requirements_file.exists():
        with open(requirements_file, 'r') as f:
            requirements = []
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and not line.startswith('-'):
                    # Handle version constraints
                    if '>=' in line and '<' in line:
                        requirements.append(line)
                    elif '>=' in line:
                        requirements.append(line)
                    else:
                        requirements.append(line)
            return requirements
    return []

# Optional dependencies for different features
extras_require = {
    # Development dependencies
    'dev': [
        'pytest>=7.4.0',
        'pytest-cov>=4.1.0',
        'pytest-asyncio>=0.21.0',
        'pytest-mock>=3.11.0',
        'black>=23.7.0',
        'flake8>=6.0.0',
        'mypy>=1.5.0',
        'isort>=5.12.0',
        'pre-commit>=3.3.0',
    ],
    
    # Documentation dependencies
    'docs': [
        'sphinx>=7.1.0',
        'sphinx-rtd-theme>=1.3.0',
        'mkdocs>=1.5.0',
        'mkdocs-material>=9.1.0',
    ],
    
    # Jupyter and interactive development
    'jupyter': [
        'jupyter>=1.0.0',
        'jupyterlab>=4.0.0',
        'ipywidgets>=8.0.0',
        'notebook>=7.0.0',
    ],
    
    # Cloud providers
    'aws': [
        'boto3>=1.28.0',
        's3fs>=2023.6.0',
    ],
    'azure': [
        'azure-storage-blob>=12.17.0',
        'azure-identity>=1.13.0',
        'adlfs>=2023.6.0',
    ],
    'gcp': [
        'google-cloud-storage>=2.10.0',
        'google-cloud-bigquery>=3.11.0',
        'gcsfs>=2023.6.0',
    ],
    
    # Additional databases
    'databases': [
        'pymongo>=4.4.0',
        'redis>=4.6.0',
        'cassandra-driver>=3.28.0',
        'mysql-connector-python>=8.1.0',
        'cx-Oracle>=8.3.0',
        'snowflake-connector-python>=3.1.0',
        'databricks-sql-connector>=2.7.0',
        'duckdb>=0.8.0',
        'clickhouse-driver>=0.2.0',
    ],
    
    # Machine Learning
    'ml': [
        'scikit-learn>=1.3.0',
        'mlflow>=2.5.0',
        'wandb>=0.15.0',
        'tensorboard>=2.13.0',
    ],
    
    # Stream processing
    'streaming': [
        'kafka-python>=2.0.0',
        'confluent-kafka>=2.2.0',
        'apache-beam[gcp]>=2.49.0',
    ],
    
    # Orchestration
    'orchestration': [
        'apache-airflow>=2.6.0',
        'prefect>=2.11.0',
        'kubernetes>=27.2.0',
    ],
    
    # Performance and distributed computing
    'performance': [
        'dask[complete]>=2023.7.0',
        'ray[default]>=2.6.0',
        'numba>=0.57.0',
        'modin[all]>=0.22.0',
    ],
    
    # Geospatial data processing
    'geo': [
        'geopandas>=0.13.0',
        'shapely>=2.0.0',
        'folium>=0.14.0',
    ],
    
    # Natural Language Processing
    'nlp': [
        'nltk>=3.8.0',
        'spacy>=3.6.0',
        'textblob>=0.17.0',
    ],
    
    # Financial data
    'finance': [
        'pandas-datareader>=0.10.0',
        'yfinance>=0.2.0',
        'ta-lib>=0.4.0',
    ],
    
    # All optional dependencies
    'all': [
        # This will be populated programmatically
    ]
}

# Populate 'all' with all optional dependencies
all_extras = set()
for extra_deps in extras_require.values():
    if extra_deps != extras_require['all']:  # Avoid circular reference
        all_extras.update(extra_deps)
extras_require['all'] = list(all_extras)

# Platform-specific dependencies
if sys.platform.startswith('win'):
    # Windows-specific dependencies
    extras_require['windows'] = [
        'pywin32>=306',
        'wmi>=1.5.1',
    ]
elif sys.platform.startswith('darwin'):
    # macOS-specific dependencies
    extras_require['macos'] = [
        'pyobjc-core>=9.2',
    ]

setup(
    # Basic package information
    name="etl-framework",
    version=get_version(),
    description="Production-ready metadata-driven ETL framework with multi-engine support",
    long_description=long_description,
    long_description_content_type="text/markdown",
    
    # Author and contact information
    author="ETL Framework Team",
    author_email="etl-framework@company.com",
    url="https://github.com/company/etl-framework",
    
    # License and classifiers
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],
    
    # Package discovery and content
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    package_data={
        "etl_framework": [
            "config/*.yaml",
            "config/*.json",
            "templates/*.j2",
            "sql/*.sql",
            "monitoring/*.yml",
        ],
    },
    
    # Dependencies
    python_requires=">=3.8",
    install_requires=get_requirements(),
    extras_require=extras_require,
    
    # Entry points for CLI
    entry_points={
        "console_scripts": [
            "etl-cli=cli:cli",
            "etl-framework=cli:cli",
            "etl-server=src.api.server:main",
            "etl-scheduler=src.scheduler_orchestrator:main",
            "etl-worker=src.worker:main",
        ],
        "etl_framework.engines": [
            "python=src.python_implementation:PythonETLOrchestrator",
            "pyspark=src.pyspark_implementation:SparkETLOrchestrator",
            "sql=src.sql_implementation:SQLETLOrchestrator",
        ],
        "etl_framework.sources": [
            "postgresql=src.sources.postgresql_source:PostgreSQLSource",
            "mysql=src.sources.mysql_source:MySQLSource",
            "oracle=src.sources.oracle_source:OracleSource",
            "mongodb=src.sources.mongodb_source:MongoDBSource",
            "file=src.sources.file_source:FileSource",
            "api=src.sources.api_source:APISource",
            "kafka=src.sources.streaming_source:KafkaSource",
        ],
        "etl_framework.targets": [
            "postgresql=src.targets.postgresql_target:PostgreSQLTarget",
            "delta=src.targets.delta_target:DeltaTarget",
            "parquet=src.targets.parquet_target:ParquetTarget",
            "s3=src.targets.s3_target:S3Target",
        ],
        "etl_framework.quality_checks": [
            "completeness=src.data_quality_framework:CompletenessCheck",
            "uniqueness=src.data_quality_framework:UniquenessCheck",
            "validity=src.data_quality_framework:ValidityCheck",
            "business_rule=src.data_quality_framework:BusinessRuleCheck",
            "range=src.data_quality_framework:RangeCheck",
        ],
    },
    
    # Project URLs
    project_urls={
        "Documentation": "https://etl-framework.readthedocs.io/",
        "Bug Reports": "https://github.com/company/etl-framework/issues",
        "Source": "https://github.com/company/etl-framework",
        "Changelog": "https://github.com/company/etl-framework/blob/main/CHANGELOG.md",
    },
    
    # Keywords for PyPI search
    keywords=[
        "etl", "data-pipeline", "data-engineering", "spark", "delta-lake",
        "data-quality", "metadata-driven", "orchestration", "scheduling",
        "data-warehouse", "big-data", "apache-spark", "postgresql", "python"
    ],
    
    # Zip safety
    zip_safe=False,
    
    # Additional metadata
    platforms=["any"],
    
    # Test suite
    test_suite="tests",
    tests_require=extras_require['dev'],
    
    # Options for bdist_wheel
    options={
        "bdist_wheel": {
            "universal": False,  # Not universal since we require Python 3.8+
        },
    },
)

# Post-installation message
def post_install_message():
    """Display post-installation instructions"""
    print("""
🚀 ETL Framework installed successfully!

Next steps:
1. Initialize configuration:
   etl-cli config init

2. Set up your environment:
   export ETL_ENVIRONMENT=development

3. Configure your databases and run discovery:
   etl-cli discover tables

4. Start your first pipeline:
   etl-cli pipeline run

📖 Documentation: https://etl-framework.readthedocs.io/
🐛 Issues: https://github.com/company/etl-framework/issues
💬 Community: https://discord.gg/etl-framework

Happy data engineering! 🎉
    """)

# Custom commands
class PostInstallCommand:
    """Custom command to run after installation"""
    
    def run(self):
        post_install_message()

# Only show message on direct installation, not on dependency resolution
if __name__ == "__main__" and "install" in sys.argv:
    import atexit
    atexit.register(post_install_message)
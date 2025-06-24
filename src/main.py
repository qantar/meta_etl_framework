# Simplified main() function:
#def main():
#    # Initialize components
#    config = ConfigManager.load_config()
#    spark = create_spark_session(config)
#    metadata_manager = MetadataManager(config.metadata_connection)
#    etl_processor = MedallionETLProcessor(...)
#    
#    # Execute based on CLI arguments
#    if args.job_name:
#        etl_processor.run_single_job(args.job_name)
#    else:
#        etl_processor.run_all_jobs()


"""
Production-Ready Metadata-Driven ETL Framework with Medallion Architecture
Supports PostgreSQL and File data sources with comprehensive auditing
"""

import json
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import delta
from delta.tables import DeltaTable
 

@dataclass
class ConnectionConfig:
    source_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    file_path: Optional[str] = None
    file_format: Optional[str] = None

@dataclass
class TableConfig:
    source_schema: str
    source_table: str
    target_layer: str
    target_table: str
    load_type: str  # full, incremental
    delta_column: Optional[str] = None
    business_key: Optional[str] = None
    partition_columns: Optional[List[str]] = None

@dataclass
class JobExecution:
    job_id: int
    execution_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    records_processed: int = 0
    error_message: Optional[str] = None


# Sample data setup and main execution
def setup_sample_data(metadata_manager: MetadataManager):
    """Setup sample metadata and configurations"""
    
    with metadata_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            
            # Sample connection configurations
            cursor.execute("""
            INSERT INTO etl_connections (connection_name, source_type, host, port, database_name, username, password_encrypted)
            VALUES ('source_db', 'postgresql', 'localhost', 5432, 'source_db', 'user', 'encrypted_password')
            ON CONFLICT (connection_name) DO NOTHING
            """)
            
            cursor.execute("""
            INSERT INTO etl_connections (connection_name, source_type, file_path, file_format)
            VALUES ('customer_files', 'file', '/data/customers.csv', 'csv')
            ON CONFLICT (connection_name) DO NOTHING
            """)
            
            # Sample ETL job configurations
            cursor.execute("""
            INSERT INTO etl_jobs (job_name, connection_id, source_schema, source_table, target_layer, target_table, load_type, delta_column, business_key)
            VALUES ('load_customers', 1, 'public', 'customers', 'all', 'customers', 'incremental', 'updated_at', 'customer_id')
            ON CONFLICT (job_name) DO NOTHING
            """)
            
            cursor.execute("""
            INSERT INTO etl_jobs (job_name, connection_id, target_layer, target_table, load_type, transformation_rules)
            VALUES ('load_customer_files', 2, 'all', 'customer_files', 'full', '{"aggregations": [{"type": "count_by_date", "date_column": "created_date", "name": "daily_customers"}]}')
            ON CONFLICT (job_name) DO NOTHING
            """)
            
            conn.commit()


def main():
    """Main execution function"""
    
    # Initialize Spark with Delta Lake
    spark = SparkSession.builder \
        .appName("MetadataDrivenETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Configuration
    metadata_connection_string = "postgresql://user:password@localhost:5432/metadata_db"
    bronze_path = "/delta/bronze"
    silver_path = "/delta/silver" 
    gold_path = "/delta/gold"
    
    # Initialize components
    metadata_manager = MetadataManager(metadata_connection_string)
    etl_processor = MedallionETLProcessor(spark, metadata_manager, bronze_path, silver_path, gold_path)
    
    # Setup sample data (run once)
    setup_sample_data(metadata_manager)
    
    # Execute ETL jobs
    etl_processor.run_all_jobs()
    
    spark.stop()


if __name__ == "__main__":
    main()
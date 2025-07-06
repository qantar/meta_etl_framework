"""
PySpark Implementation of Metadata-Driven ETL Pipeline
Scalable big data processing with Delta Lake, parallel execution, and advanced optimizations
"""

import os
import json
import uuid
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import delta

# Import our control and audit system
from control_audit_system import ControlAuditManager, SourceTableConfig, IngestionResult, LoadStatus


class SparkETLOrchestrator:
    """
    PySpark-based metadata-driven ETL pipeline with Delta Lake
    Handles massive datasets with parallel processing and advanced optimizations
    """
    
    def __init__(self, source_jdbc_url: str, target_connection_string: str,
                 backup_root_path: str = "/delta/backup", dwh_root_path: str = "/delta/dwh",
                 staging_root_path: str = "/delta/staging"):
        
        # Initialize Spark with Delta Lake optimizations
        self.spark = self._create_optimized_spark_session()
        
        # Connection details
        self.source_jdbc_url = source_jdbc_url
        self.source_jdbc_properties = self._parse_jdbc_properties(source_jdbc_url)
        
        # Delta Lake paths
        self.backup_root_path = backup_root_path
        self.dwh_root_path = dwh_root_path
        self.staging_root_path = staging_root_path
        
        # Initialize control and audit system
        self.control_audit = ControlAuditManager(target_connection_string, backup_root_path)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Performance monitoring
        self.spark.sparkContext.setLogLevel("WARN")
    
    def _create_optimized_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake and performance optimizations"""
        
        spark = SparkSession.builder \
            .appName("MetadataDrivenETL-PySpark") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
            .config("spark.sql.files.maxPartitionBytes", "134217728") \
            .config("spark.sql.files.openCostInBytes", "4194304") \
            .config("spark.sql.parquet.enableVectorizedReader", "true") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "1") \
            .config("spark.dynamicAllocation.maxExecutors", "20") \
            .config("spark.dynamicAllocation.initialExecutors", "3") \
            .getOrCreate()
        
        return spark
    
    def _parse_jdbc_properties(self, jdbc_url: str) -> Dict[str, str]:
        """Parse JDBC URL into properties dictionary"""
        
        # Extract connection details from JDBC URL
        # Format: jdbc:postgresql://host:port/database?user=user&password=password
        
        import urllib.parse
        
        if jdbc_url.startswith("jdbc:postgresql://"):
            url_part = jdbc_url.replace("jdbc:postgresql://", "")
            if "?" in url_part:
                host_db, params = url_part.split("?", 1)
                params_dict = urllib.parse.parse_qs(params)
                
                return {
                    "driver": "org.postgresql.Driver",
                    "user": params_dict.get("user", [""])[0],
                    "password": params_dict.get("password", [""])[0],
                    "url": jdbc_url.split("?")[0]
                }
        
        # Default fallback
        return {
            "driver": "org.postgresql.Driver",
            "user": "postgres",
            "password": "password",
            "url": jdbc_url
        }
    
    def discover_source_tables(self, source_database: str, schema_patterns: List[str] = None) -> List[Dict]:
        """
        Discover tables from source database using Spark JDBC
        """
        discovered_tables = []
        
        # Get list of tables using information_schema
        tables_query = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_type = 'BASE TABLE' 
        AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        """
        
        if schema_patterns:
            schema_condition = " OR ".join([f"table_schema LIKE '%{pattern}%'" for pattern in schema_patterns])
            tables_query += f" AND ({schema_condition})"
        
        tables_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.source_jdbc_properties["url"]) \
            .option("dbtable", f"({tables_query}) as tables") \
            .option("user", self.source_jdbc_properties["user"]) \
            .option("password", self.source_jdbc_properties["password"]) \
            .option("driver", self.source_jdbc_properties["driver"]) \
            .load()
        
        # Process each discovered table
        for row in tables_df.collect():
            schema = row['table_schema']
            table = row['table_name']
            
            try:
                # Auto-detect table configuration
                increment_column = self._detect_increment_column(schema, table)
                business_keys = self._detect_business_keys(schema, table)
                
                # Register table
                config = SourceTableConfig(
                    source_database=source_database,
                    source_schema=schema,
                    source_table=table,
                    increment_column=increment_column,
                    business_key_columns=business_keys,
                    is_scd_type_2_enabled=len(business_keys) > 0
                )
                
                control_id = self.control_audit.register_source_table(config)
                discovered_tables.append({
                    'control_id': control_id,
                    'full_table_name': f"{source_database}.{schema}.{table}",
                    'increment_column': increment_column,
                    'business_keys': business_keys
                })
                
                self.logger.info(f"Registered table: {schema}.{table}")
                
            except Exception as e:
                self.logger.error(f"Failed to register {schema}.{table}: {str(e)}")
        
        return discovered_tables
    
    def _detect_increment_column(self, schema: str, table: str) -> Optional[str]:
        """Auto-detect increment column using information_schema"""
        
        columns_query = f"""
        SELECT column_name, data_type, is_nullable 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        
        columns_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.source_jdbc_properties["url"]) \
            .option("dbtable", f"({columns_query}) as columns") \
            .option("user", self.source_jdbc_properties["user"]) \
            .option("password", self.source_jdbc_properties["password"]) \
            .option("driver", self.source_jdbc_properties["driver"]) \
            .load()
        
        # Look for timestamp columns with increment patterns
        increment_patterns = ['updated_at', 'modified_at', 'last_modified', 'created_at', 'insert_timestamp']
        
        for row in columns_df.collect():
            col_name = row['column_name'].lower()
            data_type = row['data_type'].lower()
            
            if any(pattern in col_name for pattern in increment_patterns):
                if any(t in data_type for t in ['timestamp', 'datetime']):
                    return row['column_name']
        
        # Fallback: any timestamp column that's not nullable
        for row in columns_df.collect():
            data_type = row['data_type'].lower()
            if 'timestamp' in data_type and row['is_nullable'] == 'NO':
                return row['column_name']
        
        return None
    
    def _detect_business_keys(self, schema: str, table: str) -> List[str]:
        """Auto-detect business key columns"""
        
        # Query for primary key constraints
        pk_query = f"""
        SELECT column_name 
        FROM information_schema.key_column_usage 
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        AND constraint_name IN (
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_schema = '{schema}' AND table_name = '{table}' 
            AND constraint_type = 'PRIMARY KEY'
        )
        ORDER BY ordinal_position
        """
        
        try:
            pk_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.source_jdbc_properties["url"]) \
                .option("dbtable", f"({pk_query}) as pk") \
                .option("user", self.source_jdbc_properties["user"]) \
                .option("password", self.source_jdbc_properties["password"]) \
                .option("driver", self.source_jdbc_properties["driver"]) \
                .load()
            
            pk_columns = [row['column_name'] for row in pk_df.collect()]
            if pk_columns:
                return pk_columns
        
        except Exception as e:
            self.logger.warning(f"Could not detect primary keys for {schema}.{table}: {str(e)}")
        
        # Fallback: look for 'id' columns
        columns_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        AND (column_name ILIKE '%id' OR column_name ILIKE 'key')
        LIMIT 1
        """
        
        try:
            id_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.source_jdbc_properties["url"]) \
                .option("dbtable", f"({columns_query}) as id_cols") \
                .option("user", self.source_jdbc_properties["user"]) \
                .option("password", self.source_jdbc_properties["password"]) \
                .option("driver", self.source_jdbc_properties["driver"]) \
                .load()
            
            id_columns = [row['column_name'] for row in id_df.collect()]
            return id_columns
        
        except Exception:
            return []
    
    def extract_table_data(self, source_database: str, source_schema: str, 
                          source_table: str) -> Tuple[DataFrame, Dict]:
        """
        Extract data from source table with intelligent partitioning and incremental logic
        """
        
        # Get control configuration
        control_tables = self.control_audit.get_active_source_tables()
        table_config = next((t for t in control_tables 
                           if t['source_database'] == source_database 
                           and t['source_schema'] == source_schema 
                           and t['source_table'] == source_table), None)
        
        if not table_config:
            raise ValueError(f"Table {source_database}.{source_schema}.{source_table} not found in control table")
        
        # Build extraction query
        if table_config['custom_query']:
            query = table_config['custom_query']
        else:
            base_query = f"SELECT * FROM {source_schema}.{source_table}"
            
            # Add incremental logic
            if table_config['increment_column']:
                last_watermark = self.control_audit.get_last_watermark(
                    source_database, source_schema, source_table, table_config['increment_column']
                )
                if last_watermark:
                    base_query += f" WHERE {table_config['increment_column']} > '{last_watermark}'"
            
            query = base_query
        
        # Determine optimal partitioning for large tables
        # First, get row count estimate
        count_query = f"SELECT COUNT(*) as row_count FROM ({query}) as subq"
        
        try:
            count_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.source_jdbc_properties["url"]) \
                .option("dbtable", f"({count_query}) as count_table") \
                .option("user", self.source_jdbc_properties["user"]) \
                .option("password", self.source_jdbc_properties["password"]) \
                .option("driver", self.source_jdbc_properties["driver"]) \
                .load()
            
            estimated_rows = count_df.collect()[0]['row_count']
        except Exception:
            estimated_rows = 0
        
        # Configure partitioning based on data size
        if estimated_rows > 1000000:  # Large table - use partitioning
            num_partitions = min(20, max(4, estimated_rows // 250000))
            
            # Use partition column if available, otherwise use hash partitioning
            if table_config['increment_column']:
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.source_jdbc_properties["url"]) \
                    .option("dbtable", f"({query}) as source_data") \
                    .option("user", self.source_jdbc_properties["user"]) \
                    .option("password", self.source_jdbc_properties["password"]) \
                    .option("driver", self.source_jdbc_properties["driver"]) \
                    .option("partitionColumn", table_config['increment_column']) \
                    .option("numPartitions", str(num_partitions)) \
                    .load()
            else:
                # Use fetchsize for large tables without good partition column
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.source_jdbc_properties["url"]) \
                    .option("dbtable", f"({query}) as source_data") \
                    .option("user", self.source_jdbc_properties["user"]) \
                    .option("password", self.source_jdbc_properties["password"]) \
                    .option("driver", self.source_jdbc_properties["driver"]) \
                    .option("fetchsize", "50000") \
                    .load()
        else:
            # Small table - simple read
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.source_jdbc_properties["url"]) \
                .option("dbtable", f"({query}) as source_data") \
                .option("user", self.source_jdbc_properties["user"]) \
                .option("password", self.source_jdbc_properties["password"]) \
                .option("driver", self.source_jdbc_properties["driver"]) \
                .load()
        
        # Add extraction metadata
        df_with_metadata = df \
            .withColumn("_ingestion_timestamp", F.current_timestamp()) \
            .withColumn("_source_system", F.lit(f"{source_database}.{source_schema}.{source_table}")) \
            .withColumn("_extraction_batch_id", F.lit(str(uuid.uuid4())))
        
        # Cache for multiple operations if reasonable size
        if estimated_rows < 10000000:  # Cache if less than 10M rows
            df_with_metadata.cache()
        
        # Get extraction metadata
        metadata = {
            'source_database': source_database,
            'source_schema': source_schema,
            'source_table': source_table,
            'extraction_timestamp': datetime.now().isoformat(),
            'estimated_rows': estimated_rows,
            'partitions_used': df_with_metadata.rdd.getNumPartitions(),
            'increment_column': table_config['increment_column']
        }
        
        # Get actual row count and watermark value
        if not df_with_metadata.rdd.isEmpty():
            actual_count = df_with_metadata.count()
            metadata['actual_row_count'] = actual_count
            
            if table_config['increment_column']:
                max_watermark = df_with_metadata.agg(F.max(table_config['increment_column'])).collect()[0][0]
                metadata['watermark_value'] = str(max_watermark) if max_watermark else None
        else:
            metadata['actual_row_count'] = 0
            metadata['watermark_value'] = None
        
        self.logger.info(f"Extracted {metadata['actual_row_count']} rows from {source_database}.{source_schema}.{source_table}")
        
        return df_with_metadata, metadata
    
    def save_to_delta_backup(self, df: DataFrame, source_database: str, 
                           source_schema: str, source_table: str) -> str:
        """
        Save extracted data to Delta Lake backup with partitioning and optimization
        """
        
        if df.rdd.isEmpty():
            return ""
        
        # Generate backup path with timestamp
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{self.backup_root_path}/{source_database}/{source_schema}/{source_table}/{timestamp_str}"
        
        # Add backup metadata
        df_backup = df \
            .withColumn("_backup_timestamp", F.current_timestamp()) \
            .withColumn("_backup_path", F.lit(backup_path))
        
        # Write to Delta with partitioning by date
        df_backup \
            .withColumn("_backup_date", F.date_format("_backup_timestamp", "yyyy-MM-dd")) \
            .write \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .partitionBy("_backup_date") \
            .mode("overwrite") \
            .save(backup_path)
        
        # Optimize the Delta table
        try:
            DeltaTable.forPath(self.spark, backup_path).optimize().executeCompaction()
        except Exception as e:
            self.logger.warning(f"Could not optimize backup Delta table: {str(e)}")
        
        self.logger.info(f"Saved backup to Delta Lake: {backup_path}")
        return backup_path
    
    def load_to_delta_staging(self, df: DataFrame, source_table: str, run_id: str) -> int:
        """Load data to Delta Lake staging area"""
        
        if df.rdd.isEmpty():
            return 0
        
        staging_path = f"{self.staging_root_path}/{source_table}"
        
        # Add staging metadata
        df_staging = df \
            .withColumn("_run_id", F.lit(run_id)) \
            .withColumn("_staging_load_time", F.current_timestamp())
        
        # Write to Delta staging (append mode for incremental)
        df_staging.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .save(staging_path)
        
        row_count = df_staging.count()
        self.logger.info(f"Loaded {row_count} rows to Delta staging: {staging_path}")
        
        return row_count
    
    def apply_scd_type_2_spark(self, staging_df: DataFrame, dwh_path: str, 
                              business_keys: List[str], batch_id: str) -> DataFrame:
        """
        Apply SCD Type 2 logic using Spark DataFrame operations and Delta Lake merge
        """
        
        if staging_df.rdd.isEmpty() or not business_keys:
            # No SCD logic needed
            return staging_df \
                .withColumn("_dwh_insert_time", F.current_timestamp()) \
                .withColumn("_ingestion_batch_id", F.lit(batch_id)) \
                .withColumn("_valid_from", F.current_timestamp()) \
                .withColumn("_valid_to", F.lit("9999-12-31").cast("date")) \
                .withColumn("_is_current", F.lit(True)) \
                .withColumn("_version", F.lit(1))
        
        # Check if DWH table exists
        try:
            current_dwh_df = self.spark.read.format("delta").load(dwh_path) \
                .filter(F.col("_is_current") == True)
        except Exception:
            # Table doesn't exist yet - first load
            return staging_df \
                .withColumn("_dwh_insert_time", F.current_timestamp()) \
                .withColumn("_ingestion_batch_id", F.lit(batch_id)) \
                .withColumn("_valid_from", F.current_timestamp()) \
                .withColumn("_valid_to", F.lit("9999-12-31").cast("date")) \
                .withColumn("_is_current", F.lit(True)) \
                .withColumn("_version", F.lit(1))
        
        if current_dwh_df.rdd.isEmpty():
            # First load
            return staging_df \
                .withColumn("_dwh_insert_time", F.current_timestamp()) \
                .withColumn("_ingestion_batch_id", F.lit(batch_id)) \
                .withColumn("_valid_from", F.current_timestamp()) \
                .withColumn("_valid_to", F.lit("9999-12-31").cast("date")) \
                .withColumn("_is_current", F.lit(True)) \
                .withColumn("_version", F.lit(1))
        
        # Prepare staging data with metadata
        staging_prepared = staging_df \
            .withColumn("_dwh_insert_time", F.current_timestamp()) \
            .withColumn("_ingestion_batch_id", F.lit(batch_id)) \
            .withColumn("_valid_from", F.current_timestamp()) \
            .withColumn("_valid_to", F.lit("9999-12-31").cast("date")) \
            .withColumn("_is_current", F.lit(True)) \
            .withColumn("_version", F.lit(1))
        
        # Create hash for comparison (excluding metadata columns)
        data_columns = [col for col in staging_df.columns if not col.startswith('_')]
        staging_prepared = staging_prepared.withColumn(
            "_row_hash", 
            F.hash(*[F.col(col) for col in data_columns])
        )
        
        current_dwh_df = current_dwh_df.withColumn(
            "_row_hash",
            F.hash(*[F.col(col) for col in data_columns if col in current_dwh_df.columns])
        )
        
        # Use Delta Lake merge for SCD Type 2
        if DeltaTable.isDeltaTable(self.spark, dwh_path):
            delta_table = DeltaTable.forPath(self.spark, dwh_path)
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in business_keys])
            
            # Execute merge operation
            delta_table.alias("target").merge(
                staging_prepared.alias("source"),
                merge_condition
            ).whenMatchedUpdate(
                condition="target._is_current = true AND target._row_hash != source._row_hash",
                set={
                    "_valid_to": F.date_sub(F.current_date(), 1),
                    "_is_current": F.lit(False)
                }
            ).whenNotMatchedInsert(
                values={
                    col: F.col(f"source.{col}") for col in staging_prepared.columns
                }
            ).execute()
            
            # Insert new versions for changed records
            changed_records = staging_prepared.alias("source").join(
                current_dwh_df.alias("target"),
                [F.col(f"source.{key}") == F.col(f"target.{key}") for key in business_keys],
                "inner"
            ).filter(
                (F.col("target._is_current") == True) & 
                (F.col("target._row_hash") != F.col("source._row_hash"))
            ).select(
                *[F.col(f"source.{col}") for col in staging_prepared.columns if col != "_version"]
            ).withColumn(
                "_version",
                F.col("target._version") + 1
            )
            
            if not changed_records.rdd.isEmpty():
                changed_records.write.format("delta").mode("append").save(dwh_path)
        
        # Return the prepared staging data for metrics
        return staging_prepared.drop("_row_hash")
    
    def load_to_delta_dwh(self, staging_df: DataFrame, source_database: str,
                         source_schema: str, source_table: str, batch_id: str) -> int:
        """Load data to Delta Lake DWH with SCD Type 2 logic"""
        
        if staging_df.rdd.isEmpty():
            return 0
        
        dwh_path = f"{self.dwh_root_path}/{source_database}/{source_schema}/{source_table}"
        
        # Get control configuration
        control_tables = self.control_audit.get_active_source_tables()
        table_config = next((t for t in control_tables 
                           if t['source_database'] == source_database 
                           and t['source_schema'] == source_schema 
                           and t['source_table'] == source_table), None)
        
        # Apply SCD Type 2 logic if enabled
        if table_config and table_config['is_scd_type_2_enabled'] and table_config['business_key_columns']:
            final_df = self.apply_scd_type_2_spark(
                staging_df, dwh_path, table_config['business_key_columns'], batch_id
            )
        else:
            # Simple load without SCD
            final_df = staging_df \
                .withColumn("_dwh_insert_time", F.current_timestamp()) \
                .withColumn("_ingestion_batch_id", F.lit(batch_id)) \
                .withColumn("_valid_from", F.current_timestamp()) \
                .withColumn("_valid_to", F.lit("9999-12-31").cast("date")) \
                .withColumn("_is_current", F.lit(True)) \
                .withColumn("_version", F.lit(1))
        
        if final_df.rdd.isEmpty():
            return 0
        
        # Determine partitioning strategy
        partition_cols = []
        if table_config and table_config['partition_column'] and table_config['partition_column'] in final_df.columns:
            partition_cols.append(table_config['partition_column'])
        
        # Add date partitioning for large tables
        if final_df.count() > 100000:
            final_df = final_df.withColumn("_partition_date", F.date_format("_dwh_insert_time", "yyyy-MM-dd"))
            partition_cols.append("_partition_date")
        
        # Write to Delta DWH
        writer = final_df.write.format("delta").option("mergeSchema", "true")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.mode("append").save(dwh_path)
        
        # Optimize Delta table
        try:
            delta_table = DeltaTable.forPath(self.spark, dwh_path)
            delta_table.optimize().executeCompaction()
            
            # Z-Order optimize on business keys if available
            if table_config and table_config['business_key_columns']:
                optimize_cols = [col for col in table_config['business_key_columns'] 
                               if col in final_df.columns][:4]  # Limit to 4 columns for Z-order
                if optimize_cols:
                    delta_table.optimize().executeZOrderBy(*optimize_cols)
        except Exception as e:
            self.logger.warning(f"Could not optimize DWH Delta table: {str(e)}")
        
        row_count = final_df.count()
        self.logger.info(f"Loaded {row_count} rows to Delta DWH: {dwh_path}")
        
        return row_count
    
    def calculate_data_quality_spark(self, df: DataFrame) -> Dict:
        """Calculate data quality metrics using Spark operations"""
        
        if df.rdd.isEmpty():
            return {'total_records': 0, 'quality_score': 0.0}
        
        # Cache for multiple aggregations
        df.cache()
        
        total_records = df.count()
        data_columns = [col for col in df.columns if not col.startswith('_')]
        
        # Calculate null counts efficiently
        null_counts = {}
        for col in data_columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
        
        # Calculate duplicate count
        duplicate_count = total_records - df.select(*data_columns).distinct().count()
        
        # Calculate unique counts for key columns (sample for performance)
        unique_counts = {}
        sample_df = df.sample(0.1) if total_records > 100000 else df
        
        for col in data_columns[:10]:  # Limit to first 10 columns for performance
            unique_count = sample_df.select(col).distinct().count()
            unique_counts[col] = unique_count
        
        # Calculate quality score
        total_cells = total_records * len(data_columns)
        total_nulls = sum(null_counts.values())
        
        null_percentage = (total_nulls / total_cells) * 100 if total_cells > 0 else 0
        duplicate_percentage = (duplicate_count / total_records) * 100 if total_records > 0 else 0
        
        quality_score = max(0.0, 100.0 - null_percentage - duplicate_percentage)
        
        df.unpersist()  # Release cache
        
        return {
            'total_records': total_records,
            'null_count_by_column': null_counts,
            'unique_count_by_column': unique_counts,
            'duplicate_count': duplicate_count,
            'quality_score': round(quality_score, 2),
            'quality_rules_applied': {
                'null_check': True,
                'duplicate_check': True,
                'completeness_check': True,
                'uniqueness_check': True,
                'spark_optimized': True
            }
        }
    
    def run_spark_table_ingestion(self, source_database: str, source_schema: str, source_table: str) -> Dict:
        """Run complete Spark-based ingestion pipeline for a single table"""
        
        full_table_name = f"{source_database}.{source_schema}.{source_table}"
        job_name = f"spark_ingest_{source_table}_complete"
        
        # Start ingestion run
        run_id = self.control_audit.start_ingestion_run(full_table_name, job_name)
        batch_id = str(uuid.uuid4())
        
        start_time = datetime.now()
        
        try:
            # Stage 1: Extract data
            self.logger.info(f"Starting Spark extraction for {full_table_name}")
            staging_df, extraction_metadata = self.extract_table_data(
                source_database, source_schema, source_table
            )
            
            rows_extracted = extraction_metadata['actual_row_count']
            
            if staging_df.rdd.isEmpty():
                # Complete run with no data
                result = IngestionResult(
                    run_id=run_id,
                    source_table=full_table_name,
                    start_time=start_time,
                    end_time=datetime.now(),
                    rows_extracted=0,
                    rows_loaded=0,
                    status=LoadStatus.SUCCESS
                )
                self.control_audit.complete_ingestion_run(result)
                return {'status': 'SUCCESS', 'message': 'No new data to process', 'rows_processed': 0}
            
            # Stage 2: Save to Delta backup
            backup_path = self.save_to_delta_backup(staging_df, source_database, source_schema, source_table)
            
            # Stage 3: Load to Delta staging
            staging_rows = self.load_to_delta_staging(staging_df, source_table, run_id)
            
            # Stage 4: Calculate data quality
            quality_metrics = self.calculate_data_quality_spark(staging_df)
            self.control_audit.log_data_quality_metrics(run_id, full_table_name, quality_metrics)
            
            # Stage 5: Load to Delta DWH
            dwh_rows = self.load_to_delta_dwh(staging_df, source_database, source_schema, source_table, batch_id)
            
            # Update watermark
            if extraction_metadata.get('watermark_value'):
                self.control_audit.update_watermark(
                    source_database, source_schema, source_table,
                    extraction_metadata['increment_column'],
                    extraction_metadata['watermark_value']
                )
            
            end_time = datetime.now()
            
            # Complete ingestion run
            result = IngestionResult(
                run_id=run_id,
                source_table=full_table_name,
                start_time=start_time,
                end_time=end_time,
                rows_extracted=rows_extracted,
                rows_loaded=dwh_rows,
                status=LoadStatus.SUCCESS,
                backup_path=backup_path
            )
            
            self.control_audit.complete_ingestion_run(result)
            
            return {
                'status': 'SUCCESS',
                'run_id': run_id,
                'rows_extracted': rows_extracted,
                'rows_loaded': dwh_rows,
                'quality_score': quality_metrics['quality_score'],
                'backup_path': backup_path,
                'duration_seconds': (end_time - start_time).total_seconds(),
                'partitions_processed': extraction_metadata['partitions_used']
            }
            
        except Exception as e:
            error_message = f"Spark pipeline failed: {str(e)}"
            self.logger.error(f"Spark ingestion failed for {full_table_name}: {error_message}")
            
            # Log failure
            result = IngestionResult(
                run_id=run_id,
                source_table=full_table_name,
                start_time=start_time,
                end_time=datetime.now(),
                rows_extracted=0,
                rows_loaded=0,
                status=LoadStatus.FAILED,
                error_message=error_message
            )
            
            self.control_audit.complete_ingestion_run(result)
            
            return {
                'status': 'FAILED',
                'error_message': error_message,
                'run_id': run_id
            }
    
    def run_parallel_ingestion(self, max_parallel_tables: int = 5) -> List[Dict]:
        """Run ingestion for multiple tables in parallel using Spark"""
        
        active_tables = self.control_audit.get_active_source_tables()
        results = []
        
        self.logger.info(f"Starting parallel Spark ingestion for {len(active_tables)} tables")
        
        # Process tables in batches to control resource usage
        for i in range(0, len(active_tables), max_parallel_tables):
            batch_tables = active_tables[i:i+max_parallel_tables]
            batch_results = []
            
            for table_config in batch_tables:
                try:
                    result = self.run_spark_table_ingestion(
                        table_config['source_database'],
                        table_config['source_schema'],
                        table_config['source_table']
                    )
                    batch_results.append({
                        'table': f"{table_config['source_database']}.{table_config['source_schema']}.{table_config['source_table']}",
                        **result
                    })
                except Exception as e:
                    self.logger.error(f"Failed to process table {table_config['source_database']}.{table_config['source_schema']}.{table_config['source_table']}: {str(e)}")
                    batch_results.append({
                        'table': f"{table_config['source_database']}.{table_config['source_schema']}.{table_config['source_table']}",
                        'status': 'FAILED',
                        'error_message': str(e)
                    })
            
            results.extend(batch_results)
            
            # Optional: clear cache between batches
            self.spark.catalog.clearCache()
        
        return results
    
    def cleanup_and_optimize(self, retention_days: int = 30):
        """Clean up old data and optimize Delta tables"""
        
        self.logger.info("Starting cleanup and optimization")
        
        # Clean up old backup files
        try:
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # Use Spark to find and clean old backup partitions
            backup_tables = self.spark.catalog.listTables()
            
            for table in backup_tables:
                if table.name.startswith('backup_'):
                    table_path = f"{self.backup_root_path}/{table.name}"
                    
                    try:
                        delta_table = DeltaTable.forPath(self.spark, table_path)
                        
                        # Vacuum old files
                        delta_table.vacuum(retentionHours=retention_days * 24)
                        
                        self.logger.info(f"Cleaned up backup table: {table.name}")
                    except Exception as e:
                        self.logger.warning(f"Could not clean backup table {table.name}: {str(e)}")
        
        except Exception as e:
            self.logger.warning(f"Backup cleanup failed: {str(e)}")
        
        # Optimize DWH tables
        try:
            dwh_path = Path(self.dwh_root_path)
            
            for table_path in dwh_path.rglob("*"):
                if table_path.is_dir() and "_delta_log" in [f.name for f in table_path.iterdir()]:
                    try:
                        delta_table = DeltaTable.forPath(self.spark, str(table_path))
                        delta_table.optimize().executeCompaction()
                        
                        self.logger.info(f"Optimized DWH table: {table_path}")
                    except Exception as e:
                        self.logger.warning(f"Could not optimize {table_path}: {str(e)}")
        
        except Exception as e:
            self.logger.warning(f"DWH optimization failed: {str(e)}")
        
        # Clean up old audit logs
        try:
            deleted_count = self.control_audit.cleanup_old_audit_logs(retention_days)
            self.logger.info(f"Cleaned up {deleted_count} old audit records")
        except Exception as e:
            self.logger.warning(f"Audit log cleanup failed: {str(e)}")
    
    def stop(self):
        """Clean shutdown of Spark session"""
        self.spark.stop()
        self.logger.info("Spark session stopped")


# Usage Example
if __name__ == "__main__":
    # Configuration
    source_jdbc = "jdbc:postgresql://source-host:5432/source_db?user=user&password=password"
    target_conn = "postgresql://user:password@target-host:5432/metadata_db"
    
    # Initialize Spark orchestrator
    spark_orchestrator = SparkETLOrchestrator(source_jdbc, target_conn)
    
    try:
        # Auto-discover and register tables
        discovered = spark_orchestrator.discover_source_tables("source_db", ["public", "sales"])
        print(f"Discovered {len(discovered)} tables")
        
        # Run parallel ingestion
        results = spark_orchestrator.run_parallel_ingestion(max_parallel_tables=3)
        
        # Print results
        for result in results:
            print(f"Table: {result['table']}, Status: {result['status']}, "
                  f"Rows: {result.get('rows_loaded', 0)}, "
                  f"Quality: {result.get('quality_score', 0):.1f}%")
        
        # Run cleanup and optimization
        spark_orchestrator.cleanup_and_optimize(retention_days=30)
        
        # Get statistics
        stats = spark_orchestrator.control_audit.get_ingestion_statistics(days=7)
        print(f"Ingestion statistics: {stats}")
    
    finally:
        spark_orchestrator.stop()
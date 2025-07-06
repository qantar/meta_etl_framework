"""
Production-Ready Control & Audit System for Metadata-Driven ETL
Manages ETL control tables, audit trails, and schema evolution
"""

import json
import uuid
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker


class LoadStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING" 
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class SchemaChangeType(Enum):
    COLUMN_ADDED = "COLUMN_ADDED"
    COLUMN_REMOVED = "COLUMN_REMOVED"
    COLUMN_TYPE_CHANGED = "COLUMN_TYPE_CHANGED"
    TABLE_RENAMED = "TABLE_RENAMED"


@dataclass
class SourceTableConfig:
    """Configuration for a source table to be ingested"""
    source_database: str
    source_schema: str
    source_table: str
    increment_column: Optional[str] = None
    business_key_columns: Optional[List[str]] = None
    is_scd_type_2_enabled: bool = False
    is_active: bool = True
    partition_column: Optional[str] = None
    custom_query: Optional[str] = None


@dataclass
class IngestionResult:
    """Result of an ingestion operation"""
    run_id: str
    source_table: str
    start_time: datetime
    end_time: datetime
    rows_extracted: int
    rows_loaded: int
    status: LoadStatus
    error_message: Optional[str] = None
    source_row_hash: Optional[str] = None
    backup_path: Optional[str] = None


class ControlAuditManager:
    """
    Core metadata management system for ETL operations
    Handles control tables, audit trails, and schema evolution
    """
    
    def __init__(self, connection_string: str, backup_root_path: str = "/loaded_data_back_up"):
        self.connection_string = connection_string
        self.backup_root_path = backup_root_path
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        self.logger = logging.getLogger(__name__)
        
        # Initialize control and audit tables
        self._create_control_audit_tables()
    
    def _create_control_audit_tables(self):
        """Create or update control and audit tables"""
        
        control_audit_ddl = """
        -- ETL Control Table
        CREATE TABLE IF NOT EXISTS etl_control_table (
            control_id SERIAL PRIMARY KEY,
            source_database VARCHAR(100) NOT NULL,
            source_schema VARCHAR(100) NOT NULL,
            source_table VARCHAR(100) NOT NULL,
            increment_column VARCHAR(100),
            business_key_columns TEXT[], -- Array of column names
            last_ingestion_time TIMESTAMP,
            is_scd_type_2_enabled BOOLEAN DEFAULT FALSE,
            staging_to_dwh_load_time TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            partition_column VARCHAR(100),
            custom_query TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_database, source_schema, source_table)
        );
        
        -- ETL Audit Log
        CREATE TABLE IF NOT EXISTS etl_audit_log (
            audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_name VARCHAR(200) NOT NULL,
            run_id UUID NOT NULL,
            source_table VARCHAR(300) NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            row_count BIGINT DEFAULT 0,
            rows_extracted BIGINT DEFAULT 0,
            rows_loaded BIGINT DEFAULT 0,
            status VARCHAR(20) NOT NULL,
            error_message TEXT,
            source_row_hash VARCHAR(64),
            backup_path TEXT,
            ingestion_batch_id UUID,
            processing_duration_seconds INTEGER,
            data_size_mb DECIMAL(15,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Schema Evolution Tracking
        CREATE TABLE IF NOT EXISTS table_schema_version (
            schema_version_id SERIAL PRIMARY KEY,
            source_database VARCHAR(100) NOT NULL,
            source_schema VARCHAR(100) NOT NULL,
            source_table VARCHAR(100) NOT NULL,
            schema_hash VARCHAR(64) NOT NULL,
            column_definitions JSONB NOT NULL,
            version_number INTEGER NOT NULL,
            change_type VARCHAR(50),
            change_details JSONB,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE,
            UNIQUE(source_database, source_schema, source_table, version_number)
        );
        
        -- Data Quality Metrics
        CREATE TABLE IF NOT EXISTS etl_data_quality_metrics (
            quality_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            run_id UUID REFERENCES etl_audit_log(run_id),
            source_table VARCHAR(300) NOT NULL,
            total_records BIGINT,
            null_count_by_column JSONB,
            duplicate_count BIGINT,
            unique_count_by_column JSONB,
            data_type_violations JSONB,
            quality_score DECIMAL(5,2),
            quality_rules_applied JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Incremental Load Watermarks
        CREATE TABLE IF NOT EXISTS etl_watermarks (
            watermark_id SERIAL PRIMARY KEY,
            source_database VARCHAR(100) NOT NULL,
            source_schema VARCHAR(100) NOT NULL, 
            source_table VARCHAR(100) NOT NULL,
            watermark_column VARCHAR(100) NOT NULL,
            last_value TEXT NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_database, source_schema, source_table, watermark_column)
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_audit_log_run_id ON etl_audit_log(run_id);
        CREATE INDEX IF NOT EXISTS idx_audit_log_source_table ON etl_audit_log(source_table);
        CREATE INDEX IF NOT EXISTS idx_audit_log_start_time ON etl_audit_log(start_time);
        CREATE INDEX IF NOT EXISTS idx_audit_log_status ON etl_audit_log(status);
        CREATE INDEX IF NOT EXISTS idx_control_table_source ON etl_control_table(source_database, source_schema, source_table);
        CREATE INDEX IF NOT EXISTS idx_schema_version_current ON table_schema_version(source_database, source_schema, source_table, is_current);
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(control_audit_ddl))
            conn.commit()
        
        self.logger.info("Control and audit tables initialized successfully")
    
    def register_source_table(self, config: SourceTableConfig) -> int:
        """Register a new source table for ingestion"""
        
        insert_sql = """
        INSERT INTO etl_control_table (
            source_database, source_schema, source_table, increment_column,
            business_key_columns, is_scd_type_2_enabled, partition_column, custom_query
        ) VALUES (
            :source_database, :source_schema, :source_table, :increment_column,
            :business_key_columns, :is_scd_type_2_enabled, :partition_column, :custom_query
        )
        ON CONFLICT (source_database, source_schema, source_table) 
        DO UPDATE SET
            increment_column = EXCLUDED.increment_column,
            business_key_columns = EXCLUDED.business_key_columns,
            is_scd_type_2_enabled = EXCLUDED.is_scd_type_2_enabled,
            partition_column = EXCLUDED.partition_column,
            custom_query = EXCLUDED.custom_query,
            updated_at = CURRENT_TIMESTAMP
        RETURNING control_id
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(insert_sql), {
                'source_database': config.source_database,
                'source_schema': config.source_schema,
                'source_table': config.source_table,
                'increment_column': config.increment_column,
                'business_key_columns': config.business_key_columns,
                'is_scd_type_2_enabled': config.is_scd_type_2_enabled,
                'partition_column': config.partition_column,
                'custom_query': config.custom_query
            })
            conn.commit()
            control_id = result.fetchone()[0]
            
        self.logger.info(f"Registered source table: {config.source_database}.{config.source_schema}.{config.source_table}")
        return control_id
    
    def get_active_source_tables(self) -> List[Dict]:
        """Get all active source tables for ingestion"""
        
        query = """
        SELECT 
            control_id, source_database, source_schema, source_table,
            increment_column, business_key_columns, last_ingestion_time,
            is_scd_type_2_enabled, staging_to_dwh_load_time, partition_column, custom_query
        FROM etl_control_table 
        WHERE is_active = TRUE
        ORDER BY source_database, source_schema, source_table
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return [dict(row._mapping) for row in result]
    
    def start_ingestion_run(self, source_table: str, job_name: str) -> str:
        """Start a new ingestion run and return run_id"""
        
        run_id = str(uuid.uuid4())
        
        insert_sql = """
        INSERT INTO etl_audit_log (run_id, job_name, source_table, start_time, status)
        VALUES (:run_id, :job_name, :source_table, :start_time, :status)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(insert_sql), {
                'run_id': run_id,
                'job_name': job_name,
                'source_table': source_table,
                'start_time': datetime.now(),
                'status': LoadStatus.RUNNING.value
            })
            conn.commit()
        
        self.logger.info(f"Started ingestion run {run_id} for {source_table}")
        return run_id
    
    def complete_ingestion_run(self, result: IngestionResult):
        """Complete an ingestion run with results"""
        
        duration_seconds = (result.end_time - result.start_time).total_seconds()
        
        update_sql = """
        UPDATE etl_audit_log SET
            end_time = :end_time,
            row_count = :row_count,
            rows_extracted = :rows_extracted,
            rows_loaded = :rows_loaded,
            status = :status,
            error_message = :error_message,
            source_row_hash = :source_row_hash,
            backup_path = :backup_path,
            processing_duration_seconds = :processing_duration_seconds
        WHERE run_id = :run_id
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(update_sql), {
                'run_id': result.run_id,
                'end_time': result.end_time,
                'row_count': result.rows_loaded,
                'rows_extracted': result.rows_extracted,
                'rows_loaded': result.rows_loaded,
                'status': result.status.value,
                'error_message': result.error_message,
                'source_row_hash': result.source_row_hash,
                'backup_path': result.backup_path,
                'processing_duration_seconds': int(duration_seconds)
            })
            conn.commit()
        
        # Update last ingestion time in control table if successful
        if result.status == LoadStatus.SUCCESS:
            self._update_last_ingestion_time(result.source_table, result.end_time)
        
        self.logger.info(f"Completed ingestion run {result.run_id} with status {result.status.value}")
    
    def _update_last_ingestion_time(self, source_table: str, ingestion_time: datetime):
        """Update last ingestion time in control table"""
        
        parts = source_table.split('.')
        if len(parts) == 3:
            database, schema, table = parts
            
            update_sql = """
            UPDATE etl_control_table SET
                last_ingestion_time = :ingestion_time,
                updated_at = CURRENT_TIMESTAMP
            WHERE source_database = :database 
            AND source_schema = :schema 
            AND source_table = :table
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(update_sql), {
                    'ingestion_time': ingestion_time,
                    'database': database,
                    'schema': schema,
                    'table': table
                })
                conn.commit()
    
    def get_last_watermark(self, source_database: str, source_schema: str, 
                          source_table: str, watermark_column: str) -> Optional[str]:
        """Get the last watermark value for incremental loading"""
        
        query = """
        SELECT last_value FROM etl_watermarks 
        WHERE source_database = :database 
        AND source_schema = :schema 
        AND source_table = :table 
        AND watermark_column = :column
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                'database': source_database,
                'schema': source_schema,
                'table': source_table,
                'column': watermark_column
            })
            row = result.fetchone()
            return row[0] if row else None
    
    def update_watermark(self, source_database: str, source_schema: str,
                        source_table: str, watermark_column: str, last_value: str):
        """Update watermark value after successful ingestion"""
        
        upsert_sql = """
        INSERT INTO etl_watermarks (source_database, source_schema, source_table, watermark_column, last_value)
        VALUES (:database, :schema, :table, :column, :last_value)
        ON CONFLICT (source_database, source_schema, source_table, watermark_column)
        DO UPDATE SET 
            last_value = EXCLUDED.last_value,
            last_updated = CURRENT_TIMESTAMP
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(upsert_sql), {
                'database': source_database,
                'schema': source_schema,
                'table': source_table,
                'column': watermark_column,
                'last_value': last_value
            })
            conn.commit()
    
    def detect_schema_changes(self, source_database: str, source_schema: str,
                             source_table: str, current_columns: Dict) -> Optional[Dict]:
        """Detect schema changes and version them"""
        
        # Calculate schema hash
        schema_str = json.dumps(current_columns, sort_keys=True)
        current_hash = hashlib.sha256(schema_str.encode()).hexdigest()
        
        # Get latest schema version
        query = """
        SELECT schema_hash, column_definitions, version_number 
        FROM table_schema_version 
        WHERE source_database = :database 
        AND source_schema = :schema 
        AND source_table = :table 
        AND is_current = TRUE
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                'database': source_database,
                'schema': source_schema,
                'table': source_table
            })
            latest_version = result.fetchone()
        
        # If no previous version or schema changed
        if not latest_version or latest_version[0] != current_hash:
            new_version = 1 if not latest_version else latest_version[2] + 1
            
            # Mark previous version as not current
            if latest_version:
                with self.engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE table_schema_version SET is_current = FALSE 
                        WHERE source_database = :database 
                        AND source_schema = :schema 
                        AND source_table = :table 
                        AND is_current = TRUE
                    """), {
                        'database': source_database,
                        'schema': source_schema,
                        'table': source_table
                    })
                    conn.commit()
            
            # Insert new schema version
            change_details = self._analyze_schema_changes(
                latest_version[1] if latest_version else {}, 
                current_columns
            )
            
            insert_sql = """
            INSERT INTO table_schema_version 
            (source_database, source_schema, source_table, schema_hash, column_definitions, 
             version_number, change_type, change_details)
            VALUES (:database, :schema, :table, :hash, :columns, :version, :change_type, :change_details)
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(insert_sql), {
                    'database': source_database,
                    'schema': source_schema,
                    'table': source_table,
                    'hash': current_hash,
                    'columns': json.dumps(current_columns),
                    'version': new_version,
                    'change_type': change_details.get('primary_change_type'),
                    'change_details': json.dumps(change_details)
                })
                conn.commit()
            
            self.logger.info(f"Schema change detected for {source_database}.{source_schema}.{source_table}, version {new_version}")
            return change_details
        
        return None
    
    def _analyze_schema_changes(self, old_schema: Dict, new_schema: Dict) -> Dict:
        """Analyze what changed between schema versions"""
        
        changes = {
            'primary_change_type': None,
            'columns_added': [],
            'columns_removed': [],
            'columns_type_changed': []
        }
        
        old_cols = set(old_schema.keys())
        new_cols = set(new_schema.keys())
        
        # Find added/removed columns
        changes['columns_added'] = list(new_cols - old_cols)
        changes['columns_removed'] = list(old_cols - new_cols)
        
        # Find type changes
        common_cols = old_cols & new_cols
        for col in common_cols:
            if old_schema[col] != new_schema[col]:
                changes['columns_type_changed'].append({
                    'column': col,
                    'old_type': old_schema[col],
                    'new_type': new_schema[col]
                })
        
        # Determine primary change type
        if changes['columns_added']:
            changes['primary_change_type'] = SchemaChangeType.COLUMN_ADDED.value
        elif changes['columns_removed']:
            changes['primary_change_type'] = SchemaChangeType.COLUMN_REMOVED.value
        elif changes['columns_type_changed']:
            changes['primary_change_type'] = SchemaChangeType.COLUMN_TYPE_CHANGED.value
        
        return changes
    
    def log_data_quality_metrics(self, run_id: str, source_table: str, metrics: Dict):
        """Log data quality metrics for a run"""
        
        insert_sql = """
        INSERT INTO etl_data_quality_metrics 
        (run_id, source_table, total_records, null_count_by_column, duplicate_count,
         unique_count_by_column, data_type_violations, quality_score, quality_rules_applied)
        VALUES (:run_id, :source_table, :total_records, :null_count, :duplicate_count,
                :unique_count, :type_violations, :quality_score, :quality_rules)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(insert_sql), {
                'run_id': run_id,
                'source_table': source_table,
                'total_records': metrics.get('total_records', 0),
                'null_count': json.dumps(metrics.get('null_count_by_column', {})),
                'duplicate_count': metrics.get('duplicate_count', 0),
                'unique_count': json.dumps(metrics.get('unique_count_by_column', {})),
                'type_violations': json.dumps(metrics.get('data_type_violations', {})),
                'quality_score': metrics.get('quality_score', 0.0),
                'quality_rules': json.dumps(metrics.get('quality_rules_applied', {}))
            })
            conn.commit()
    
    def get_ingestion_statistics(self, days: int = 7) -> Dict:
        """Get ingestion statistics for the last N days"""
        
        query = """
        SELECT 
            DATE(start_time) as ingestion_date,
            status,
            COUNT(*) as job_count,
            SUM(row_count) as total_rows,
            AVG(processing_duration_seconds) as avg_duration_seconds,
            SUM(data_size_mb) as total_data_mb
        FROM etl_audit_log 
        WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY DATE(start_time), status
        ORDER BY ingestion_date DESC, status
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), (days,))
            return [dict(row._mapping) for row in result]
    
    def cleanup_old_audit_logs(self, retention_days: int = 90):
        """Clean up old audit logs beyond retention period"""
        
        delete_sql = """
        DELETE FROM etl_audit_log 
        WHERE start_time < CURRENT_DATE - INTERVAL '%s days'
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(delete_sql), (retention_days,))
            deleted_count = result.rowcount
            conn.commit()
        
        self.logger.info(f"Cleaned up {deleted_count} old audit log records")
        return deleted_count
"""
Python Implementation of Metadata-Driven ETL Pipeline
Using Pandas + SQLAlchemy for data processing and backup management
"""

import os
import json
import uuid
import hashlib
import logging
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, MetaData, Table, inspect, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool


class PythonETLOrchestrator:
    """
    Python-based metadata-driven ETL pipeline
    Handles ingestion, staging, DWH loading with SCD Type 2, and backup management
    """
    
    def __init__(self, source_connection_string: str, target_connection_string: str, 
                 backup_root_path: str = "/loaded_data_back_up", dwh_data_path: str = "/dwh_data"):
        
        self.source_engine = create_engine(source_connection_string, poolclass=QueuePool, pool_size=10)
        self.target_engine = create_engine(target_connection_string, poolclass=QueuePool, pool_size=10)
        self.backup_root_path = Path(backup_root_path)
        self.dwh_data_path = Path(dwh_data_path)
        
        # Ensure directories exist
        self.backup_root_path.mkdir(parents=True, exist_ok=True)
        self.dwh_data_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize control and audit system
        from control_audit_system import ControlAuditManager
        self.control_audit = ControlAuditManager(target_connection_string, str(backup_root_path))
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def discover_and_register_tables(self, source_database: str, schema_patterns: List[str] = None) -> List[Dict]:
        """
        Auto-discover tables from source database and register them in control table
        """
        discovered_tables = []
        inspector = inspect(self.source_engine)
        
        # Get all schemas
        schemas = inspector.get_schema_names()
        if schema_patterns:
            schemas = [s for s in schemas if any(pattern in s for pattern in schema_patterns)]
        
        for schema in schemas:
            if schema in ['information_schema', 'pg_catalog', 'pg_toast']:
                continue
                
            tables = inspector.get_table_names(schema=schema)
            
            for table in tables:
                try:
                    # Get table columns for increment detection
                    columns = inspector.get_columns(table, schema=schema)
                    
                    # Auto-detect increment column
                    increment_column = self._detect_increment_column(columns)
                    
                    # Auto-detect business keys (primary keys or unique constraints)
                    business_keys = self._detect_business_keys(inspector, table, schema)
                    
                    # Register table
                    config = SourceTableConfig(
                        source_database=source_database,
                        source_schema=schema,
                        source_table=table,
                        increment_column=increment_column,
                        business_key_columns=business_keys,
                        is_scd_type_2_enabled=len(business_keys) > 0  # Enable SCD2 if we have business keys
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
                    continue
        
        return discovered_tables
    
    def _detect_increment_column(self, columns: List[Dict]) -> Optional[str]:
        """Auto-detect increment column for incremental loads"""
        
        # Look for common patterns
        increment_patterns = [
            'updated_at', 'modified_at', 'last_modified', 'update_timestamp',
            'created_at', 'insert_timestamp', 'created_date', 'date_modified'
        ]
        
        for col in columns:
            col_name = col['name'].lower()
            col_type = str(col['type']).lower()
            
            # Check for timestamp/datetime columns with increment patterns
            if any(pattern in col_name for pattern in increment_patterns):
                if any(t in col_type for t in ['timestamp', 'datetime', 'date']):
                    return col['name']
        
        # Fallback: look for any timestamp column
        for col in columns:
            col_type = str(col['type']).lower()
            if 'timestamp' in col_type and not col.get('nullable', True):
                return col['name']
        
        return None
    
    def _detect_business_keys(self, inspector, table: str, schema: str) -> List[str]:
        """Auto-detect business key columns"""
        
        try:
            # Get primary key
            pk = inspector.get_pk_constraint(table, schema=schema)
            if pk and pk['constrained_columns']:
                return pk['constrained_columns']
            
            # Get unique constraints
            unique_constraints = inspector.get_unique_constraints(table, schema=schema)
            for constraint in unique_constraints:
                if constraint['column_names']:
                    return constraint['column_names']
            
            # Fallback: look for 'id' column
            columns = inspector.get_columns(table, schema=schema)
            for col in columns:
                if col['name'].lower() in ['id', f'{table}_id', 'key']:
                    return [col['name']]
        
        except Exception as e:
            self.logger.warning(f"Could not detect business keys for {schema}.{table}: {str(e)}")
        
        return []
    
    def extract_and_backup_table(self, source_database: str, source_schema: str, 
                                source_table: str) -> Tuple[pd.DataFrame, str, Dict]:
        """
        Extract data from source table and create structured backup
        Returns: (dataframe, backup_path, metadata)
        """
        
        # Generate backup path with timestamp
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = self.backup_root_path / source_database / source_schema / source_table / timestamp_str
        backup_path.mkdir(parents=True, exist_ok=True)
        
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
        
        # Extract data
        self.logger.info(f"Extracting data from {source_database}.{source_schema}.{source_table}")
        df = pd.read_sql(query, self.source_engine)
        
        if df.empty:
            self.logger.info(f"No new data found for {source_database}.{source_schema}.{source_table}")
            return df, str(backup_path), {}
        
        # Add metadata columns
        df['_ingestion_timestamp'] = datetime.now()
        df['_source_system'] = f"{source_database}.{source_schema}.{source_table}"
        
        # Save to backup location
        parquet_filename = f"{source_table}_{timestamp_str}.parquet"
        parquet_path = backup_path / parquet_filename
        df.to_parquet(parquet_path, index=False, compression='snappy')
        
        # Create ingestion metadata
        metadata = {
            'source_database': source_database,
            'source_schema': source_schema,
            'source_table': source_table,
            'extraction_timestamp': datetime.now().isoformat(),
            'row_count': len(df),
            'column_count': len(df.columns),
            'file_size_mb': round(parquet_path.stat().st_size / (1024 * 1024), 2),
            'columns': list(df.columns),
            'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'increment_column': table_config['increment_column'],
            'watermark_value': str(df[table_config['increment_column']].max()) if table_config['increment_column'] and not df.empty else None
        }
        
        # Save metadata
        metadata_path = backup_path / 'ingestion_metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        self.logger.info(f"Backed up {len(df)} rows to {backup_path}")
        return df, str(backup_path), metadata
    
    def load_to_staging(self, df: pd.DataFrame, target_schema: str, target_table: str, 
                       run_id: str) -> int:
        """Load dataframe to staging table"""
        
        if df.empty:
            return 0
        
        # Add run metadata
        df['_run_id'] = run_id
        df['_staging_load_time'] = datetime.now()
        
        staging_table = f"{target_schema}.{target_table}"
        
        # Create staging schema if not exists
        with self.target_engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
            conn.commit()
        
        # Load data using pandas to_sql with append mode
        df.to_sql(target_table, self.target_engine, schema=target_schema, 
                 if_exists='append', index=False, method='multi', chunksize=10000)
        
        self.logger.info(f"Loaded {len(df)} rows to staging table {staging_table}")
        return len(df)
    
    def apply_scd_type_2(self, staging_df: pd.DataFrame, dwh_table_name: str, 
                        business_keys: List[str], batch_id: str) -> pd.DataFrame:
        """
        Apply Slowly Changing Dimension Type 2 logic
        Returns the final dataframe to be loaded to DWH
        """
        
        if staging_df.empty or not business_keys:
            # No SCD logic needed
            staging_df['_dwh_insert_time'] = datetime.now()
            staging_df['_ingestion_batch_id'] = batch_id
            staging_df['_valid_from'] = datetime.now()
            staging_df['_valid_to'] = pd.to_datetime('9999-12-31')
            staging_df['_is_current'] = True
            staging_df['_version'] = 1
            return staging_df
        
        try:
            # Read current DWH data
            current_dwh_df = pd.read_sql(f"SELECT * FROM {dwh_table_name} WHERE _is_current = TRUE", 
                                       self.target_engine)
        except Exception:
            # Table doesn't exist yet
            current_dwh_df = pd.DataFrame()
        
        if current_dwh_df.empty:
            # First load - all records are new
            staging_df['_dwh_insert_time'] = datetime.now()
            staging_df['_ingestion_batch_id'] = batch_id
            staging_df['_valid_from'] = datetime.now()
            staging_df['_valid_to'] = pd.to_datetime('9999-12-31')
            staging_df['_is_current'] = True
            staging_df['_version'] = 1
            return staging_df
        
        # Prepare staging data
        staging_df = staging_df.copy()
        staging_df['_dwh_insert_time'] = datetime.now()
        staging_df['_ingestion_batch_id'] = batch_id
        staging_df['_valid_from'] = datetime.now()
        staging_df['_valid_to'] = pd.to_datetime('9999-12-31')
        staging_df['_is_current'] = True
        staging_df['_version'] = 1
        
        # Create hash columns for comparison (excluding metadata columns)
        data_columns = [col for col in staging_df.columns if not col.startswith('_')]
        
        def create_hash(row):
            return hashlib.md5(str(tuple(row[data_columns].values)).encode()).hexdigest()
        
        staging_df['_row_hash'] = staging_df.apply(create_hash, axis=1)
        current_dwh_df['_row_hash'] = current_dwh_df.apply(create_hash, axis=1)
        
        # Find records that need SCD Type 2 processing
        final_records = []
        
        for _, staging_row in staging_df.iterrows():
            # Find matching business key in current DWH
            business_key_match = current_dwh_df
            for key in business_keys:
                business_key_match = business_key_match[business_key_match[key] == staging_row[key]]
            
            if business_key_match.empty:
                # New record - add as is
                final_records.append(staging_row)
            else:
                # Existing business key - check if data changed
                existing_row = business_key_match.iloc[0]
                
                if existing_row['_row_hash'] != staging_row['_row_hash']:
                    # Data changed - need to historize old record and insert new
                    # Set version for new record
                    staging_row['_version'] = existing_row['_version'] + 1
                    final_records.append(staging_row)
                    
                    # Mark old record as historical (will be updated in database)
                    historical_update_sql = f"""
                        UPDATE {dwh_table_name} SET 
                            _valid_to = CURRENT_DATE - INTERVAL '1 day',
                            _is_current = FALSE
                        WHERE {' AND '.join([f"{key} = %s" for key in business_keys])}
                        AND _is_current = TRUE
                    """
                    
                    with self.target_engine.connect() as conn:
                        conn.execute(text(historical_update_sql), 
                                   [staging_row[key] for key in business_keys])
                        conn.commit()
                # If data hasn't changed, don't add the record
        
        if final_records:
            return pd.DataFrame(final_records)
        else:
            return pd.DataFrame(columns=staging_df.columns)
    
    def load_to_dwh(self, staging_df: pd.DataFrame, source_database: str, 
                   source_schema: str, source_table: str, batch_id: str) -> int:
        """Load data from staging to DWH with SCD Type 2 logic"""
        
        if staging_df.empty:
            return 0
        
        # Get control configuration
        control_tables = self.control_audit.get_active_source_tables()
        table_config = next((t for t in control_tables 
                           if t['source_database'] == source_database 
                           and t['source_schema'] == source_schema 
                           and t['source_table'] == source_table), None)
        
        dwh_schema = 'dwh'
        dwh_table = source_table
        dwh_table_name = f"{dwh_schema}.{dwh_table}"
        
        # Create DWH schema if not exists
        with self.target_engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {dwh_schema}"))
            conn.commit()
        
        # Apply SCD Type 2 logic if enabled
        if table_config and table_config['is_scd_type_2_enabled'] and table_config['business_key_columns']:
            final_df = self.apply_scd_type_2(staging_df, dwh_table_name, 
                                           table_config['business_key_columns'], batch_id)
        else:
            # Simple load without SCD
            final_df = staging_df.copy()
            final_df['_dwh_insert_time'] = datetime.now()
            final_df['_ingestion_batch_id'] = batch_id
            final_df['_valid_from'] = datetime.now()
            final_df['_valid_to'] = pd.to_datetime('9999-12-31')
            final_df['_is_current'] = True
            final_df['_version'] = 1
        
        if final_df.empty:
            return 0
        
        # Remove hash column if it exists
        if '_row_hash' in final_df.columns:
            final_df = final_df.drop('_row_hash', axis=1)
        
        # Load to DWH table
        final_df.to_sql(dwh_table, self.target_engine, schema=dwh_schema, 
                       if_exists='append', index=False, method='multi', chunksize=5000)
        
        # Save unified parquet file to DWH data lake
        dwh_data_folder = self.dwh_data_path / source_database / source_schema / source_table
        dwh_data_folder.mkdir(parents=True, exist_ok=True)
        
        parquet_path = dwh_data_folder / f"{source_table}.parquet"
        
        # Read all current data and save unified file
        try:
            all_dwh_data = pd.read_sql(f"SELECT * FROM {dwh_table_name}", self.target_engine)
            all_dwh_data.to_parquet(parquet_path, index=False, compression='snappy')
        except Exception as e:
            self.logger.warning(f"Could not create unified parquet file: {str(e)}")
        
        self.logger.info(f"Loaded {len(final_df)} rows to DWH table {dwh_table_name}")
        return len(final_df)
    
    def calculate_data_quality_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate comprehensive data quality metrics"""
        
        if df.empty:
            return {'total_records': 0, 'quality_score': 0.0}
        
        metrics = {
            'total_records': len(df),
            'null_count_by_column': {},
            'unique_count_by_column': {},
            'duplicate_count': 0,
            'data_type_violations': {},
            'quality_score': 100.0
        }
        
        # Calculate null counts and uniqueness
        data_columns = [col for col in df.columns if not col.startswith('_')]
        
        for col in data_columns:
            null_count = df[col].isnull().sum()
            unique_count = df[col].nunique()
            
            metrics['null_count_by_column'][col] = int(null_count)
            metrics['unique_count_by_column'][col] = int(unique_count)
        
        # Calculate duplicate count (excluding metadata columns)
        if data_columns:
            metrics['duplicate_count'] = len(df) - len(df[data_columns].drop_duplicates())
        
        # Calculate quality score
        total_cells = len(df) * len(data_columns)
        total_nulls = sum(metrics['null_count_by_column'].values())
        
        if total_cells > 0:
            null_percentage = (total_nulls / total_cells) * 100
            duplicate_percentage = (metrics['duplicate_count'] / len(df)) * 100 if len(df) > 0 else 0
            
            metrics['quality_score'] = max(0.0, 100.0 - null_percentage - duplicate_percentage)
        
        metrics['quality_rules_applied'] = {
            'null_check': True,
            'duplicate_check': True,
            'completeness_check': True,
            'uniqueness_check': True
        }
        
        return metrics
    
    def run_table_ingestion(self, source_database: str, source_schema: str, source_table: str) -> Dict:
        """Run complete ingestion pipeline for a single table"""
        
        full_table_name = f"{source_database}.{source_schema}.{source_table}"
        job_name = f"ingest_{source_table}_complete"
        
        # Start ingestion run
        run_id = self.control_audit.start_ingestion_run(full_table_name, job_name)
        batch_id = str(uuid.uuid4())
        
        start_time = datetime.now()
        
        try:
            # Stage 1: Extract and backup
            self.logger.info(f"Starting extraction for {full_table_name}")
            staging_df, backup_path, extraction_metadata = self.extract_and_backup_table(
                source_database, source_schema, source_table
            )
            
            rows_extracted = len(staging_df)
            
            if staging_df.empty:
                # Complete run with no data
                result = IngestionResult(
                    run_id=run_id,
                    source_table=full_table_name,
                    start_time=start_time,
                    end_time=datetime.now(),
                    rows_extracted=0,
                    rows_loaded=0,
                    status=LoadStatus.SUCCESS,
                    backup_path=backup_path
                )
                self.control_audit.complete_ingestion_run(result)
                return {'status': 'SUCCESS', 'message': 'No new data to process', 'rows_processed': 0}
            
            # Stage 2: Load to staging
            self.logger.info(f"Loading to staging for {full_table_name}")
            staging_rows = self.load_to_staging(staging_df, 'staging', source_table, run_id)
            
            # Stage 3: Calculate data quality
            quality_metrics = self.calculate_data_quality_metrics(staging_df)
            self.control_audit.log_data_quality_metrics(run_id, full_table_name, quality_metrics)
            
            # Stage 4: Load to DWH
            self.logger.info(f"Loading to DWH for {full_table_name}")
            dwh_rows = self.load_to_dwh(staging_df, source_database, source_schema, source_table, batch_id)
            
            # Update watermark if incremental
            if extraction_metadata.get('watermark_value'):
                self.control_audit.update_watermark(
                    source_database, source_schema, source_table,
                    extraction_metadata['increment_column'], 
                    extraction_metadata['watermark_value']
                )
            
            # Calculate data hash
            data_hash = hashlib.md5(staging_df.to_string().encode()).hexdigest()
            
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
                source_row_hash=data_hash,
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
                'duration_seconds': (end_time - start_time).total_seconds()
            }
            
        except Exception as e:
            error_message = f"Pipeline failed: {str(e)}"
            self.logger.error(f"Ingestion failed for {full_table_name}: {error_message}")
            
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
    
    def run_all_table_ingestions(self) -> List[Dict]:
        """Run ingestion pipeline for all active tables"""
        
        active_tables = self.control_audit.get_active_source_tables()
        results = []
        
        self.logger.info(f"Starting ingestion for {len(active_tables)} active tables")
        
        for table_config in active_tables:
            try:
                result = self.run_table_ingestion(
                    table_config['source_database'],
                    table_config['source_schema'],
                    table_config['source_table']
                )
                results.append({
                    'table': f"{table_config['source_database']}.{table_config['source_schema']}.{table_config['source_table']}",
                    **result
                })
            except Exception as e:
                self.logger.error(f"Failed to process table {table_config['source_database']}.{table_config['source_schema']}.{table_config['source_table']}: {str(e)}")
                results.append({
                    'table': f"{table_config['source_database']}.{table_config['source_schema']}.{table_config['source_table']}",
                    'status': 'FAILED',
                    'error_message': str(e)
                })
        
        return results


# Usage Example
if __name__ == "__main__":
    # Configuration
    source_conn = "postgresql://user:password@source-host:5432/source_db"
    target_conn = "postgresql://user:password@target-host:5432/metadata_db"
    
    # Initialize orchestrator
    orchestrator = PythonETLOrchestrator(source_conn, target_conn)
    
    # Auto-discover and register tables
    discovered = orchestrator.discover_and_register_tables("source_db", ["public", "sales"])
    print(f"Discovered {len(discovered)} tables")
    
    # Run complete pipeline
    results = orchestrator.run_all_table_ingestions()
    
    # Print results
    for result in results:
        print(f"Table: {result['table']}, Status: {result['status']}, Rows: {result.get('rows_loaded', 0)}")
    
    # Get statistics
    stats = orchestrator.control_audit.get_ingestion_statistics(days=7)
    print(f"Ingestion statistics: {stats}")
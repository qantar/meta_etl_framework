"""
Unified ETL Engine - Cross-Engine Orchestrator
Provides a single interface to run ETL pipelines across SQL, Python, and PySpark engines
"""

import os
import json
import yaml
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path

# Import our engine implementations
from control_audit_system import ControlAuditManager, SourceTableConfig, LoadStatus
from python_implementation import PythonETLOrchestrator
from pyspark_implementation import SparkETLOrchestrator


class EngineType(Enum):
    SQL = "sql"
    PYTHON = "python"
    PYSPARK = "pyspark"


class ExecutionMode(Enum):
    SINGLE_TABLE = "single_table"
    BATCH_TABLES = "batch_tables"
    ALL_TABLES = "all_tables"
    DISCOVERY_MODE = "discovery_mode"


@dataclass
class EngineConfig:
    """Configuration for ETL engines"""
    engine_type: EngineType
    source_connection: str
    target_connection: str
    backup_path: str = "/data/backup"
    dwh_path: str = "/data/dwh"
    staging_path: str = "/data/staging"
    max_parallel_tables: int = 5
    enable_optimizations: bool = True
    retention_days: int = 30


@dataclass
class PipelineConfig:
    """Complete pipeline configuration"""
    name: str
    description: str
    engines: Dict[str, EngineConfig]
    default_engine: EngineType
    auto_discovery: bool = True
    schema_patterns: List[str] = None
    notification_settings: Dict = None
    schedule_cron: str = None


class UnifiedETLEngine:
    """
    Unified ETL Engine that orchestrates across SQL, Python, and PySpark implementations
    Provides intelligent engine selection and cross-engine compatibility
    """
    
    def __init__(self, config: Union[PipelineConfig, str, Dict]):
        """
        Initialize with configuration
        config can be PipelineConfig object, path to config file, or config dict
        """
        
        if isinstance(config, str):
            self.config = self._load_config_from_file(config)
        elif isinstance(config, dict):
            self.config = self._load_config_from_dict(config)
        else:
            self.config = config
        
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Initialize control and audit system (shared across all engines)
        self.control_audit = ControlAuditManager(
            self.config.engines[self.config.default_engine.value].target_connection,
            self.config.engines[self.config.default_engine.value].backup_path
        )
        
        # Initialize engines
        self.engines = {}
        self._initialize_engines()
        
        self.logger.info(f"Unified ETL Engine initialized with {len(self.engines)} engines")
    
    def _load_config_from_file(self, config_path: str) -> PipelineConfig:
        """Load configuration from YAML or JSON file"""
        
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            if config_path.suffix.lower() in ['.yaml', '.yml']:
                config_dict = yaml.safe_load(f)
            else:
                config_dict = json.load(f)
        
        return self._load_config_from_dict(config_dict)
    
    def _load_config_from_dict(self, config_dict: Dict) -> PipelineConfig:
        """Load configuration from dictionary"""
        
        engines = {}
        for engine_name, engine_config in config_dict.get('engines', {}).items():
            engines[engine_name] = EngineConfig(
                engine_type=EngineType(engine_config['engine_type']),
                source_connection=engine_config['source_connection'],
                target_connection=engine_config['target_connection'],
                backup_path=engine_config.get('backup_path', '/data/backup'),
                dwh_path=engine_config.get('dwh_path', '/data/dwh'),
                staging_path=engine_config.get('staging_path', '/data/staging'),
                max_parallel_tables=engine_config.get('max_parallel_tables', 5),
                enable_optimizations=engine_config.get('enable_optimizations', True),
                retention_days=engine_config.get('retention_days', 30)
            )
        
        return PipelineConfig(
            name=config_dict['name'],
            description=config_dict.get('description', ''),
            engines=engines,
            default_engine=EngineType(config_dict.get('default_engine', 'python')),
            auto_discovery=config_dict.get('auto_discovery', True),
            schema_patterns=config_dict.get('schema_patterns', ['public']),
            notification_settings=config_dict.get('notification_settings', {}),
            schedule_cron=config_dict.get('schedule_cron')
        )
    
    def _setup_logging(self):
        """Setup comprehensive logging"""
        
        log_level = logging.INFO
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # Create logs directory
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        # Configure root logger
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(log_dir / f'unified_etl_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
    
    def _initialize_engines(self):
        """Initialize all configured engines"""
        
        for engine_name, engine_config in self.config.engines.items():
            try:
                if engine_config.engine_type == EngineType.PYTHON:
                    self.engines[engine_name] = PythonETLOrchestrator(
                        engine_config.source_connection,
                        engine_config.target_connection,
                        engine_config.backup_path,
                        engine_config.dwh_path
                    )
                
                elif engine_config.engine_type == EngineType.PYSPARK:
                    self.engines[engine_name] = SparkETLOrchestrator(
                        engine_config.source_connection,
                        engine_config.target_connection,
                        engine_config.backup_path,
                        engine_config.dwh_path,
                        engine_config.staging_path
                    )
                
                elif engine_config.engine_type == EngineType.SQL:
                    # SQL engine uses stored procedures - just validate connection
                    from sqlalchemy import create_engine
                    sql_engine = create_engine(engine_config.target_connection)
                    sql_engine.connect().close()
                    self.engines[engine_name] = {'type': 'sql', 'connection': engine_config.target_connection}
                
                self.logger.info(f"Initialized {engine_config.engine_type.value} engine: {engine_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to initialize engine {engine_name}: {str(e)}")
    
    def intelligent_engine_selection(self, table_info: Dict) -> str:
        """
        Intelligently select the best engine based on data characteristics
        """
        
        estimated_rows = table_info.get('estimated_row_count', 0)
        has_complex_transformations = table_info.get('complex_transformations', False)
        requires_real_time = table_info.get('real_time_processing', False)
        
        # Engine selection logic
        if estimated_rows > 10_000_000:  # Large datasets
            if 'pyspark' in self.engines:
                return 'pyspark'
            elif 'python' in self.engines:
                return 'python'
        
        elif estimated_rows > 1_000_000:  # Medium datasets
            if has_complex_transformations and 'pyspark' in self.engines:
                return 'pyspark'
            elif 'python' in self.engines:
                return 'python'
        
        else:  # Small datasets
            if 'sql' in self.engines and not has_complex_transformations:
                return 'sql'
            elif 'python' in self.engines:
                return 'python'
        
        # Fallback to default engine
        default_engine_name = self.config.default_engine.value
        if default_engine_name in self.engines:
            return default_engine_name
        
        # Ultimate fallback
        return list(self.engines.keys())[0] if self.engines else None
    
    def discover_and_register_all_tables(self) -> Dict[str, List[Dict]]:
        """
        Discover tables using the most capable engine and register them
        """
        
        discovery_results = {}
        
        # Use PySpark for discovery if available (most capable), otherwise Python
        discovery_engine = None
        if 'pyspark' in self.engines:
            discovery_engine = self.engines['pyspark']
            engine_type = 'pyspark'
        elif 'python' in self.engines:
            discovery_engine = self.engines['python']
            engine_type = 'python'
        else:
            self.logger.error("No suitable engine available for table discovery")
            return discovery_results
        
        self.logger.info(f"Starting table discovery using {engine_type} engine")
        
        try:
            # Extract source database name from configuration
            source_config = list(self.config.engines.values())[0]
            source_database = self._extract_database_name(source_config.source_connection)
            
            if engine_type == 'pyspark':
                discovered = discovery_engine.discover_source_tables(
                    source_database, 
                    self.config.schema_patterns
                )
            else:  # python
                discovered = discovery_engine.discover_and_register_tables(
                    source_database,
                    self.config.schema_patterns
                )
            
            discovery_results[engine_type] = discovered
            
            # Enrich with metadata for intelligent engine selection
            enriched_tables = []
            for table in discovered:
                # Get table size estimate
                table_stats = self._get_table_statistics(table['full_table_name'])
                table.update(table_stats)
                enriched_tables.append(table)
            
            discovery_results[engine_type] = enriched_tables
            
            self.logger.info(f"Discovered {len(enriched_tables)} tables")
            
        except Exception as e:
            self.logger.error(f"Table discovery failed: {str(e)}")
        
        return discovery_results
    
    def _extract_database_name(self, connection_string: str) -> str:
        """Extract database name from connection string"""
        
        if 'postgresql://' in connection_string:
            # Format: postgresql://user:pass@host:port/database
            return connection_string.split('/')[-1].split('?')[0]
        elif 'jdbc:postgresql://' in connection_string:
            # Format: jdbc:postgresql://host:port/database?user=...
            return connection_string.split('/')[-1].split('?')[0]
        
        return 'unknown_db'
    
    def _get_table_statistics(self, full_table_name: str) -> Dict:
        """Get basic table statistics for engine selection"""
        
        try:
            # Use control audit system to get table info
            parts = full_table_name.split('.')
            if len(parts) >= 3:
                database, schema, table = parts[0], parts[1], parts[2]
                
                # Estimate based on table name patterns and metadata
                estimated_rows = 1000  # Default estimate
                
                # Simple heuristics based on table names
                if any(keyword in table.lower() for keyword in ['fact', 'transaction', 'log', 'event']):
                    estimated_rows = 5_000_000  # Large fact tables
                elif any(keyword in table.lower() for keyword in ['dim', 'dimension', 'lookup']):
                    estimated_rows = 50_000   # Medium dimension tables
                elif any(keyword in table.lower() for keyword in ['config', 'setting', 'parameter']):
                    estimated_rows = 100      # Small config tables
                
                return {
                    'estimated_row_count': estimated_rows,
                    'complex_transformations': 'fact' in table.lower(),
                    'real_time_processing': 'event' in table.lower() or 'log' in table.lower()
                }
        
        except Exception as e:
            self.logger.warning(f"Could not get statistics for {full_table_name}: {str(e)}")
        
        return {
            'estimated_row_count': 1000,
            'complex_transformations': False,
            'real_time_processing': False
        }
    
    def run_single_table_ingestion(self, source_database: str, source_schema: str, 
                                  source_table: str, engine_name: str = None) -> Dict:
        """
        Run ingestion for a single table using specified or intelligent engine selection
        """
        
        full_table_name = f"{source_database}.{source_schema}.{source_table}"
        
        # Get table info for intelligent engine selection
        table_info = self._get_table_statistics(full_table_name)
        table_info['full_table_name'] = full_table_name
        
        # Select engine
        if engine_name is None:
            engine_name = self.intelligent_engine_selection(table_info)
        
        if engine_name not in self.engines:
            raise ValueError(f"Engine '{engine_name}' not available. Available engines: {list(self.engines.keys())}")
        
        self.logger.info(f"Running ingestion for {full_table_name} using {engine_name} engine")
        
        try:
            engine = self.engines[engine_name]
            
            if isinstance(engine, PythonETLOrchestrator):
                result = engine.run_table_ingestion(source_database, source_schema, source_table)
            
            elif isinstance(engine, SparkETLOrchestrator):
                result = engine.run_spark_table_ingestion(source_database, source_schema, source_table)
            
            elif isinstance(engine, dict) and engine['type'] == 'sql':
                # Execute SQL stored procedure
                result = self._run_sql_ingestion(engine['connection'], source_database, source_schema, source_table)
            
            else:
                raise ValueError(f"Unknown engine type: {type(engine)}")
            
            # Add engine info to result
            result['engine_used'] = engine_name
            result['table'] = full_table_name
            
            return result
            
        except Exception as e:
            self.logger.error(f"Ingestion failed for {full_table_name} using {engine_name}: {str(e)}")
            return {
                'status': 'FAILED',
                'table': full_table_name,
                'engine_used': engine_name,
                'error_message': str(e)
            }
    
    def _run_sql_ingestion(self, connection_string: str, source_database: str, 
                          source_schema: str, source_table: str) -> Dict:
        """Execute SQL-based ingestion using stored procedures"""
        
        from sqlalchemy import create_engine, text
        
        engine = create_engine(connection_string)
        
        try:
            with engine.connect() as conn:
                # Run the complete pipeline stored procedure
                result = conn.execute(
                    text("SELECT * FROM run_complete_etl_pipeline(:db, :schema, :table)"),
                    {"db": source_database, "schema": source_schema, "table": source_table}
                )
                
                pipeline_result = result.fetchone()
                
                if pipeline_result:
                    return {
                        'status': pipeline_result.status,
                        'staging_run_id': str(pipeline_result.staging_run_id),
                        'dwh_run_id': str(pipeline_result.dwh_run_id),
                        'duration_seconds': pipeline_result.total_duration_seconds,
                        'rows_processed': 0  # SQL procedure doesn't return this easily
                    }
                else:
                    return {'status': 'FAILED', 'error_message': 'No result from SQL procedure'}
        
        except Exception as e:
            return {'status': 'FAILED', 'error_message': str(e)}
    
    def run_batch_ingestion(self, table_list: List[Dict] = None, 
                           engine_preferences: Dict[str, str] = None) -> List[Dict]:
        """
        Run ingestion for multiple tables with intelligent engine selection
        """
        
        if table_list is None:
            # Get all active tables
            active_tables = self.control_audit.get_active_source_tables()
            table_list = [
                {
                    'source_database': t['source_database'],
                    'source_schema': t['source_schema'],
                    'source_table': t['source_table']
                }
                for t in active_tables
            ]
        
        results = []
        engine_preferences = engine_preferences or {}
        
        self.logger.info(f"Starting batch ingestion for {len(table_list)} tables")
        
        for table_info in table_list:
            try:
                # Get engine preference or use intelligent selection
                table_key = f"{table_info['source_database']}.{table_info['source_schema']}.{table_info['source_table']}"
                preferred_engine = engine_preferences.get(table_key)
                
                result = self.run_single_table_ingestion(
                    table_info['source_database'],
                    table_info['source_schema'],
                    table_info['source_table'],
                    preferred_engine
                )
                
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Failed to process table {table_info}: {str(e)}")
                results.append({
                    'status': 'FAILED',
                    'table': f"{table_info['source_database']}.{table_info['source_schema']}.{table_info['source_table']}",
                    'error_message': str(e)
                })
        
        return results
    
    def run_parallel_ingestion(self, max_parallel_engines: int = 3) -> List[Dict]:
        """
        Run ingestion across multiple engines in parallel for maximum throughput
        """
        
        active_tables = self.control_audit.get_active_source_tables()
        
        # Group tables by optimal engine
        engine_assignments = {}
        
        for table in active_tables:
            table_info = self._get_table_statistics(f"{table['source_database']}.{table['source_schema']}.{table['source_table']}")
            optimal_engine = self.intelligent_engine_selection(table_info)
            
            if optimal_engine not in engine_assignments:
                engine_assignments[optimal_engine] = []
            
            engine_assignments[optimal_engine].append(table)
        
        self.logger.info(f"Parallel execution plan: {[(k, len(v)) for k, v in engine_assignments.items()]}")
        
        all_results = []
        
        # Execute each engine's batch
        for engine_name, tables in engine_assignments.items():
            try:
                engine = self.engines[engine_name]
                
                if isinstance(engine, SparkETLOrchestrator):
                    # Use Spark's built-in parallel processing
                    results = engine.run_parallel_ingestion(
                        max_parallel_tables=self.config.engines[engine_name].max_parallel_tables
                    )
                
                elif isinstance(engine, PythonETLOrchestrator):
                    # Run sequentially for Python engine (can be enhanced with threading)
                    results = []
                    for table in tables:
                        result = engine.run_table_ingestion(
                            table['source_database'],
                            table['source_schema'],
                            table['source_table']
                        )
                        result['table'] = f"{table['source_database']}.{table['source_schema']}.{table['source_table']}"
                        result['engine_used'] = engine_name
                        results.append(result)
                
                else:  # SQL engine
                    results = []
                    for table in tables:
                        result = self._run_sql_ingestion(
                            engine['connection'],
                            table['source_database'],
                            table['source_schema'],
                            table['source_table']
                        )
                        result['table'] = f"{table['source_database']}.{table['source_schema']}.{table['source_table']}"
                        result['engine_used'] = engine_name
                        results.append(result)
                
                all_results.extend(results)
                
            except Exception as e:
                self.logger.error(f"Engine {engine_name} execution failed: {str(e)}")
                # Add failed results for all tables assigned to this engine
                for table in tables:
                    all_results.append({
                        'status': 'FAILED',
                        'table': f"{table['source_database']}.{table['source_schema']}.{table['source_table']}",
                        'engine_used': engine_name,
                        'error_message': str(e)
                    })
        
        return all_results
    
    def get_pipeline_status(self, days: int = 7) -> Dict:
        """Get comprehensive pipeline status and statistics"""
        
        stats = self.control_audit.get_ingestion_statistics(days)
        
        # Aggregate by engine type and status
        engine_stats = {}
        for stat in stats:
            status = stat['status']
            # Note: We don't track engine type in audit log yet, could be enhanced
            if status not in engine_stats:
                engine_stats[status] = {
                    'job_count': 0,
                    'total_rows': 0,
                    'avg_duration': 0
                }
            
            engine_stats[status]['job_count'] += stat['job_count']
            engine_stats[status]['total_rows'] += stat.get('total_rows', 0) or 0
            engine_stats[status]['avg_duration'] = stat.get('avg_duration_seconds', 0) or 0
        
        return {
            'pipeline_name': self.config.name,
            'available_engines': list(self.engines.keys()),
            'default_engine': self.config.default_engine.value,
            'period_days': days,
            'status_summary': engine_stats,
            'detailed_stats': stats
        }
    
    def cleanup_and_optimize_all(self):
        """Run cleanup and optimization across all engines"""
        
        self.logger.info("Starting cleanup and optimization across all engines")
        
        for engine_name, engine in self.engines.items():
            try:
                engine_config = self.config.engines[engine_name]
                
                if isinstance(engine, SparkETLOrchestrator):
                    engine.cleanup_and_optimize(engine_config.retention_days)
                
                elif isinstance(engine, PythonETLOrchestrator):
                    # Python cleanup (could be enhanced)
                    self.control_audit.cleanup_old_audit_logs(engine_config.retention_days)
                
                elif isinstance(engine, dict) and engine['type'] == 'sql':
                    # SQL cleanup using stored procedures
                    self.control_audit.cleanup_old_audit_logs(engine_config.retention_days)
                
                self.logger.info(f"Completed cleanup for {engine_name} engine")
                
            except Exception as e:
                self.logger.error(f"Cleanup failed for {engine_name}: {str(e)}")
    
    def export_pipeline_config(self, output_path: str):
        """Export current pipeline configuration"""
        
        config_dict = {
            'name': self.config.name,
            'description': self.config.description,
            'default_engine': self.config.default_engine.value,
            'auto_discovery': self.config.auto_discovery,
            'schema_patterns': self.config.schema_patterns,
            'engines': {}
        }
        
        for engine_name, engine_config in self.config.engines.items():
            config_dict['engines'][engine_name] = {
                'engine_type': engine_config.engine_type.value,
                'source_connection': engine_config.source_connection,
                'target_connection': engine_config.target_connection,
                'backup_path': engine_config.backup_path,
                'dwh_path': engine_config.dwh_path,
                'staging_path': engine_config.staging_path,
                'max_parallel_tables': engine_config.max_parallel_tables,
                'enable_optimizations': engine_config.enable_optimizations,
                'retention_days': engine_config.retention_days
            }
        
        output_path = Path(output_path)
        with open(output_path, 'w') as f:
            if output_path.suffix.lower() in ['.yaml', '.yml']:
                yaml.dump(config_dict, f, default_flow_style=False, indent=2)
            else:
                json.dump(config_dict, f, indent=2)
        
        self.logger.info(f"Pipeline configuration exported to {output_path}")
    
    def stop_all_engines(self):
        """Clean shutdown of all engines"""
        
        for engine_name, engine in self.engines.items():
            try:
                if isinstance(engine, SparkETLOrchestrator):
                    engine.stop()
                # Python and SQL engines don't need explicit shutdown
                
                self.logger.info(f"Stopped {engine_name} engine")
            except Exception as e:
                self.logger.error(f"Error stopping {engine_name}: {str(e)}")


# Factory function for easy initialization
def create_unified_engine(config_path: str = None, **kwargs) -> UnifiedETLEngine:
    """
    Factory function to create UnifiedETLEngine with sensible defaults
    """
    
    if config_path:
        return UnifiedETLEngine(config_path)
    
    # Create default configuration
    default_config = PipelineConfig(
        name=kwargs.get('name', 'default_pipeline'),
        description=kwargs.get('description', 'Default ETL Pipeline'),
        engines={
            'python': EngineConfig(
                engine_type=EngineType.PYTHON,
                source_connection=kwargs.get('source_connection', 'postgresql://user:pass@localhost/source'),
                target_connection=kwargs.get('target_connection', 'postgresql://user:pass@localhost/target')
            )
        },
        default_engine=EngineType.PYTHON,
        auto_discovery=kwargs.get('auto_discovery', True),
        schema_patterns=kwargs.get('schema_patterns', ['public'])
    )
    
    return UnifiedETLEngine(default_config)


# Usage Example
if __name__ == "__main__":
    # Example configuration
    config = {
        'name': 'production_etl',
        'description': 'Production ETL Pipeline with Multi-Engine Support',
        'default_engine': 'python',
        'auto_discovery': True,
        'schema_patterns': ['public', 'sales', 'analytics'],
        'engines': {
            'python': {
                'engine_type': 'python',
                'source_connection': 'postgresql://user:password@source-host:5432/source_db',
                'target_connection': 'postgresql://user:password@target-host:5432/metadata_db',
                'backup_path': '/data/python_backup',
                'dwh_path': '/data/python_dwh',
                'max_parallel_tables': 3,
                'retention_days': 30
            },
            'pyspark': {
                'engine_type': 'pyspark',
                'source_connection': 'jdbc:postgresql://source-host:5432/source_db?user=user&password=password',
                'target_connection': 'postgresql://user:password@target-host:5432/metadata_db',
                'backup_path': '/delta/backup',
                'dwh_path': '/delta/dwh',
                'staging_path': '/delta/staging',
                'max_parallel_tables': 10,
                'retention_days': 90
            },
            'sql': {
                'engine_type': 'sql',
                'source_connection': 'postgresql://user:password@source-host:5432/source_db',
                'target_connection': 'postgresql://user:password@target-host:5432/metadata_db',
                'retention_days': 30
            }
        }
    }
    
    try:
        # Initialize unified engine
        unified_engine = UnifiedETLEngine(config)
        
        # Auto-discover tables
        print("🔍 Discovering tables...")
        discovery_results = unified_engine.discover_and_register_all_tables()
        print(f"Found {sum(len(tables) for tables in discovery_results.values())} tables")
        
        # Run parallel ingestion across all engines
        print("🚀 Running parallel ingestion...")
        results = unified_engine.run_parallel_ingestion()
        
        # Print summary
        success_count = len([r for r in results if r['status'] == 'SUCCESS'])
        failed_count = len([r for r in results if r['status'] == 'FAILED'])
        
        print(f"✅ Pipeline completed: {success_count} successful, {failed_count} failed")
        
        # Get pipeline status
        status = unified_engine.get_pipeline_status()
        print(f"📊 Pipeline status: {status}")
        
        # Cleanup and optimize
        print("🧹 Running cleanup and optimization...")
        unified_engine.cleanup_and_optimize_all()
        
    finally:
        # Clean shutdown
        unified_engine.stop_all_engines()
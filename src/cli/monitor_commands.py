#!/usr/bin/env python3
"""
ETL Framework CLI - Command Line Interface
Production-ready CLI for managing metadata-driven ETL pipelines
"""

import click
import json
import yaml
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from tabulate import tabulate
import colorama
from colorama import Fore, Back, Style

# Initialize colorama for cross-platform colored output
colorama.init()

# Import our core modules
from unified_engine import UnifiedETLEngine, create_unified_engine, EngineType, ExecutionMode
from control_audit_system import ControlAuditManager, SourceTableConfig


class ETLCLIContext:
    """Context object for CLI commands"""
    
    def __init__(self):
        self.config_path = None
        self.engine = None
        self.verbose = False
        self.output_format = 'table'
    
    def setup_logging(self, verbose: bool = False):
        """Setup logging based on verbosity"""
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )


# Global context
pass_context = click.make_pass_decorator(ETLCLIContext, ensure=True)


def print_success(message: str):
    """Print success message with green color"""
    click.echo(f"{Fore.GREEN}✅ {message}{Style.RESET_ALL}")


def print_error(message: str):
    """Print error message with red color"""
    click.echo(f"{Fore.RED}❌ {message}{Style.RESET_ALL}")


def print_warning(message: str):
    """Print warning message with yellow color"""
    click.echo(f"{Fore.YELLOW}⚠️  {message}{Style.RESET_ALL}")


def print_info(message: str):
    """Print info message with blue color"""
    click.echo(f"{Fore.BLUE}ℹ️  {message}{Style.RESET_ALL}")


def format_output(data: List[Dict], output_format: str = 'table') -> str:
    """Format output based on specified format"""
    
    if not data:
        return "No data to display"
    
    if output_format == 'json':
        return json.dumps(data, indent=2, default=str)
    
    elif output_format == 'yaml':
        return yaml.dump(data, default_flow_style=False)
    
    else:  # table format
        if isinstance(data[0], dict):
            headers = list(data[0].keys())
            rows = [[str(row.get(header, '')) for header in headers] for row in data]
            return tabulate(rows, headers=headers, tablefmt='grid')
        else:
            return str(data)


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), help='Configuration file path')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json', 'yaml']), 
              default='table', help='Output format')
@pass_context
def cli(ctx: ETLCLIContext, config: str, verbose: bool, output_format: str):
    """
    🚀 ETL Framework CLI - Metadata-Driven Data Pipeline Management
    
    A powerful command-line tool for managing enterprise ETL pipelines with
    support for SQL, Python, and PySpark execution engines.
    """
    ctx.verbose = verbose
    ctx.output_format = output_format
    ctx.setup_logging(verbose)
    
    if config:
        ctx.config_path = config
        try:
            ctx.engine = UnifiedETLEngine(config)
            print_success(f"Initialized ETL engine with config: {config}")
        except Exception as e:
            print_error(f"Failed to initialize engine: {str(e)}")
            sys.exit(1)


@cli.group()
def config():
    """Configuration management commands"""
    pass


@config.command('init')
@click.option('--name', '-n', prompt='Pipeline name', help='Pipeline name')
@click.option('--source-host', prompt='Source database host', help='Source database host')
@click.option('--source-db', prompt='Source database name', help='Source database name')
@click.option('--source-user', prompt='Source database user', help='Source database user')
@click.option('--source-password', prompt='Source database password', hide_input=True, help='Source database password')
@click.option('--target-host', prompt='Target database host', help='Target database host')
@click.option('--target-db', prompt='Target database name', help='Target database name')
@click.option('--target-user', prompt='Target database user', help='Target database user')
@click.option('--target-password', prompt='Target database password', hide_input=True, help='Target database password')
@click.option('--engines', multiple=True, type=click.Choice(['python', 'pyspark', 'sql']), 
              default=['python'], help='Engines to configure')
@click.option('--output', '-o', default='etl_config.yaml', help='Output configuration file')
def init_config(name, source_host, source_db, source_user, source_password,
                target_host, target_db, target_user, target_password, engines, output):
    """Initialize a new ETL pipeline configuration"""
    
    source_conn = f"postgresql://{source_user}:{source_password}@{source_host}:5432/{source_db}"
    target_conn = f"postgresql://{target_user}:{target_password}@{target_host}:5432/{target_db}"
    
    config = {
        'name': name,
        'description': f'ETL Pipeline for {name}',
        'default_engine': engines[0] if engines else 'python',
        'auto_discovery': True,
        'schema_patterns': ['public'],
        'engines': {}
    }
    
    for engine in engines:
        if engine == 'python':
            config['engines']['python'] = {
                'engine_type': 'python',
                'source_connection': source_conn,
                'target_connection': target_conn,
                'backup_path': '/data/python_backup',
                'dwh_path': '/data/python_dwh',
                'max_parallel_tables': 3,
                'retention_days': 30
            }
        elif engine == 'pyspark':
            spark_source = f"jdbc:postgresql://{source_host}:5432/{source_db}?user={source_user}&password={source_password}"
            config['engines']['pyspark'] = {
                'engine_type': 'pyspark',
                'source_connection': spark_source,
                'target_connection': target_conn,
                'backup_path': '/delta/backup',
                'dwh_path': '/delta/dwh',
                'staging_path': '/delta/staging',
                'max_parallel_tables': 10,
                'retention_days': 90
            }
        elif engine == 'sql':
            config['engines']['sql'] = {
                'engine_type': 'sql',
                'source_connection': source_conn,
                'target_connection': target_conn,
                'retention_days': 30
            }
    
    output_path = Path(output)
    with open(output_path, 'w') as f:
        if output_path.suffix.lower() in ['.yaml', '.yml']:
            yaml.dump(config, f, default_flow_style=False, indent=2)
        else:
            json.dump(config, f, indent=2)
    
    print_success(f"Configuration initialized: {output_path}")
    print_info(f"Next steps:")
    print_info(f"1. Edit {output_path} to customize settings")
    print_info(f"2. Run: etl-cli --config {output_path} discover")
    print_info(f"3. Run: etl-cli --config {output_path} pipeline run")


@config.command('validate')
@pass_context
def validate_config(ctx: ETLCLIContext):
    """Validate the current configuration"""
    
    if not ctx.config_path:
        print_error("No configuration file specified. Use --config option.")
        sys.exit(1)
    
    try:
        # Try to initialize engine
        engine = UnifiedETLEngine(ctx.config_path)
        print_success("Configuration is valid")
        
        # Show configuration summary
        print_info("Configuration Summary:")
        print(f"  Pipeline Name: {engine.config.name}")
        print(f"  Default Engine: {engine.config.default_engine.value}")
        print(f"  Available Engines: {list(engine.engines.keys())}")
        print(f"  Auto Discovery: {engine.config.auto_discovery}")
        print(f"  Schema Patterns: {engine.config.schema_patterns}")
        
    except Exception as e:
        print_error(f"Configuration validation failed: {str(e)}")
        sys.exit(1)


@config.command('show')
@pass_context
def show_config(ctx: ETLCLIContext):
    """Show current configuration"""
    
    if not ctx.config_path:
        print_error("No configuration file specified. Use --config option.")
        sys.exit(1)
    
    try:
        with open(ctx.config_path, 'r') as f:
            if ctx.config_path.endswith('.yaml') or ctx.config_path.endswith('.yml'):
                config_data = yaml.safe_load(f)
            else:
                config_data = json.load(f)
        
        if ctx.output_format == 'json':
            print(json.dumps(config_data, indent=2))
        elif ctx.output_format == 'yaml':
            print(yaml.dump(config_data, default_flow_style=False))
        else:
            print_info("Current Configuration:")
            print(yaml.dump(config_data, default_flow_style=False))
            
    except Exception as e:
        print_error(f"Failed to read configuration: {str(e)}")


@cli.group()
def discover():
    """Table discovery and registration commands"""
    pass


@discover.command('tables')
@click.option('--schemas', '-s', multiple=True, help='Schema patterns to discover')
@click.option('--register', is_flag=True, help='Register discovered tables')
@pass_context
def discover_tables(ctx: ETLCLIContext, schemas, register):
    """Discover tables from source databases"""
    
    if not ctx.engine:
        print_error("No ETL engine initialized. Use --config option.")
        sys.exit(1)
    
    try:
        if schemas:
            ctx.engine.config.schema_patterns = list(schemas)
        
        print_info(f"Discovering tables from schemas: {ctx.engine.config.schema_patterns}")
        
        results = ctx.engine.discover_and_register_all_tables()
        
        all_tables = []
        for engine_type, tables in results.items():
            for table in tables:
                table['discovery_engine'] = engine_type
                all_tables.append(table)
        
        if all_tables:
            print_success(f"Discovered {len(all_tables)} tables")
            print(format_output(all_tables, ctx.output_format))
        else:
            print_warning("No tables discovered")
            
    except Exception as e:
        print_error(f"Table discovery failed: {str(e)}")
        sys.exit(1)


@discover.command('status')
@pass_context
def discover_status(ctx: ETLCLIContext):
    """Show registered tables status"""
    
    if not ctx.engine:
        print_error("No ETL engine initialized. Use --config option.")
        sys.exit(1)
    
    try:
        active_tables = ctx.engine.control_audit.get_active_source_tables()
        
        if active_tables:
            # Format for display
            display_tables = []
            for table in active_tables:
                display_tables.append({
                    'Table': f"{table['source_database']}.{table['source_schema']}.{table['source_table']}",
                    'Increment Column': table.get('increment_column', 'None'),
                    'Business Keys': ', '.join(table.get('business_key_columns', []) or []),
                    'SCD Type 2': 'Yes' if table.get('is_scd_type_2_enabled') else 'No',
                    'Last Ingestion': table.get('last_ingestion_time', 'Never')
                })
            
            print_success(f"Found {len(display_tables)} registered tables")
            print(format_output(display_tables, ctx.output_format))
        else:
            print_warning("No tables registered")
            print_info("Run 'etl-cli discover tables --register' to discover and register tables")
            
    except Exception as e:
        print_error(f"Failed to get table status: {str(e)}")


@cli.group()
def pipeline():
    """Pipeline execution commands"""
    pass


@pipeline.command('run')
@click.option('--table', '-t', help='Specific table to process (format: database.schema.table)')
@click.option('--engine', '-e', type=click.Choice(['python', 'pyspark', 'sql']), 
              help='Specific engine to use')
@click.option('--parallel', is_flag=True, help='Run in parallel mode')
@click.option('--dry-run', is_flag=True, help='Show execution plan without running')
@pass_context
def run_pipeline(ctx: ETLCLIContext, table, engine, parallel, dry_run):
    """Execute ETL pipeline"""
    
    if not ctx.engine:
        print_error("No ETL engine initialized. Use --config option.")
        sys.exit(1)
    
    try:
        if dry_run:
            print_info("🔍 Dry run mode - showing execution plan")
            
            if table:
                parts = table.split('.')
                if len(parts) != 3:
                    print_error("Table format should be: database.schema.table")
                    sys.exit(1)
                
                table_info = ctx.engine._get_table_statistics(table)
                selected_engine = engine or ctx.engine.intelligent_engine_selection(table_info)
                
                print_info(f"Would execute table: {table}")
                print_info(f"Selected engine: {selected_engine}")
                print_info(f"Estimated rows: {table_info.get('estimated_row_count', 'Unknown')}")
                
            else:
                active_tables = ctx.engine.control_audit.get_active_source_tables()
                print_info(f"Would process {len(active_tables)} tables")
                
                if parallel:
                    print_info("Execution mode: Parallel")
                else:
                    print_info("Execution mode: Sequential")
            
            return
        
        # Actual execution
        if table:
            parts = table.split('.')
            if len(parts) != 3:
                print_error("Table format should be: database.schema.table")
                sys.exit(1)
            
            database, schema, table_name = parts
            print_info(f"🚀 Processing table: {table}")
            
            result = ctx.engine.run_single_table_ingestion(database, schema, table_name, engine)
            
            if result['status'] == 'SUCCESS':
                print_success(f"Table processed successfully")
                print_info(f"Rows processed: {result.get('rows_loaded', 0)}")
                print_info(f"Engine used: {result.get('engine_used', 'Unknown')}")
                print_info(f"Duration: {result.get('duration_seconds', 0):.1f} seconds")
            else:
                print_error(f"Table processing failed: {result.get('error_message', 'Unknown error')}")
                sys.exit(1)
        
        else:
            # Process all tables
            print_info("🚀 Processing all registered tables")
            
            if parallel:
                results = ctx.engine.run_parallel_ingestion()
            else:
                results = ctx.engine.run_batch_ingestion()
            
            # Summarize results
            success_count = len([r for r in results if r['status'] == 'SUCCESS'])
            failed_count = len([r for r in results if r['status'] == 'FAILED'])
            total_rows = sum([r.get('rows_loaded', 0) for r in results if r['status'] == 'SUCCESS'])
            
            print_success(f"Pipeline completed: {success_count} successful, {failed_count} failed")
            print_info(f"Total rows processed: {total_rows:,}")
            
            # Show detailed results
            if ctx.verbose or ctx.output_format != 'table':
                print(format_output(results, ctx.output_format))
            else:
                # Show summary table
                summary = []
                for result in results:
                    summary.append({
                        'Table': result['table'],
                        'Status': result['status'],
                        'Engine': result.get('engine_used', 'Unknown'),
                        'Rows': result.get('rows_loaded', 0),
                        'Duration (s)': result.get('duration_seconds', 0)
                    })
                print(format_output(summary, 'table'))
            
            if failed_count > 0:
                print_warning(f"{failed_count} tables failed. Use --verbose for error details.")
                
    except Exception as e:
        print_error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)


@pipeline.command('status')
@click.option('--days', '-d', default=7, help='Number of days to include in status')
@pass_context
def pipeline_status(ctx: ETLCLIContext, days):
    """Show pipeline execution status and statistics"""
    
    if not ctx.engine:
        print_error("No ETL engine initialized. Use --config option.")
        sys.exit(1)
    
    try:
        status = ctx.engine.get_pipeline_status(days)
        
        print_success(f"Pipeline Status: {status['pipeline_name']}")
        print_info(f"Period: Last {days} days")
        print_info(f"Available engines: {', '.join(status['available_engines'])}")
        print_info(f"Default engine: {status['default_engine']}")
        
        # Show status summary
        if status['status_summary']:
            print("\n📊 Status Summary:")
            summary_data = []
            for status_type, stats in status['status_summary'].items():
                summary_data.append({
                    'Status': status_type,
                    'Jobs': stats['job_count'],
                    'Total Rows': f"{stats['total_rows']:,}",
                    'Avg Duration (s)': f"{stats['avg_duration']:.1f}"
                })
            
            print(format_output(summary_data, 'table'))
        
        # Show detailed stats if requested
        if ctx.verbose and status['detailed_stats']:
            print("\n📈 Detailed Statistics:")
            print(format_output(status['detailed_stats'], ctx.output_format))
            
    except Exception as e:
        print_error(f"Failed to get pipeline status: {str(e)}")


@cli.group()
def maintenance():
    """Maintenance and cleanup commands"""
    pass


@maintenance.command('cleanup')
@click.option('--retention-days', '-r', default=30, help='Retention period in days')
@click.option('--dry-run', is_flag=True, help='Show what would be cleaned without actually doing it')
@pass_context
def cleanup(ctx: ETLCLIContext, retention_days, dry_run):
    """Clean up old data and optimize storage"""
    
    if not ctx.engine:
        print_error("No ETL engine initialized. Use --config option.")
        sys.exit(1)
    
    try:
        if dry_run:
            print_info(f"🔍 Dry run - would clean up data older than {retention_days} days")
            
            # Show what would be cleaned
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            print_info(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Get old audit records count
            old_records = ctx.engine.control_audit.get_ingestion_statistics(days=9999)  # Get all
            old_count = len([r for r in old_records if datetime.strptime(str(r['ingestion_date']), '%Y-%m-%d') < cutoff_date.date()])
            
            print_info(f"Would clean up approximately {old_count} audit log records")
            print_info("Would optimize all Delta tables and backup files")
            
        else:
            print_info(f"🧹 Cleaning up data older than {retention_days} days")
            
            ctx.engine.cleanup_and_optimize_all()
            
            print_success("Cleanup and optimization completed")
            
    except Exception as e:
        print_error(f"Cleanup failed: {str(e)}")


@maintenance.command('optimize')
@click.option('--engine', '-e', type=click.Choice(['python', 'pyspark', 'sql']), 
              help='Specific engine to optimize')
@pass_context
def optimize(ctx: ETLCLIContext, engine):
    """Optimize storage and performance"""
    
    if not ctx.engine:
        print_error("No ETL engine initialized. Use --config option.")
        sys.exit(1)
    
    try:
        print_info("⚡ Running optimization...")
        
        if engine:
            if engine in ctx.engine.engines:
                # Optimize specific engine
                engine_obj = ctx.engine.engines[engine]
                if hasattr(engine_obj, 'cleanup_and_optimize'):
                    engine_obj.cleanup_and_optimize()
                print_success(f"Optimization completed for {engine} engine")
            else:
                print_error(f"Engine '{engine}' not available")
        else:
            # Optimize all engines
            ctx.engine.cleanup_and_optimize_all()
            print_success("Optimization completed for all engines")
            
    except Exception as e:
        print_error(f"Optimization failed: {str(e)}")


@cli.group()
def monitor():
    """Monitoring and alerting commands"""
    pass


@monitor.command('health')
@pass_context
def health_check(ctx: ETLCLIContext):
    """Check pipeline health status"""
    
    if not ctx.engine:
        print_error("No ETL engine initialized. Use --config option.")
        sys.exit(1)
    
    try:
        print_info("🔍 Performing health check...")
        
        # Check recent failures
        stats = ctx.engine.control_audit.get_ingestion_statistics(days=1)
        failed_jobs = [s for s in stats if s['status'] == 'FAILED']
        
        if failed_jobs:
            print_warning(f"Found {len(failed_jobs)} failed jobs in the last 24 hours")
            for job in failed_jobs:
                print_error(f"  Failed: {job.get('source_table', 'Unknown')}")
        else:
            print_success("No failed jobs in the last 24 hours")
        
        # Check engine availability
        print_info("Engine Status:")
        for engine_name in ctx.engine.engines:
            print_success(f"  ✅ {engine_name} engine: Available")
        
        # Check table registration
        active_tables = ctx.engine.control_audit.get_active_source_tables()
        print_info(f"Registered tables: {len(active_tables)}")
        
        if len(active_tables) == 0:
            print_warning("No tables registered. Run 'discover tables' first.")
        
        print_success("Health check completed")
        
    except Exception as e:
        print_error(f"Health check failed: {str(e)}")


@monitor.command('logs')
@click.option('--lines', '-n', default=50, help='Number of log lines to show')
@click.option('--follow', '-f', is_flag=True, help='Follow log output')
@click.option('--level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']), 
              default='INFO', help='Log level filter')
def show_logs(lines, follow, level):
    """Show ETL pipeline logs"""
    
    log_dir = Path('logs')
    if not log_dir.exists():
        print_warning("No log directory found")
        return
    
    # Find latest log file
    log_files = list(log_dir.glob('unified_etl_*.log'))
    if not log_files:
        print_warning("No log files found")
        return
    
    latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
    
    try:
        if follow:
            print_info(f"Following log file: {latest_log}")
            # Simple tail -f implementation
            import subprocess
            subprocess.run(['tail', '-f', '-n', str(lines), str(latest_log)])
        else:
            # Read last N lines
            with open(latest_log, 'r') as f:
                all_lines = f.readlines()
                recent_lines = all_lines[-lines:]
                
                for line in recent_lines:
                    if level in line:
                        # Color code based on log level
                        if 'ERROR' in line:
                            print(f"{Fore.RED}{line.strip()}{Style.RESET_ALL}")
                        elif 'WARNING' in line:
                            print(f"{Fore.YELLOW}{line.strip()}{Style.RESET_ALL}")
                        elif 'INFO' in line:
                            print(f"{Fore.BLUE}{line.strip()}{Style.RESET_ALL}")
                        else:
                            print(line.strip())
                            
    except Exception as e:
        print_error(f"Failed to read logs: {str(e)}")


@cli.command()
@click.option('--include-secrets', is_flag=True, help='Include connection strings (sensitive)')
@pass_context
def info(ctx: ETLCLIContext, include_secrets):
    """Show system information and configuration"""
    
    print_info("🚀 ETL Framework CLI Information")
    print(f"Version: 1.0.0")
    print(f"Python: {sys.version}")
    print(f"Working Directory: {os.getcwd()}")
    
    if ctx.config_path:
        print(f"Configuration: {ctx.config_path}")
        
        if ctx.engine:
            print(f"Pipeline Name: {ctx.engine.config.name}")
            print(f"Available Engines: {list(ctx.engine.engines.keys())}")
            print(f"Default Engine: {ctx.engine.config.default_engine.value}")
            
            if include_secrets:
                print_warning("🔐 Connection strings (SENSITIVE):")
                for engine_name, engine_config in ctx.engine.config.engines.items():
                    print(f"  {engine_name}:")
                    print(f"    Source: {engine_config.source_connection}")
                    print(f"    Target: {engine_config.target_connection}")
    else:
        print_warning("No configuration loaded")
        print_info("Use --config option to specify configuration file")
        print_info("Use 'etl-cli config init' to create new configuration")


if __name__ == '__main__':
    cli()
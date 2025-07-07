"""
src/cli/main.py
Main CLI entry point for the ETL Framework
Provides comprehensive command-line interface for all framework operations
"""

import click
import logging
import sys
import os
from pathlib import Path
from typing import Optional, Dict, Any
import yaml
import json
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich import print as rich_print

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.unified_engine import UnifiedETLEngine, PipelineConfig
from core.engine_selector import EngineSelector
from quality.data_quality_framework import DataQualityFramework
from utils.config_manager import ConfigManager
from utils.notification_service import NotificationService

# Initialize rich console for beautiful output
console = Console()


def print_success(message: str):
    """Print success message in green"""
    console.print(f"✅ {message}", style="bold green")


def print_error(message: str):
    """Print error message in red"""
    console.print(f"❌ {message}", style="bold red")


def print_warning(message: str):
    """Print warning message in yellow"""
    console.print(f"⚠️  {message}", style="bold yellow")


def print_info(message: str):
    """Print info message in blue"""
    console.print(f"ℹ️  {message}", style="bold blue")


# Global context object to pass state between commands
class CLIContext:
    def __init__(self):
        self.config_path: Optional[str] = None
        self.engine: Optional[UnifiedETLEngine] = None
        self.config_manager: Optional[ConfigManager] = None
        self.verbose: bool = False


@click.group()
@click.option('--config', '-c', help='Path to configuration file')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--environment', '-e', default='development', help='Environment (development/staging/production)')
@click.pass_context
def cli(ctx, config, verbose, environment):
    """
    🚀 ETL Framework - Production-Ready Metadata-Driven Data Pipeline
    
    A comprehensive ETL framework supporting SQL, Python, and PySpark engines
    with advanced features like data quality validation, intelligent scheduling,
    and real-time monitoring.
    
    Examples:
      etl-cli config init --name my_pipeline
      etl-cli discover tables --register
      etl-cli pipeline run --parallel
      etl-cli monitor health
    """
    
    # Initialize CLI context
    ctx.ensure_object(CLIContext)
    ctx.obj.config_path = config
    ctx.obj.verbose = verbose
    
    # Setup logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize configuration manager
    try:
        ctx.obj.config_manager = ConfigManager(environment=environment)
        if config:
            # Load user-specified config
            ctx.obj.config_manager.load_config(config)
    except Exception as e:
        print_error(f"Failed to initialize configuration: {str(e)}")
        sys.exit(1)


# ==========================================
# CONFIGURATION COMMANDS
# ==========================================

@cli.group()
def config():
    """⚙️  Configuration management commands"""
    pass


@config.command('init')
@click.option('--name', required=True, help='Pipeline name')
@click.option('--description', help='Pipeline description')
@click.option('--output', '-o', help='Output configuration file path')
@click.pass_context
def config_init(ctx, name, description, output):
    """Initialize a new ETL pipeline configuration"""
    
    try:
        print_info(f"Initializing configuration for pipeline: {name}")
        
        # Create default configuration
        config_data = {
            'name': name,
            'description': description or f"ETL Pipeline: {name}",
            'default_engine': 'python',
            'auto_discovery': True,
            'schema_patterns': ['public'],
            'engines': {
                'python': {
                    'engine_type': 'python',
                    'source_connection': 'postgresql://user:password@localhost:5432/source_db',
                    'target_connection': 'postgresql://user:password@localhost:5432/target_db',
                    'backup_path': '/data/backup',
                    'dwh_path': '/data/dwh',
                    'staging_path': '/data/staging',
                    'max_parallel_tables': 5,
                    'enable_optimizations': True,
                    'retention_days': 30
                },
                'spark': {
                    'engine_type': 'pyspark',
                    'source_connection': 'jdbc:postgresql://localhost:5432/source_db',
                    'target_connection': 'postgresql://user:password@localhost:5432/target_db',
                    'backup_path': '/delta/backup',
                    'dwh_path': '/delta/dwh',
                    'staging_path': '/delta/staging',
                    'max_parallel_tables': 10,
                    'enable_optimizations': True,
                    'retention_days': 30
                }
            }
        }
        
        # Save configuration
        output_path = output or f"{name}_config.yaml"
        with open(output_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)
        
        print_success(f"Configuration initialized: {output_path}")
        print_info("Edit the configuration file to update connection strings and settings")
        
    except Exception as e:
        print_error(f"Failed to initialize configuration: {str(e)}")
        sys.exit(1)


@config.command('validate')
@click.option('--config-file', help='Configuration file to validate')
@click.pass_context
def config_validate(ctx, config_file):
    """Validate configuration file"""
    
    try:
        config_path = config_file or ctx.obj.config_path
        if not config_path:
            print_error("No configuration file specified. Use --config-file or global --config")
            sys.exit(1)
        
        print_info(f"Validating configuration: {config_path}")
        
        # Load and validate configuration
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Perform validation checks
        required_fields = ['name', 'engines', 'default_engine']
        for field in required_fields:
            if field not in config_data:
                print_error(f"Missing required field: {field}")
                sys.exit(1)
        
        # Validate engines
        if not config_data['engines']:
            print_error("No engines configured")
            sys.exit(1)
        
        for engine_name, engine_config in config_data['engines'].items():
            if 'engine_type' not in engine_config:
                print_error(f"Engine {engine_name} missing engine_type")
                sys.exit(1)
            if 'source_connection' not in engine_config:
                print_error(f"Engine {engine_name} missing source_connection")
                sys.exit(1)
        
        print_success("Configuration is valid")
        
    except Exception as e:
        print_error(f"Configuration validation failed: {str(e)}")
        sys.exit(1)


# ==========================================
# DISCOVERY COMMANDS
# ==========================================

@cli.group()
def discover():
    """🔍 Data source discovery commands"""
    pass


@discover.command('tables')
@click.option('--register', is_flag=True, help='Register discovered tables')
@click.option('--schema', multiple=True, help='Specific schemas to discover')
@click.option('--limit', type=int, help='Limit number of tables to discover')
@click.pass_context
def discover_tables(ctx, register, schema, limit):
    """Discover tables from source databases"""
    
    try:
        # Initialize engine if not already done
        if not ctx.obj.engine:
            if not ctx.obj.config_path:
                print_error("Configuration required. Use --config option or run 'config init' first")
                sys.exit(1)
            
            ctx.obj.engine = UnifiedETLEngine(ctx.obj.config_path)
        
        print_info("Discovering tables from source databases...")
        
        # Run discovery
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
            task = progress.add_task("Scanning databases...", total=None)
            
            # Convert schema tuple to list
            schema_patterns = list(schema) if schema else None
            discovered_tables = ctx.obj.engine.discover_tables(schema_patterns)
            
            progress.update(task, description="Discovery complete")
        
        if limit:
            discovered_tables = discovered_tables[:limit]
        
        # Display results
        if discovered_tables:
            table = Table(title="Discovered Tables")
            table.add_column("Database", style="cyan")
            table.add_column("Schema", style="magenta")
            table.add_column("Table", style="green")
            table.add_column("Estimated Rows", style="yellow")
            table.add_column("Size (MB)", style="blue")
            
            for tbl in discovered_tables:
                table.add_row(
                    tbl.get('database', 'N/A'),
                    tbl.get('schema', 'N/A'),
                    tbl.get('table', 'N/A'),
                    str(tbl.get('estimated_rows', 'Unknown')),
                    str(tbl.get('estimated_size_mb', 'Unknown'))
                )
            
            console.print(table)
            print_success(f"Found {len(discovered_tables)} tables")
            
            if register:
                print_info("Registering tables for ETL processing...")
                registered_count = ctx.obj.engine.register_discovered_tables(discovered_tables)
                print_success(f"Registered {registered_count} tables")
        else:
            print_warning("No tables discovered")
    
    except Exception as e:
        print_error(f"Table discovery failed: {str(e)}")
        if ctx.obj.verbose:
            import traceback
            console.print(traceback.format_exc())
        sys.exit(1)


# ==========================================
# PIPELINE COMMANDS
# ==========================================

@cli.group()
def pipeline():
    """🔄 Pipeline execution commands"""
    pass


@pipeline.command('run')
@click.option('--table', help='Run pipeline for specific table (database.schema.table)')
@click.option('--parallel', is_flag=True, help='Run multiple tables in parallel')
@click.option('--max-parallel', type=int, default=5, help='Maximum parallel executions')
@click.option('--engine', help='Force specific engine (python/spark/sql)')
@click.option('--dry-run', is_flag=True, help='Validate pipeline without execution')
@click.pass_context
def pipeline_run(ctx, table, parallel, max_parallel, engine, dry_run):
    """Execute ETL pipeline"""
    
    try:
        # Initialize engine
        if not ctx.obj.engine:
            if not ctx.obj.config_path:
                print_error("Configuration required. Use --config option")
                sys.exit(1)
            ctx.obj.engine = UnifiedETLEngine(ctx.obj.config_path)
        
        if dry_run:
            print_info("🧪 Dry run mode - validating pipeline configuration")
            
            # Validate configuration and connections
            validation_results = ctx.obj.engine.validate_pipeline()
            
            if validation_results['valid']:
                print_success("Pipeline validation passed")
                for check in validation_results['checks']:
                    print_info(f"✓ {check}")
            else:
                print_error("Pipeline validation failed")
                for error in validation_results['errors']:
                    print_error(f"✗ {error}")
                sys.exit(1)
            return
        
        if table:
            # Run specific table
            print_info(f"Running pipeline for table: {table}")
            parts = table.split('.')
            if len(parts) != 3:
                print_error("Table must be in format: database.schema.table")
                sys.exit(1)
            
            database, schema, table_name = parts
            result = ctx.obj.engine.run_table_ingestion(database, schema, table_name, force_engine=engine)
            
            if result['status'] == 'SUCCESS':
                print_success(f"Pipeline completed successfully in {result.get('duration_seconds', 0):.1f}s")
                print_info(f"Processed {result.get('rows_loaded', 0):,} rows")
            else:
                print_error(f"Pipeline failed: {result.get('error_message', 'Unknown error')}")
                sys.exit(1)
        
        elif parallel:
            # Run all tables in parallel
            print_info(f"Running pipeline for all tables (max {max_parallel} parallel)")
            
            with Progress() as progress:
                task = progress.add_task("Executing pipeline...", total=None)
                
                results = ctx.obj.engine.run_parallel_ingestion(
                    max_parallel_tables=max_parallel,
                    force_engine=engine
                )
                
                progress.update(task, description="Pipeline complete")
            
            # Display results summary
            successful = [r for r in results if r.get('status') == 'SUCCESS']
            failed = [r for r in results if r.get('status') == 'FAILED']
            
            print_success(f"Pipeline completed: {len(successful)} successful, {len(failed)} failed")
            
            if failed:
                print_error("Failed tables:")
                for failure in failed:
                    print_error(f"  • {failure.get('table', 'Unknown')}: {failure.get('error_message', 'Unknown error')}")
        
        else:
            # Run all tables sequentially
            print_info("Running pipeline for all tables (sequential)")
            results = ctx.obj.engine.run_all_table_ingestions()
            
            successful = [r for r in results if r.get('status') == 'SUCCESS']
            failed = [r for r in results if r.get('status') == 'FAILED']
            
            print_success(f"Pipeline completed: {len(successful)} successful, {len(failed)} failed")
    
    except Exception as e:
        print_error(f"Pipeline execution failed: {str(e)}")
        if ctx.obj.verbose:
            import traceback
            console.print(traceback.format_exc())
        sys.exit(1)


@pipeline.command('status')
@click.option('--days', type=int, default=1, help='Number of days to show')
@click.option('--table', help='Filter by specific table')
@click.option('--failed-only', is_flag=True, help='Show only failed executions')
@click.pass_context
def pipeline_status(ctx, days, table, failed_only):
    """Show pipeline execution status and history"""
    
    try:
        if not ctx.obj.engine:
            if not ctx.obj.config_path:
                print_error("Configuration required. Use --config option")
                sys.exit(1)
            ctx.obj.engine = UnifiedETLEngine(ctx.obj.config_path)
        
        print_info(f"Pipeline status for last {days} day(s)")
        
        # Get execution statistics
        stats = ctx.obj.engine.control_audit.get_ingestion_statistics(days=days)
        
        if table:
            stats = [s for s in stats if table.lower() in s.get('source_table', '').lower()]
        
        if failed_only:
            stats = [s for s in stats if s.get('status') == 'FAILED']
        
        if stats:
            # Create status table
            status_table = Table(title=f"Pipeline Executions - Last {days} Days")
            status_table.add_column("Table", style="cyan")
            status_table.add_column("Status", style="bold")
            status_table.add_column("Start Time", style="magenta")
            status_table.add_column("Duration", style="yellow")
            status_table.add_column("Rows", style="green")
            status_table.add_column("Error", style="red")
            
            for stat in stats[-20:]:  # Show last 20 executions
                status_style = "green" if stat.get('status') == 'SUCCESS' else "red"
                duration = f"{stat.get('processing_duration_seconds', 0):.1f}s" if stat.get('processing_duration_seconds') else 'N/A'
                
                status_table.add_row(
                    stat.get('source_table', 'Unknown'),
                    f"[{status_style}]{stat.get('status', 'Unknown')}[/{status_style}]",
                    stat.get('start_time', 'Unknown')[:19] if stat.get('start_time') else 'Unknown',
                    duration,
                    f"{stat.get('row_count', 0):,}",
                    (stat.get('error_message', '')[:50] + '...') if len(stat.get('error_message', '')) > 50 else stat.get('error_message', '')
                )
            
            console.print(status_table)
            
            # Summary statistics
            total_executions = len(stats)
            successful = len([s for s in stats if s.get('status') == 'SUCCESS'])
            failed = len([s for s in stats if s.get('status') == 'FAILED'])
            
            summary = Panel(
                f"Total Executions: {total_executions}\n"
                f"Successful: {successful} ({successful/total_executions*100:.1f}%)\n"
                f"Failed: {failed} ({failed/total_executions*100:.1f}%)",
                title="Summary",
                border_style="blue"
            )
            console.print(summary)
        else:
            print_warning("No execution records found for the specified criteria")
    
    except Exception as e:
        print_error(f"Failed to get pipeline status: {str(e)}")
        sys.exit(1)


# ==========================================
# QUALITY COMMANDS
# ==========================================

@cli.group()
def quality():
    """✅ Data quality commands"""
    pass


@quality.command('check')
@click.option('--table', required=True, help='Table to check (database.schema.table)')
@click.option('--rules', help='Comma-separated list of rules to run')
@click.option('--output', help='Output file for quality report')
@click.pass_context
def quality_check(ctx, table, rules, output):
    """Run data quality checks on a table"""
    
    try:
        print_info(f"Running quality checks for table: {table}")
        
        # Initialize quality framework
        quality_framework = DataQualityFramework()
        
        # For demo purposes, create sample data
        # In production, this would load actual table data
        import pandas as pd
        sample_data = pd.DataFrame({
            'id': range(1, 101),
            'email': [f'user{i}@example.com' for i in range(1, 101)],
            'phone': [f'+1-555-{i:04d}' for i in range(1, 101)],
            'age': [20 + (i % 60) for i in range(1, 101)]
        })
        
        # Determine rules to run
        rules_to_run = None
        if rules:
            rules_to_run = [r.strip() for r in rules.split(',')]
        
        # Execute quality checks
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
            task = progress.add_task("Running quality checks...", total=None)
            
            quality_report = quality_framework.execute_quality_checks(
                data=sample_data,
                table_name=table,
                layer='bronze',
                rules_to_run=rules_to_run
            )
            
            progress.update(task, description="Quality checks complete")
        
        # Display results
        quality_table = Table(title="Quality Check Results")
        quality_table.add_column("Rule", style="cyan")
        quality_table.add_column("Type", style="magenta")
        quality_table.add_column("Status", style="bold")
        quality_table.add_column("Pass Rate", style="green")
        quality_table.add_column("Failed Records", style="red")
        
        for result in quality_report.rule_results:
            status_style = "green" if result.is_passed else "red"
            status_text = "PASS" if result.is_passed else "FAIL"
            
            quality_table.add_row(
                result.rule_name,
                result.rule_type.value.title(),
                f"[{status_style}]{status_text}[/{status_style}]",
                f"{result.pass_rate:.1%}",
                str(result.failed_records)
            )
        
        console.print(quality_table)
        
        # Overall quality score
        score_color = "green" if quality_report.overall_quality_score >= 85 else "yellow" if quality_report.overall_quality_score >= 70 else "red"
        console.print(f"\n📊 Overall Quality Score: [{score_color}]{quality_report.overall_quality_score:.1f}/100[/{score_color}]")
        
        # Show recommendations
        if quality_report.recommendations:
            console.print("\n💡 Recommendations:")
            for rec in quality_report.recommendations:
                console.print(f"  {rec}")
        
        # Save report if requested
        if output:
            report_data = {
                'table_name': quality_report.table_name,
                'execution_timestamp': quality_report.execution_timestamp.isoformat(),
                'overall_quality_score': quality_report.overall_quality_score,
                'total_records': quality_report.total_records,
                'rules_passed': quality_report.rules_passed,
                'rules_failed': quality_report.rules_failed,
                'recommendations': quality_report.recommendations,
                'rule_results': [
                    {
                        'rule_name': r.rule_name,
                        'rule_type': r.rule_type.value,
                        'is_passed': r.is_passed,
                        'pass_rate': r.pass_rate,
                        'failed_records': r.failed_records,
                        'error_message': r.error_message
                    }
                    for r in quality_report.rule_results
                ]
            }
            
            with open(output, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            print_success(f"Quality report saved to: {output}")
    
    except Exception as e:
        print_error(f"Quality check failed: {str(e)}")
        if ctx.obj.verbose:
            import traceback
            console.print(traceback.format_exc())
        sys.exit(1)


# ==========================================
# MONITORING COMMANDS
# ==========================================

@cli.group()
def monitor():
    """📊 Monitoring and health check commands"""
    pass


@monitor.command('health')
@click.pass_context
def monitor_health(ctx):
    """Check system health and engine status"""
    
    try:
        print_info("🔍 Performing system health check...")
        
        if not ctx.obj.engine:
            if not ctx.obj.config_path:
                print_error("Configuration required. Use --config option")
                sys.exit(1)
            ctx.obj.engine = UnifiedETLEngine(ctx.obj.config_path)
        
        # Check engine health
        health_results = {}
        for engine_name in ctx.obj.engine.engines:
            try:
                engine = ctx.obj.engine.engines[engine_name]
                health = engine.health_check()
                health_results[engine_name] = health
            except Exception as e:
                health_results[engine_name] = {'status': 'error', 'error': str(e)}
        
        # Display health status
        health_table = Table(title="System Health Status")
        health_table.add_column("Component", style="cyan")
        health_table.add_column("Status", style="bold")
        health_table.add_column("Details", style="blue")
        
        for engine_name, health in health_results.items():
            status = health.get('status', 'unknown')
            status_style = "green" if status == 'healthy' else "red"
            
            details = []
            if 'total_jobs_processed' in health:
                details.append(f"Jobs: {health['total_jobs_processed']}")
            if 'average_processing_time' in health:
                details.append(f"Avg Time: {health['average_processing_time']:.1f}s")
            
            health_table.add_row(
                f"{engine_name.title()} Engine",
                f"[{status_style}]{status.upper()}[/{status_style}]",
                " | ".join(details) if details else health.get('error', 'No details')
            )
        
        console.print(health_table)
        
        # Overall system status
        healthy_engines = sum(1 for h in health_results.values() if h.get('status') == 'healthy')
        total_engines = len(health_results)
        
        if healthy_engines == total_engines:
            print_success("All systems healthy")
        else:
            print_warning(f"{healthy_engines}/{total_engines} engines healthy")
    
    except Exception as e:
        print_error(f"Health check failed: {str(e)}")
        sys.exit(1)


# ==========================================
# UTILITY COMMANDS
# ==========================================

@cli.command('version')
def version():
    """Show ETL Framework version"""
    try:
        # Try to get version from package
        import pkg_resources
        version = pkg_resources.get_distribution('etl-framework').version
    except:
        version = "development"
    
    console.print(Panel(
        f"🚀 ETL Framework v{version}\n"
        f"Production-Ready Metadata-Driven Data Pipeline\n"
        f"Supporting SQL, Python, and PySpark engines",
        title="Version Information",
        border_style="blue"
    ))


@cli.command('docs')
def docs():
    """Open documentation"""
    print_info("📚 Documentation available at: https://etl-framework.readthedocs.io")
    print_info("💡 For quick help on any command, use: etl-cli COMMAND --help")


if __name__ == '__main__':
    cli()
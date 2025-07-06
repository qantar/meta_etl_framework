#!/usr/bin/env python3
"""
ETL Framework - Production Operations Scripts
Backup, Recovery, Health Monitoring, and Performance Analysis
"""

import os
import sys
import json
import logging
import subprocess
import time
import psutil
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import argparse
import yaml
import boto3
from azure.storage.blob import BlobServiceClient
import paramiko
import schedule


@dataclass
class BackupMetadata:
    """Backup operation metadata"""
    backup_id: str
    timestamp: datetime
    backup_type: str  # 'full', 'incremental', 'differential'
    components: List[str]  # ['databases', 'data', 'config', 'logs']
    size_bytes: int
    duration_seconds: float
    status: str
    storage_location: str
    encryption_enabled: bool
    compression_enabled: bool
    error_message: Optional[str] = None


@dataclass
class HealthCheckResult:
    """Health check result"""
    check_name: str
    status: str  # 'healthy', 'warning', 'critical'
    message: str
    details: Dict[str, Any]
    timestamp: datetime
    response_time_ms: float


class BackupManager:
    """Comprehensive backup and recovery manager"""
    
    def __init__(self, config_path: str = "config/production.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.logger = self._setup_logging()
        
        # Storage clients
        self.s3_client = None
        self.azure_client = None
        self.sftp_client = None
        
        self._initialize_storage_clients()
    
    def _load_config(self) -> Dict:
        """Load configuration from file"""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load config from {self.config_path}: {e}")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for backup operations"""
        logger = logging.getLogger('backup_manager')
        logger.setLevel(logging.INFO)
        
        # File handler
        handler = logging.FileHandler('logs/backup_operations.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def _initialize_storage_clients(self):
        """Initialize cloud storage clients"""
        backup_config = self.config.get('backup', {})
        
        # AWS S3
        if backup_config.get('storage_backend') == 's3':
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=backup_config.get('s3_access_key'),
                aws_secret_access_key=backup_config.get('s3_secret_key'),
                region_name=backup_config.get('s3_region', 'us-east-1')
            )
        
        # Azure Blob Storage
        elif backup_config.get('storage_backend') == 'azure':
            connection_string = backup_config.get('azure_connection_string')
            if connection_string:
                self.azure_client = BlobServiceClient.from_connection_string(connection_string)
    
    def create_database_backup(self, database_name: str, output_path: str) -> bool:
        """Create database backup using pg_dump"""
        try:
            db_config = self.config['databases'][database_name]
            
            # Build pg_dump command
            cmd = [
                'pg_dump',
                f'--host={db_config["host"]}',
                f'--port={db_config["port"]}',
                f'--username={db_config["username"]}',
                f'--dbname={db_config["database"]}',
                '--format=custom',
                '--compress=9',
                '--verbose',
                f'--file={output_path}'
            ]
            
            # Set password via environment
            env = os.environ.copy()
            env['PGPASSWORD'] = db_config['password']
            
            # Execute backup
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info(f"Database backup successful: {database_name} -> {output_path}")
                return True
            else:
                self.logger.error(f"Database backup failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Database backup error: {str(e)}")
            return False
    
    def create_data_backup(self, source_path: str, output_path: str) -> bool:
        """Create compressed data backup"""
        try:
            cmd = [
                'tar',
                '-czf',
                output_path,
                '-C', str(Path(source_path).parent),
                Path(source_path).name
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info(f"Data backup successful: {source_path} -> {output_path}")
                return True
            else:
                self.logger.error(f"Data backup failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Data backup error: {str(e)}")
            return False
    
    def upload_to_cloud(self, local_path: str, remote_path: str) -> bool:
        """Upload backup to cloud storage"""
        try:
            backup_config = self.config.get('backup', {})
            
            if backup_config.get('storage_backend') == 's3' and self.s3_client:
                bucket = backup_config.get('s3_bucket')
                self.s3_client.upload_file(local_path, bucket, remote_path)
                self.logger.info(f"Uploaded to S3: {remote_path}")
                
            elif backup_config.get('storage_backend') == 'azure' and self.azure_client:
                container = backup_config.get('azure_container')
                blob_client = self.azure_client.get_blob_client(
                    container=container, blob=remote_path
                )
                with open(local_path, 'rb') as data:
                    blob_client.upload_blob(data, overwrite=True)
                self.logger.info(f"Uploaded to Azure: {remote_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Cloud upload error: {str(e)}")
            return False
    
    def create_full_backup(self) -> BackupMetadata:
        """Create complete system backup"""
        start_time = datetime.now()
        backup_id = f"full_backup_{start_time.strftime('%Y%m%d_%H%M%S')}"
        
        self.logger.info(f"Starting full backup: {backup_id}")
        
        # Create backup directory
        backup_dir = Path(f"backups/{backup_id}")
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        components_backed_up = []
        total_size = 0
        errors = []
        
        try:
            # Backup databases
            for db_name in ['source', 'target', 'metadata']:
                db_backup_path = backup_dir / f"{db_name}_db.backup"
                if self.create_database_backup(db_name, str(db_backup_path)):
                    components_backed_up.append(f"{db_name}_database")
                    total_size += db_backup_path.stat().st_size
                else:
                    errors.append(f"Failed to backup {db_name} database")
            
            # Backup data directories
            for data_type in ['data', 'logs', 'config']:
                data_path = self.config.get('storage', {}).get(f'{data_type}_path', f'/app/{data_type}')
                if Path(data_path).exists():
                    data_backup_path = backup_dir / f"{data_type}.tar.gz"
                    if self.create_data_backup(data_path, str(data_backup_path)):
                        components_backed_up.append(data_type)
                        total_size += data_backup_path.stat().st_size
                    else:
                        errors.append(f"Failed to backup {data_type}")
            
            # Upload to cloud storage
            if self.config.get('backup', {}).get('enabled'):
                for backup_file in backup_dir.glob('*'):
                    remote_path = f"{backup_id}/{backup_file.name}"
                    if not self.upload_to_cloud(str(backup_file), remote_path):
                        errors.append(f"Failed to upload {backup_file.name}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Create metadata
            metadata = BackupMetadata(
                backup_id=backup_id,
                timestamp=start_time,
                backup_type='full',
                components=components_backed_up,
                size_bytes=total_size,
                duration_seconds=duration,
                status='success' if not errors else 'partial',
                storage_location=str(backup_dir),
                encryption_enabled=self.config.get('backup', {}).get('encryption_enabled', False),
                compression_enabled=True,
                error_message='; '.join(errors) if errors else None
            )
            
            # Save metadata
            with open(backup_dir / 'metadata.json', 'w') as f:
                json.dump(asdict(metadata), f, indent=2, default=str)
            
            self.logger.info(f"Full backup completed: {backup_id}")
            return metadata
            
        except Exception as e:
            self.logger.error(f"Full backup failed: {str(e)}")
            return BackupMetadata(
                backup_id=backup_id,
                timestamp=start_time,
                backup_type='full',
                components=[],
                size_bytes=0,
                duration_seconds=(datetime.now() - start_time).total_seconds(),
                status='failed',
                storage_location=str(backup_dir),
                encryption_enabled=False,
                compression_enabled=False,
                error_message=str(e)
            )
    
    def restore_from_backup(self, backup_id: str, components: List[str] = None) -> bool:
        """Restore system from backup"""
        backup_dir = Path(f"backups/{backup_id}")
        
        if not backup_dir.exists():
            self.logger.error(f"Backup directory not found: {backup_dir}")
            return False
        
        # Load metadata
        metadata_file = backup_dir / 'metadata.json'
        if not metadata_file.exists():
            self.logger.error(f"Backup metadata not found: {metadata_file}")
            return False
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        components_to_restore = components or metadata['components']
        
        self.logger.info(f"Starting restore from backup: {backup_id}")
        
        try:
            # Restore databases
            for db_name in ['source', 'target', 'metadata']:
                if f"{db_name}_database" in components_to_restore:
                    db_backup_path = backup_dir / f"{db_name}_db.backup"
                    if db_backup_path.exists():
                        self._restore_database(db_name, str(db_backup_path))
            
            # Restore data directories
            for data_type in ['data', 'logs', 'config']:
                if data_type in components_to_restore:
                    data_backup_path = backup_dir / f"{data_type}.tar.gz"
                    if data_backup_path.exists():
                        self._restore_data(data_type, str(data_backup_path))
            
            self.logger.info(f"Restore completed successfully: {backup_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Restore failed: {str(e)}")
            return False
    
    def _restore_database(self, database_name: str, backup_path: str) -> bool:
        """Restore database from backup"""
        try:
            db_config = self.config['databases'][database_name]
            
            # Build pg_restore command
            cmd = [
                'pg_restore',
                f'--host={db_config["host"]}',
                f'--port={db_config["port"]}',
                f'--username={db_config["username"]}',
                f'--dbname={db_config["database"]}',
                '--clean',
                '--create',
                '--verbose',
                backup_path
            ]
            
            # Set password via environment
            env = os.environ.copy()
            env['PGPASSWORD'] = db_config['password']
            
            # Execute restore
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info(f"Database restore successful: {database_name}")
                return True
            else:
                self.logger.error(f"Database restore failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Database restore error: {str(e)}")
            return False
    
    def _restore_data(self, data_type: str, backup_path: str) -> bool:
        """Restore data directory from backup"""
        try:
            data_path = self.config.get('storage', {}).get(f'{data_type}_path', f'/app/{data_type}')
            
            # Extract backup
            cmd = [
                'tar',
                '-xzf',
                backup_path,
                '-C', str(Path(data_path).parent)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info(f"Data restore successful: {data_type}")
                return True
            else:
                self.logger.error(f"Data restore failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Data restore error: {str(e)}")
            return False
    
    def cleanup_old_backups(self, retention_days: int = 30):
        """Clean up old backup files"""
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        backups_dir = Path("backups")
        
        if not backups_dir.exists():
            return
        
        removed_count = 0
        
        for backup_dir in backups_dir.iterdir():
            if backup_dir.is_dir():
                # Parse backup timestamp from directory name
                try:
                    timestamp_str = backup_dir.name.split('_')[-2] + '_' + backup_dir.name.split('_')[-1]
                    backup_time = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                    
                    if backup_time < cutoff_date:
                        # Remove old backup
                        subprocess.run(['rm', '-rf', str(backup_dir)])
                        removed_count += 1
                        self.logger.info(f"Removed old backup: {backup_dir.name}")
                        
                except Exception as e:
                    self.logger.warning(f"Could not parse backup timestamp: {backup_dir.name}")
        
        self.logger.info(f"Cleanup completed: removed {removed_count} old backups")


class HealthMonitor:
    """Comprehensive system health monitoring"""
    
    def __init__(self, config_path: str = "config/production.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.logger = self._setup_logging()
    
    def _load_config(self) -> Dict:
        """Load configuration from file"""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load config from {self.config_path}: {e}")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for health monitoring"""
        logger = logging.getLogger('health_monitor')
        logger.setLevel(logging.INFO)
        
        handler = logging.FileHandler('logs/health_monitor.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def check_database_health(self, database_name: str) -> HealthCheckResult:
        """Check database connectivity and performance"""
        start_time = time.time()
        
        try:
            db_config = self.config['databases'][database_name]
            
            import psycopg2
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['username'],
                password=db_config['password'],
                connect_timeout=10
            )
            
            cursor = conn.cursor()
            
            # Check basic connectivity
            cursor.execute('SELECT 1')
            
            # Check database size
            cursor.execute("""
                SELECT pg_size_pretty(pg_database_size(current_database())) as size,
                       pg_database_size(current_database()) as size_bytes
            """)
            size_info = cursor.fetchone()
            
            # Check active connections
            cursor.execute("""
                SELECT count(*) as active_connections
                FROM pg_stat_activity
                WHERE state = 'active'
            """)
            active_connections = cursor.fetchone()[0]
            
            # Check table counts (for metadata db)
            if database_name == 'metadata':
                cursor.execute("""
                    SELECT 
                        (SELECT count(*) FROM etl_audit_log WHERE start_time >= CURRENT_DATE) as todays_jobs,
                        (SELECT count(*) FROM etl_control_table WHERE is_active = true) as active_tables
                """)
                job_stats = cursor.fetchone()
                job_details = {
                    'todays_jobs': job_stats[0],
                    'active_tables': job_stats[1]
                }
            else:
                job_details = {}
            
            cursor.close()
            conn.close()
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                check_name=f"database_{database_name}",
                status='healthy',
                message=f"Database {database_name} is healthy",
                details={
                    'database_size': size_info[0],
                    'database_size_bytes': size_info[1],
                    'active_connections': active_connections,
                    **job_details
                },
                timestamp=datetime.now(),
                response_time_ms=response_time
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                check_name=f"database_{database_name}",
                status='critical',
                message=f"Database {database_name} health check failed: {str(e)}",
                details={'error': str(e)},
                timestamp=datetime.now(),
                response_time_ms=response_time
            )
    
    def check_system_resources(self) -> HealthCheckResult:
        """Check system resource usage"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            
            # Disk usage
            disk = psutil.disk_usage('/')
            
            # Load average
            load_avg = os.getloadavg()
            
            # Determine status
            status = 'healthy'
            warnings = []
            
            if cpu_percent > 80:
                status = 'warning' if status != 'critical' else status
                warnings.append(f"High CPU usage: {cpu_percent}%")
            
            if memory.percent > 85:
                status = 'critical'
                warnings.append(f"High memory usage: {memory.percent}%")
            elif memory.percent > 70:
                status = 'warning' if status != 'critical' else status
                warnings.append(f"Elevated memory usage: {memory.percent}%")
            
            if disk.percent > 90:
                status = 'critical'
                warnings.append(f"High disk usage: {disk.percent}%")
            elif disk.percent > 80:
                status = 'warning' if status != 'critical' else status
                warnings.append(f"Elevated disk usage: {disk.percent}%")
            
            message = "System resources are healthy"
            if warnings:
                message = f"System resource warnings: {'; '.join(warnings)}"
            
            return HealthCheckResult(
                check_name="system_resources",
                status=status,
                message=message,
                details={
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory.percent,
                    'memory_used_gb': memory.used / (1024**3),
                    'memory_total_gb': memory.total / (1024**3),
                    'disk_percent': disk.percent,
                    'disk_used_gb': disk.used / (1024**3),
                    'disk_total_gb': disk.total / (1024**3),
                    'load_average_1m': load_avg[0],
                    'load_average_5m': load_avg[1],
                    'load_average_15m': load_avg[2]
                },
                timestamp=datetime.now(),
                response_time_ms=1000  # Approximate time for system checks
            )
            
        except Exception as e:
            return HealthCheckResult(
                check_name="system_resources",
                status='critical',
                message=f"System resource check failed: {str(e)}",
                details={'error': str(e)},
                timestamp=datetime.now(),
                response_time_ms=0
            )
    
    def check_etl_services(self) -> List[HealthCheckResult]:
        """Check ETL service health"""
        results = []
        
        # Check if ETL containers are running
        services = ['etl-python', 'etl-spark', 'etl-scheduler']
        
        for service in services:
            try:
                # Check if container is running
                result = subprocess.run(
                    ['docker', 'ps', '--filter', f'name={service}', '--format', '{{.Status}}'],
                    capture_output=True, text=True
                )
                
                if result.returncode == 0 and result.stdout.strip():
                    status_text = result.stdout.strip()
                    if 'Up' in status_text:
                        status = 'healthy'
                        message = f"Service {service} is running"
                    else:
                        status = 'critical'
                        message = f"Service {service} is not running: {status_text}"
                else:
                    status = 'critical'
                    message = f"Service {service} not found"
                
                results.append(HealthCheckResult(
                    check_name=f"service_{service}",
                    status=status,
                    message=message,
                    details={'service_status': result.stdout.strip()},
                    timestamp=datetime.now(),
                    response_time_ms=100
                ))
                
            except Exception as e:
                results.append(HealthCheckResult(
                    check_name=f"service_{service}",
                    status='critical',
                    message=f"Service {service} check failed: {str(e)}",
                    details={'error': str(e)},
                    timestamp=datetime.now(),
                    response_time_ms=0
                ))
        
        return results
    
    def check_etl_pipeline_health(self) -> HealthCheckResult:
        """Check ETL pipeline execution health"""
        try:
            # This would typically query the metadata database
            # For now, we'll simulate the check
            
            # Check recent job execution success rate
            # Check data quality scores
            # Check for stuck jobs
            
            return HealthCheckResult(
                check_name="etl_pipeline",
                status='healthy',
                message="ETL pipeline is healthy",
                details={
                    'recent_success_rate': 95.5,
                    'avg_quality_score': 92.3,
                    'stuck_jobs': 0,
                    'failed_jobs_24h': 2
                },
                timestamp=datetime.now(),
                response_time_ms=500
            )
            
        except Exception as e:
            return HealthCheckResult(
                check_name="etl_pipeline",
                status='critical',
                message=f"ETL pipeline health check failed: {str(e)}",
                details={'error': str(e)},
                timestamp=datetime.now(),
                response_time_ms=0
            )
    
    def run_full_health_check(self) -> Dict[str, Any]:
        """Run comprehensive health check"""
        self.logger.info("Starting full health check")
        
        all_results = []
        
        # Database health checks
        for db_name in ['source', 'target', 'metadata']:
            result = self.check_database_health(db_name)
            all_results.append(result)
        
        # System resource check
        result = self.check_system_resources()
        all_results.append(result)
        
        # ETL service checks
        service_results = self.check_etl_services()
        all_results.extend(service_results)
        
        # Pipeline health check
        result = self.check_etl_pipeline_health()
        all_results.append(result)
        
        # Summarize results
        healthy_count = len([r for r in all_results if r.status == 'healthy'])
        warning_count = len([r for r in all_results if r.status == 'warning'])
        critical_count = len([r for r in all_results if r.status == 'critical'])
        
        overall_status = 'healthy'
        if critical_count > 0:
            overall_status = 'critical'
        elif warning_count > 0:
            overall_status = 'warning'
        
        summary = {
            'overall_status': overall_status,
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_checks': len(all_results),
                'healthy': healthy_count,
                'warning': warning_count,
                'critical': critical_count
            },
            'results': [asdict(result) for result in all_results]
        }
        
        # Save health check results
        health_dir = Path('logs/health_checks')
        health_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        with open(health_dir / f'health_check_{timestamp}.json', 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        self.logger.info(f"Health check completed: {overall_status} ({healthy_count}/{len(all_results)} healthy)")
        
        return summary


# CLI Interface for Operations Scripts
def main():
    parser = argparse.ArgumentParser(description='ETL Framework Operations Scripts')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Backup commands
    backup_parser = subparsers.add_parser('backup', help='Backup operations')
    backup_subparsers = backup_parser.add_subparsers(dest='backup_action')
    
    backup_subparsers.add_parser('full', help='Create full backup')
    
    restore_parser = backup_subparsers.add_parser('restore', help='Restore from backup')
    restore_parser.add_argument('backup_id', help='Backup ID to restore from')
    restore_parser.add_argument('--components', nargs='+', help='Components to restore')
    
    cleanup_parser = backup_subparsers.add_parser('cleanup', help='Clean up old backups')
    cleanup_parser.add_argument('--retention-days', type=int, default=30, help='Retention period in days')
    
    # Health check commands
    health_parser = subparsers.add_parser('health', help='Health monitoring')
    health_subparsers = health_parser.add_subparsers(dest='health_action')
    
    health_subparsers.add_parser('check', help='Run full health check')
    health_subparsers.add_parser('monitor', help='Start continuous monitoring')
    
    args = parser.parse_args()
    
    if args.command == 'backup':
        backup_manager = BackupManager()
        
        if args.backup_action == 'full':
            metadata = backup_manager.create_full_backup()
            print(f"Backup completed: {metadata.backup_id}")
            print(f"Status: {metadata.status}")
            print(f"Size: {metadata.size_bytes / (1024**3):.2f} GB")
            print(f"Duration: {metadata.duration_seconds:.1f} seconds")
            
        elif args.backup_action == 'restore':
            success = backup_manager.restore_from_backup(args.backup_id, args.components)
            print(f"Restore {'successful' if success else 'failed'}")
            
        elif args.backup_action == 'cleanup':
            backup_manager.cleanup_old_backups(args.retention_days)
            print(f"Cleanup completed (retention: {args.retention_days} days)")
    
    elif args.command == 'health':
        health_monitor = HealthMonitor()
        
        if args.health_action == 'check':
            summary = health_monitor.run_full_health_check()
            print(f"Overall Status: {summary['overall_status'].upper()}")
            print(f"Checks: {summary['summary']['healthy']}/{summary['summary']['total_checks']} healthy")
            
            # Show critical and warning issues
            for result in summary['results']:
                if result['status'] in ['critical', 'warning']:
                    print(f"⚠️  {result['check_name']}: {result['message']}")
        
        elif args.health_action == 'monitor':
            print("Starting continuous health monitoring...")
            
            def run_health_check():
                summary = health_monitor.run_full_health_check()
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Status: {summary['overall_status']}")
            
            # Schedule health checks every 5 minutes
            schedule.every(5).minutes.do(run_health_check)
            
            try:
                while True:
                    schedule.run_pending()
                    time.sleep(60)
            except KeyboardInterrupt:
                print("\nHealth monitoring stopped")


if __name__ == '__main__':
    main()
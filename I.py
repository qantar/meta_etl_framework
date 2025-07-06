"""
Job Scheduler & Orchestrator for ETL Framework
Handles cron scheduling, job dependencies, retry logic, and workflow orchestration
"""

import asyncio
import logging
import json
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, asdict, field
from enum import Enum
from pathlib import Path
import croniter
import networkx as nx
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import signal
import sys

# Import our core modules
from unified_engine import UnifiedETLEngine
from notification_service import NotificationService, NotificationContext, NotificationLevel, NotificationType
from control_audit_system import ControlAuditManager, LoadStatus


class JobStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"
    RETRY = "RETRY"


class TriggerType(Enum):
    CRON = "cron"
    INTERVAL = "interval"  
    MANUAL = "manual"
    DEPENDENCY = "dependency"
    EVENT = "event"


@dataclass
class RetryConfig:
    """Retry configuration for failed jobs"""
    enabled: bool = True
    max_attempts: int = 3
    base_delay: int = 60  # seconds
    exponential_backoff: bool = True
    backoff_multiplier: float = 2.0
    max_delay: int = 3600  # 1 hour max
    retry_on_failures: List[str] = field(default_factory=lambda: ["FAILED", "TIMEOUT"])


@dataclass
class JobDefinition:
    """Complete job definition with scheduling and dependencies"""
    job_id: str
    name: str
    description: str = ""
    
    # Execution details
    engine_type: str = "python"
    pipeline_name: str = ""
    table_filter: Optional[str] = None  # SQL-like filter for tables
    custom_config: Dict[str, Any] = field(default_factory=dict)
    
    # Scheduling
    trigger_type: TriggerType = TriggerType.MANUAL
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)  # Job IDs this job depends on
    
    # Retry and timeout
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    timeout_seconds: int = 3600  # 1 hour default
    
    # Execution constraints
    enabled: bool = True
    max_parallel_instances: int = 1
    priority: int = 50  # 0-100, higher = more priority
    
    # Notification settings
    notify_on_success: bool = False
    notify_on_failure: bool = True
    notification_channels: List[str] = field(default_factory=lambda: ["email", "slack"])
    
    # Environment
    environment_vars: Dict[str, str] = field(default_factory=dict)
    required_resources: Dict[str, Any] = field(default_factory=dict)


@dataclass
class JobExecution:
    """Runtime job execution state"""
    job_id: str
    execution_id: str
    status: JobStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    attempt_number: int = 1
    error_message: Optional[str] = None
    output: Optional[Dict[str, Any]] = None
    triggered_by: str = "scheduler"
    next_retry_time: Optional[datetime] = None
    
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now() - self.start_time).total_seconds()


class DependencyManager:
    """Manages job dependencies and execution order"""
    
    def __init__(self):
        self.dependency_graph = nx.DiGraph()
        self.logger = logging.getLogger(__name__)
    
    def add_job(self, job_def: JobDefinition):
        """Add job to dependency graph"""
        self.dependency_graph.add_node(job_def.job_id, job=job_def)
        
        # Add dependency edges
        for dependency in job_def.depends_on:
            self.dependency_graph.add_edge(dependency, job_def.job_id)
    
    def remove_job(self, job_id: str):
        """Remove job from dependency graph"""
        if job_id in self.dependency_graph:
            self.dependency_graph.remove_node(job_id)
    
    def get_execution_order(self, job_ids: List[str] = None) -> List[List[str]]:
        """Get topological execution order for jobs"""
        
        if job_ids:
            # Get subgraph for specific jobs
            subgraph = self.dependency_graph.subgraph(job_ids)
        else:
            subgraph = self.dependency_graph
        
        try:
            # Get topological sort - returns levels that can be executed in parallel
            return list(nx.topological_generations(subgraph))
        except nx.NetworkXError as e:
            if "cycle" in str(e).lower():
                cycles = list(nx.simple_cycles(subgraph))
                raise ValueError(f"Circular dependency detected: {cycles}")
            raise
    
    def get_ready_jobs(self, completed_jobs: Set[str], failed_jobs: Set[str]) -> List[str]:
        """Get jobs that are ready to run (all dependencies satisfied)"""
        
        ready = []
        
        for job_id in self.dependency_graph.nodes():
            # Skip if already completed or failed
            if job_id in completed_jobs or job_id in failed_jobs:
                continue
            
            # Check if all dependencies are satisfied
            dependencies = list(self.dependency_graph.predecessors(job_id))
            
            if all(dep in completed_jobs for dep in dependencies):
                ready.append(job_id)
        
        return ready
    
    def validate_dependencies(self) -> List[str]:
        """Validate dependency graph for issues"""
        
        issues = []
        
        # Check for circular dependencies
        try:
            cycles = list(nx.simple_cycles(self.dependency_graph))
            if cycles:
                issues.append(f"Circular dependencies found: {cycles}")
        except Exception:
            pass
        
        # Check for missing dependencies
        for job_id in self.dependency_graph.nodes():
            job_def = self.dependency_graph.nodes[job_id]['job']
            for dep in job_def.depends_on:
                if dep not in self.dependency_graph.nodes():
                    issues.append(f"Job {job_id} depends on non-existent job {dep}")
        
        return issues


class JobScheduler:
    """
    Advanced job scheduler with cron support, dependency management, and retry logic
    """
    
    def __init__(self, etl_engine: UnifiedETLEngine, notification_service: NotificationService = None,
                 config_dir: str = "scheduler_config", max_workers: int = 10):
        
        self.etl_engine = etl_engine
        self.notification_service = notification_service
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        
        # Core components
        self.dependency_manager = DependencyManager()
        self.job_definitions: Dict[str, JobDefinition] = {}
        self.job_executions: Dict[str, JobExecution] = {}
        self.running_jobs: Dict[str, asyncio.Task] = {}
        
        # Execution control
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.scheduler_running = False
        self.scheduler_task: Optional[asyncio.Task] = None
        
        # State tracking
        self.completed_jobs: Set[str] = set()
        self.failed_jobs: Set[str] = set()
        self.last_cron_check = datetime.now()
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Load existing job definitions
        self._load_job_definitions()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info(f"Job scheduler initialized with {len(self.job_definitions)} jobs")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        asyncio.create_task(self.stop())
    
    def add_job(self, job_def: JobDefinition) -> bool:
        """Add a job definition to the scheduler"""
        
        try:
            # Validate job definition
            self._validate_job_definition(job_def)
            
            # Add to collections
            self.job_definitions[job_def.job_id] = job_def
            self.dependency_manager.add_job(job_def)
            
            # Save to disk
            self._save_job_definition(job_def)
            
            self.logger.info(f"Added job: {job_def.job_id} ({job_def.name})")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add job {job_def.job_id}: {str(e)}")
            return False
    
    def remove_job(self, job_id: str) -> bool:
        """Remove a job from the scheduler"""
        
        try:
            # Cancel if running
            if job_id in self.running_jobs:
                self.running_jobs[job_id].cancel()
                del self.running_jobs[job_id]
            
            # Remove from collections
            if job_id in self.job_definitions:
                del self.job_definitions[job_id]
            
            self.dependency_manager.remove_job(job_id)
            
            # Remove from disk
            job_file = self.config_dir / f"{job_id}.yaml"
            if job_file.exists():
                job_file.unlink()
            
            self.logger.info(f"Removed job: {job_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to remove job {job_id}: {str(e)}")
            return False
    
    def enable_job(self, job_id: str) -> bool:
        """Enable a job"""
        if job_id in self.job_definitions:
            self.job_definitions[job_id].enabled = True
            self._save_job_definition(self.job_definitions[job_id])
            self.logger.info(f"Enabled job: {job_id}")
            return True
        return False
    
    def disable_job(self, job_id: str) -> bool:
        """Disable a job"""
        if job_id in self.job_definitions:
            self.job_definitions[job_id].enabled = False
            self._save_job_definition(self.job_definitions[job_id])
            self.logger.info(f"Disabled job: {job_id}")
            return True
        return False
    
    def _validate_job_definition(self, job_def: JobDefinition):
        """Validate job definition"""
        
        if not job_def.job_id or not job_def.name:
            raise ValueError("Job ID and name are required")
        
        if job_def.trigger_type == TriggerType.CRON and not job_def.cron_expression:
            raise ValueError("Cron expression required for cron trigger")
        
        if job_def.trigger_type == TriggerType.INTERVAL and not job_def.interval_seconds:
            raise ValueError("Interval seconds required for interval trigger")
        
        # Validate cron expression
        if job_def.cron_expression:
            try:
                croniter.croniter(job_def.cron_expression)
            except Exception as e:
                raise ValueError(f"Invalid cron expression: {e}")
        
        # Validate dependencies exist
        for dep in job_def.depends_on:
            if dep not in self.job_definitions and dep != job_def.job_id:
                self.logger.warning(f"Job {job_def.job_id} depends on non-existent job {dep}")
    
    def _save_job_definition(self, job_def: JobDefinition):
        """Save job definition to disk"""
        
        job_file = self.config_dir / f"{job_def.job_id}.yaml"
        with open(job_file, 'w') as f:
            yaml.dump(asdict(job_def), f, default_flow_style=False)
    
    def _load_job_definitions(self):
        """Load job definitions from disk"""
        
        for job_file in self.config_dir.glob("*.yaml"):
            try:
                with open(job_file, 'r') as f:
                    job_data = yaml.safe_load(f)
                
                # Convert back to JobDefinition
                job_def = JobDefinition(**job_data)
                self.job_definitions[job_def.job_id] = job_def
                self.dependency_manager.add_job(job_def)
                
            except Exception as e:
                self.logger.error(f"Failed to load job definition {job_file}: {str(e)}")
    
    async def start(self):
        """Start the scheduler"""
        
        if self.scheduler_running:
            self.logger.warning("Scheduler is already running")
            return
        
        self.scheduler_running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        
        self.logger.info("Job scheduler started")
        
        # Send notification
        if self.notification_service:
            self.notification_service.notify_system_health(
                "Scheduler Started", 
                {"jobs_loaded": len(self.job_definitions)},
                NotificationLevel.INFO
            )
    
    async def stop(self):
        """Stop the scheduler gracefully"""
        
        self.logger.info("Stopping scheduler...")
        self.scheduler_running = False
        
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        
        # Cancel running jobs
        for job_id, task in self.running_jobs.items():
            self.logger.info(f"Cancelling running job: {job_id}")
            task.cancel()
        
        # Wait for jobs to complete with timeout
        if self.running_jobs:
            await asyncio.wait(self.running_jobs.values(), timeout=30)
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        self.logger.info("Scheduler stopped")
        
        # Send notification
        if self.notification_service:
            self.notification_service.notify_system_health(
                "Scheduler Stopped", 
                {"running_jobs_cancelled": len(self.running_jobs)},
                NotificationLevel.WARNING
            )
    
    async def _scheduler_loop(self):
        """Main scheduler loop"""
        
        while self.scheduler_running:
            try:
                # Check for jobs that should be triggered
                await self._check_scheduled_jobs()
                
                # Check for dependency-based jobs
                await self._check_dependency_jobs()
                
                # Check for retry jobs
                await self._check_retry_jobs()
                
                # Clean up completed jobs
                self._cleanup_completed_jobs()
                
                # Health check
                await self._scheduler_health_check()
                
                # Sleep before next iteration
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Scheduler loop error: {str(e)}")
                await asyncio.sleep(30)  # Wait longer on error
    
    async def _check_scheduled_jobs(self):
        """Check for jobs that should be triggered by schedule"""
        
        now = datetime.now()
        
        for job_id, job_def in self.job_definitions.items():
            if not job_def.enabled:
                continue
            
            # Skip if already running and max instances reached
            running_count = len([j for j in self.running_jobs.values() 
                               if j.get_name() == job_id])
            if running_count >= job_def.max_parallel_instances:
                continue
            
            should_trigger = False
            
            # Check cron schedule
            if job_def.trigger_type == TriggerType.CRON and job_def.cron_expression:
                cron = croniter.croniter(job_def.cron_expression, self.last_cron_check)
                next_run = cron.get_next(datetime)
                
                if next_run <= now:
                    should_trigger = True
            
            # Check interval schedule
            elif job_def.trigger_type == TriggerType.INTERVAL and job_def.interval_seconds:
                # Get last execution time for this job
                last_execution = self._get_last_execution_time(job_id)
                if not last_execution or (now - last_execution).total_seconds() >= job_def.interval_seconds:
                    should_trigger = True
            
            if should_trigger:
                await self._trigger_job(job_id, "scheduler")
        
        self.last_cron_check = now
    
    async def _check_dependency_jobs(self):
        """Check for jobs ready to run based on dependencies"""
        
        ready_jobs = self.dependency_manager.get_ready_jobs(
            self.completed_jobs, self.failed_jobs
        )
        
        for job_id in ready_jobs:
            if job_id not in self.running_jobs and job_id in self.job_definitions:
                job_def = self.job_definitions[job_id]
                
                if job_def.enabled and job_def.trigger_type == TriggerType.DEPENDENCY:
                    await self._trigger_job(job_id, "dependency")
    
    async def _check_retry_jobs(self):
        """Check for jobs that need to be retried"""
        
        now = datetime.now()
        
        for execution_id, execution in self.job_executions.items():
            if (execution.status == JobStatus.RETRY and 
                execution.next_retry_time and 
                execution.next_retry_time <= now):
                
                job_def = self.job_definitions.get(execution.job_id)
                if job_def and job_def.enabled:
                    await self._trigger_job(execution.job_id, "retry", execution.attempt_number + 1)
    
    async def _trigger_job(self, job_id: str, triggered_by: str, attempt_number: int = 1):
        """Trigger execution of a job"""
        
        if job_id not in self.job_definitions:
            self.logger.error(f"Cannot trigger unknown job: {job_id}")
            return
        
        job_def = self.job_definitions[job_id]
        
        # Create execution record
        execution_id = f"{job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{attempt_number}"
        execution = JobExecution(
            job_id=job_id,
            execution_id=execution_id,
            status=JobStatus.PENDING,
            start_time=datetime.now(),
            attempt_number=attempt_number,
            triggered_by=triggered_by
        )
        
        self.job_executions[execution_id] = execution
        
        # Create and start job task
        task = asyncio.create_task(
            self._execute_job(job_def, execution),
            name=job_id
        )
        self.running_jobs[execution_id] = task
        
        self.logger.info(f"Triggered job: {job_id} (attempt {attempt_number}, triggered by {triggered_by})")
    
    async def _execute_job(self, job_def: JobDefinition, execution: JobExecution):
        """Execute a single job"""
        
        try:
            execution.status = JobStatus.RUNNING
            execution.start_time = datetime.now()
            
            self.logger.info(f"Starting job execution: {job_def.job_id}")
            
            # Send start notification
            if self.notification_service:
                context = NotificationContext(
                    notification_type=NotificationType.PIPELINE_START,
                    level=NotificationLevel.INFO,
                    title=f"Job Started: {job_def.name}",
                    message=f"Job '{job_def.name}' has started execution.",
                    pipeline_name=job_def.pipeline_name or job_def.name,
                    run_id=execution.execution_id
                )
                await self.notification_service.send_notification(context)
            
            # Execute job with timeout
            try:
                result = await asyncio.wait_for(
                    self._run_job_logic(job_def, execution),
                    timeout=job_def.timeout_seconds
                )
                
                execution.output = result
                execution.status = JobStatus.SUCCESS
                execution.end_time = datetime.now()
                
                # Add to completed jobs
                self.completed_jobs.add(job_def.job_id)
                
                self.logger.info(f"Job completed successfully: {job_def.job_id}")
                
                # Send success notification
                if self.notification_service and job_def.notify_on_success:
                    context = NotificationContext(
                        notification_type=NotificationType.PIPELINE_SUCCESS,
                        level=NotificationLevel.SUCCESS,
                        title=f"Job Completed: {job_def.name}",
                        message=f"Job '{job_def.name}' completed successfully.",
                        pipeline_name=job_def.pipeline_name or job_def.name,
                        run_id=execution.execution_id,
                        metadata=result or {}
                    )
                    await self.notification_service.send_notification(context)
                
            except asyncio.TimeoutError:
                execution.status = JobStatus.FAILED
                execution.error_message = f"Job timeout after {job_def.timeout_seconds} seconds"
                execution.end_time = datetime.now()
                
                await self._handle_job_failure(job_def, execution)
                
            except Exception as e:
                execution.status = JobStatus.FAILED
                execution.error_message = str(e)
                execution.end_time = datetime.now()
                
                await self._handle_job_failure(job_def, execution)
        
        finally:
            # Clean up running job tracking
            if execution.execution_id in self.running_jobs:
                del self.running_jobs[execution.execution_id]
    
    async def _run_job_logic(self, job_def: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Execute the actual job logic"""
        
        # Set environment variables
        original_env = {}
        for key, value in job_def.environment_vars.items():
            original_env[key] = os.environ.get(key)
            os.environ[key] = value
        
        try:
            # Run ETL pipeline based on job configuration
            if job_def.table_filter:
                # Run specific tables matching filter
                # This would need to be implemented based on the filter syntax
                result = await self._run_filtered_pipeline(job_def, execution)
            else:
                # Run full pipeline
                if job_def.pipeline_name:
                    # Run specific pipeline
                    result = self.etl_engine.run_single_table_ingestion(
                        *job_def.pipeline_name.split('.'), 
                        job_def.engine_type
                    )
                else:
                    # Run all tables
                    results = self.etl_engine.run_batch_ingestion()
                    
                    # Summarize results
                    success_count = len([r for r in results if r['status'] == 'SUCCESS'])
                    total_rows = sum([r.get('rows_loaded', 0) for r in results if r['status'] == 'SUCCESS'])
                    
                    result = {
                        'tables_processed': len(results),
                        'successful_tables': success_count,
                        'total_rows_processed': total_rows,
                        'detailed_results': results
                    }
            
            return result
            
        finally:
            # Restore environment variables
            for key, original_value in original_env.items():
                if original_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = original_value
    
    async def _run_filtered_pipeline(self, job_def: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Run pipeline with table filtering"""
        
        # Get active tables
        active_tables = self.etl_engine.control_audit.get_active_source_tables()
        
        # Apply filter (simplified - could be enhanced with SQL-like syntax)
        filtered_tables = []
        for table in active_tables:
            table_name = f"{table['source_database']}.{table['source_schema']}.{table['source_table']}"
            if job_def.table_filter in table_name:
                filtered_tables.append({
                    'source_database': table['source_database'],
                    'source_schema': table['source_schema'],
                    'source_table': table['source_table']
                })
        
        # Run filtered tables
        results = self.etl_engine.run_batch_ingestion(filtered_tables)
        
        return {
            'filtered_tables': len(filtered_tables),
            'results': results
        }
    
    async def _handle_job_failure(self, job_def: JobDefinition, execution: JobExecution):
        """Handle job failure with retry logic"""
        
        self.logger.error(f"Job failed: {job_def.job_id} - {execution.error_message}")
        
        # Check if retry is possible
        if (job_def.retry_config.enabled and 
            execution.attempt_number < job_def.retry_config.max_attempts):
            
            # Schedule retry
            delay = self._calculate_retry_delay(job_def.retry_config, execution.attempt_number)
            execution.next_retry_time = datetime.now() + timedelta(seconds=delay)
            execution.status = JobStatus.RETRY
            
            self.logger.info(f"Scheduling retry for job {job_def.job_id} in {delay} seconds")
        else:
            # No more retries
            self.failed_jobs.add(job_def.job_id)
            
            # Send failure notification
            if self.notification_service and job_def.notify_on_failure:
                context = NotificationContext(
                    notification_type=NotificationType.PIPELINE_FAILURE,
                    level=NotificationLevel.ERROR,
                    title=f"Job Failed: {job_def.name}",
                    message=f"Job '{job_def.name}' failed: {execution.error_message}",
                    pipeline_name=job_def.pipeline_name or job_def.name,
                    run_id=execution.execution_id,
                    metadata={
                        'attempt_number': execution.attempt_number,
                        'error_message': execution.error_message
                    }
                )
                await self.notification_service.send_notification(context)
    
    def _calculate_retry_delay(self, retry_config: RetryConfig, attempt_number: int) -> int:
        """Calculate delay before retry"""
        
        if retry_config.exponential_backoff:
            delay = retry_config.base_delay * (retry_config.backoff_multiplier ** (attempt_number - 1))
        else:
            delay = retry_config.base_delay
        
        return min(delay, retry_config.max_delay)
    
    def _get_last_execution_time(self, job_id: str) -> Optional[datetime]:
        """Get last execution time for a job"""
        
        last_time = None
        for execution in self.job_executions.values():
            if (execution.job_id == job_id and 
                execution.status in [JobStatus.SUCCESS, JobStatus.FAILED] and
                execution.end_time):
                
                if not last_time or execution.end_time > last_time:
                    last_time = execution.end_time
        
        return last_time
    
    def _cleanup_completed_jobs(self):
        """Clean up old execution records"""
        
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        # Remove old executions
        to_remove = [
            exec_id for exec_id, execution in self.job_executions.items()
            if (execution.status in [JobStatus.SUCCESS, JobStatus.FAILED] and
                execution.end_time and execution.end_time < cutoff_time)
        ]
        
        for exec_id in to_remove:
            del self.job_executions[exec_id]
        
        if to_remove:
            self.logger.debug(f"Cleaned up {len(to_remove)} old job executions")
    
    async def _scheduler_health_check(self):
        """Perform periodic health check"""
        
        # Check every 5 minutes
        if datetime.now().minute % 5 != 0:
            return
        
        running_count = len(self.running_jobs)
        total_jobs = len(self.job_definitions)
        enabled_jobs = len([j for j in self.job_definitions.values() if j.enabled])
        
        # Check for stuck jobs
        stuck_jobs = []
        now = datetime.now()
        for exec_id, execution in self.job_executions.items():
            if (execution.status == JobStatus.RUNNING and 
                (now - execution.start_time).total_seconds() > 7200):  # 2 hours
                stuck_jobs.append(execution.job_id)
        
        health_status = {
            'total_jobs': total_jobs,
            'enabled_jobs': enabled_jobs,
            'running_jobs': running_count,
            'stuck_jobs': len(stuck_jobs)
        }
        
        if stuck_jobs:
            self.logger.warning(f"Detected stuck jobs: {stuck_jobs}")
            
            if self.notification_service:
                self.notification_service.notify_system_health(
                    "Stuck Jobs Detected",
                    health_status,
                    NotificationLevel.WARNING
                )
    
    # Public API methods
    
    def get_job_status(self, job_id: str = None) -> List[Dict]:
        """Get status of jobs"""
        
        if job_id:
            if job_id in self.job_definitions:
                job_def = self.job_definitions[job_id]
                executions = [e for e in self.job_executions.values() if e.job_id == job_id]
                last_execution = max(executions, key=lambda x: x.start_time) if executions else None
                
                return [{
                    'job_id': job_id,
                    'name': job_def.name,
                    'enabled': job_def.enabled,
                    'trigger_type': job_def.trigger_type.value,
                    'last_status': last_execution.status.value if last_execution else 'NEVER_RUN',
                    'last_execution': last_execution.start_time if last_execution else None,
                    'next_retry': last_execution.next_retry_time if last_execution else None
                }]
            else:
                return []
        else:
            # Return all jobs
            result = []
            for job_id, job_def in self.job_definitions.items():
                executions = [e for e in self.job_executions.values() if e.job_id == job_id]
                last_execution = max(executions, key=lambda x: x.start_time) if executions else None
                
                result.append({
                    'job_id': job_id,
                    'name': job_def.name,
                    'enabled': job_def.enabled,
                    'trigger_type': job_def.trigger_type.value,
                    'last_status': last_execution.status.value if last_execution else 'NEVER_RUN',
                    'last_execution': last_execution.start_time if last_execution else None,
                    'dependencies': job_def.depends_on
                })
            
            return result
    
    async def trigger_job_manually(self, job_id: str) -> bool:
        """Manually trigger a job"""
        
        if job_id not in self.job_definitions:
            self.logger.error(f"Cannot trigger unknown job: {job_id}")
            return False
        
        await self._trigger_job(job_id, "manual")
        return True
    
    def cancel_job(self, execution_id: str) -> bool:
        """Cancel a running job"""
        
        if execution_id in self.running_jobs:
            self.running_jobs[execution_id].cancel()
            
            if execution_id in self.job_executions:
                self.job_executions[execution_id].status = JobStatus.CANCELLED
                self.job_executions[execution_id].end_time = datetime.now()
            
            self.logger.info(f"Cancelled job execution: {execution_id}")
            return True
        
        return False


# Usage Example
if __name__ == "__main__":
    import asyncio
    from unified_engine import create_unified_engine
    from notification_service import create_notification_service
    
    async def main():
        # Create components
        etl_engine = create_unified_engine(
            source_connection="postgresql://user:pass@localhost/source",
            target_connection="postgresql://user:pass@localhost/target"
        )
        
        notification_service = create_notification_service()
        
        # Create scheduler
        scheduler = JobScheduler(etl_engine, notification_service)
        
        # Define some example jobs
        daily_full_sync = JobDefinition(
            job_id="daily_full_sync",
            name="Daily Full Data Sync",
            description="Run full data synchronization daily at 2 AM",
            engine_type="pyspark",
            trigger_type=TriggerType.CRON,
            cron_expression="0 2 * * *",  # Daily at 2 AM
            timeout_seconds=7200,  # 2 hours
            notify_on_success=True,
            notify_on_failure=True
        )
        
        incremental_sync = JobDefinition(
            job_id="incremental_sync",
            name="Incremental Data Sync",
            description="Run incremental sync every 15 minutes",
            engine_type="python",
            trigger_type=TriggerType.INTERVAL,
            interval_seconds=900,  # 15 minutes
            table_filter="sales",  # Only tables with 'sales' in name
            priority=70
        )
        
        data_quality_check = JobDefinition(
            job_id="data_quality_check",
            name="Data Quality Validation",
            description="Validate data quality after sync",
            engine_type="python",
            trigger_type=TriggerType.DEPENDENCY,
            depends_on=["incremental_sync"],
            priority=60
        )
        
        # Add jobs to scheduler
        scheduler.add_job(daily_full_sync)
        scheduler.add_job(incremental_sync)
        scheduler.add_job(data_quality_check)
        
        # Start scheduler
        await scheduler.start()
        
        try:
            # Let it run for demo
            print("Scheduler running... Press Ctrl+C to stop")
            
            # Manual trigger for demo
            await scheduler.trigger_job_manually("incremental_sync")
            
            # Show status
            await asyncio.sleep(5)
            status = scheduler.get_job_status()
            print("Job Status:")
            for job in status:
                print(f"  {job['name']}: {job['last_status']} (enabled: {job['enabled']})")
            
            # Keep running
            while True:
                await asyncio.sleep(10)
                
        except KeyboardInterrupt:
            print("\nShutting down scheduler...")
        finally:
            await scheduler.stop()
    
    # Run the example
    asyncio.run(main())
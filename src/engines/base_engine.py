"""
src/engines/base_engine.py
Abstract base class for all ETL engines - ensures consistent interface
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import logging


class EngineCapability(Enum):
    """Engine capabilities for intelligent selection"""
    SMALL_DATA = "small_data"          # < 1GB
    MEDIUM_DATA = "medium_data"        # 1GB - 100GB  
    BIG_DATA = "big_data"             # > 100GB
    REAL_TIME = "real_time"           # Streaming data
    COMPLEX_JOINS = "complex_joins"    # Multi-table operations
    MACHINE_LEARNING = "ml"           # ML transformations
    GRAPH_PROCESSING = "graph"        # Network/graph data


@dataclass
class EngineMetrics:
    """Performance metrics for engine operations"""
    execution_time_seconds: float
    memory_usage_mb: float
    cpu_utilization_percent: float
    records_processed: int
    errors_count: int = 0
    warnings_count: int = 0


@dataclass
class TableProcessingContext:
    """Context information for table processing"""
    source_database: str
    source_schema: str
    source_table: str
    estimated_rows: Optional[int] = None
    estimated_size_mb: Optional[float] = None
    has_complex_joins: bool = False
    requires_streaming: bool = False
    quality_rules: List[str] = None


class BaseETLEngine(ABC):
    """
    Abstract base class defining the interface all ETL engines must implement
    Ensures consistency and enables intelligent engine selection
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.metrics: Dict[str, EngineMetrics] = {}
    
    @property
    @abstractmethod
    def engine_name(self) -> str:
        """Unique identifier for this engine"""
        pass
    
    @property
    @abstractmethod
    def supported_capabilities(self) -> List[EngineCapability]:
        """List of capabilities this engine supports"""
        pass
    
    @abstractmethod
    def can_handle_workload(self, context: TableProcessingContext) -> Tuple[bool, float]:
        """
        Determine if engine can handle workload and return confidence score
        Returns: (can_handle: bool, confidence_score: 0.0-1.0)
        """
        pass
    
    @abstractmethod
    def initialize(self) -> bool:
        """Initialize engine resources (connections, spark session, etc.)"""
        pass
    
    @abstractmethod
    def shutdown(self) -> bool:
        """Clean shutdown of engine resources"""
        pass
    
    @abstractmethod
    def discover_tables(self, schema_patterns: List[str] = None) -> List[Dict]:
        """Discover available tables from source systems"""
        pass
    
    @abstractmethod
    def extract_table_data(self, context: TableProcessingContext) -> Tuple[Any, Dict]:
        """
        Extract data from source table
        Returns: (data_frame, extraction_metadata)
        """
        pass
    
    @abstractmethod
    def transform_data(self, data: Any, transformation_rules: Dict) -> Tuple[Any, Dict]:
        """
        Apply transformations to extracted data
        Returns: (transformed_data, transformation_metadata)
        """
        pass
    
    @abstractmethod
    def load_data(self, data: Any, target_config: Dict) -> Dict:
        """
        Load transformed data to target destination
        Returns: load_result_metadata
        """
        pass
    
    @abstractmethod
    def run_data_quality_checks(self, data: Any, quality_rules: List[str]) -> Dict:
        """
        Execute data quality validation
        Returns: quality_results
        """
        pass
    
    @abstractmethod
    def get_performance_metrics(self) -> EngineMetrics:
        """Return current performance metrics"""
        pass
    
    # Optional methods with default implementations
    def supports_incremental_processing(self) -> bool:
        """Whether engine supports incremental data processing"""
        return True
    
    def supports_parallel_processing(self) -> bool:
        """Whether engine supports parallel table processing"""
        return False
    
    def get_optimal_batch_size(self, estimated_rows: int) -> int:
        """Calculate optimal batch size for processing"""
        if estimated_rows < 10000:
            return estimated_rows
        elif estimated_rows < 1000000:
            return 50000
        else:
            return 100000
    
    def estimate_processing_time(self, context: TableProcessingContext) -> float:
        """Estimate processing time in seconds"""
        if not context.estimated_rows:
            return 300.0  # Default 5 minutes
        
        # Base estimate: 10,000 rows per second
        base_time = context.estimated_rows / 10000
        
        # Adjust for complexity
        if context.has_complex_joins:
            base_time *= 2.0
        if context.requires_streaming:
            base_time *= 1.5
        if context.quality_rules and len(context.quality_rules) > 5:
            base_time *= 1.3
            
        return max(base_time, 30.0)  # Minimum 30 seconds
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check and return status"""
        try:
            # Basic health check implementation
            return {
                'engine': self.engine_name,
                'status': 'healthy',
                'initialized': hasattr(self, '_initialized') and self._initialized,
                'last_activity': getattr(self, '_last_activity', None),
                'total_jobs_processed': len(self.metrics),
                'average_processing_time': self._calculate_average_processing_time()
            }
        except Exception as e:
            return {
                'engine': self.engine_name,
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def _calculate_average_processing_time(self) -> float:
        """Calculate average processing time across all jobs"""
        if not self.metrics:
            return 0.0
        
        total_time = sum(m.execution_time_seconds for m in self.metrics.values())
        return total_time / len(self.metrics)
    
    def _log_performance_metrics(self, operation: str, metrics: EngineMetrics):
        """Log performance metrics for monitoring"""
        self.logger.info(
            f"Engine performance - Operation: {operation}, "
            f"Time: {metrics.execution_time_seconds:.2f}s, "
            f"Memory: {metrics.memory_usage_mb:.1f}MB, "
            f"Records: {metrics.records_processed:,}, "
            f"Errors: {metrics.errors_count}"
        )
        
        # Store metrics for analysis
        self.metrics[f"{operation}_{len(self.metrics)}"] = metrics


class EngineExecutionContext:
    """Execution context passed between engine operations"""
    
    def __init__(self, run_id: str, job_name: str):
        self.run_id = run_id
        self.job_name = job_name
        self.start_time = None
        self.metadata = {}
        self.quality_results = {}
        self.performance_metrics = {}
    
    def add_metadata(self, key: str, value: Any):
        """Add metadata to execution context"""
        self.metadata[key] = value
    
    def add_quality_result(self, table: str, results: Dict):
        """Add quality check results"""
        self.quality_results[table] = results
    
    def add_performance_metric(self, operation: str, metrics: EngineMetrics):
        """Add performance metrics"""
        self.performance_metrics[operation] = metrics
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for serialization"""
        return {
            'run_id': self.run_id,
            'job_name': self.job_name,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'metadata': self.metadata,
            'quality_results': self.quality_results,
            'performance_metrics': {
                k: {
                    'execution_time_seconds': v.execution_time_seconds,
                    'memory_usage_mb': v.memory_usage_mb,
                    'cpu_utilization_percent': v.cpu_utilization_percent,
                    'records_processed': v.records_processed,
                    'errors_count': v.errors_count,
                    'warnings_count': v.warnings_count
                }
                for k, v in self.performance_metrics.items()
            }
        }
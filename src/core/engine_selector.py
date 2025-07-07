"""
src/core/engine_selector.py
Intelligent engine selection based on data characteristics, complexity, and performance
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import psutil
import time
from pathlib import Path

from engines.base_engine import BaseETLEngine, EngineCapability, TableProcessingContext


class SelectionStrategy(Enum):
    """Engine selection strategies"""
    PERFORMANCE_OPTIMIZED = "performance"    # Choose fastest engine
    COST_OPTIMIZED = "cost"                 # Choose most cost-effective
    RESOURCE_OPTIMIZED = "resource"         # Choose based on available resources
    BALANCED = "balanced"                   # Balance performance, cost, and resources
    MANUAL = "manual"                       # Use manually specified engine


@dataclass
class EngineScore:
    """Scoring result for engine selection"""
    engine_name: str
    total_score: float
    performance_score: float
    capability_score: float
    resource_score: float
    cost_score: float
    confidence: float
    can_handle: bool
    estimated_time_seconds: float
    estimated_cost_usd: float = 0.0
    reasoning: List[str] = None


@dataclass
class SystemResources:
    """Current system resource availability"""
    cpu_count: int
    cpu_usage_percent: float
    memory_total_gb: float
    memory_available_gb: float
    memory_usage_percent: float
    disk_usage_percent: float
    spark_available: bool = False
    gpu_available: bool = False


class EngineSelector:
    """
    Intelligent engine selection system that chooses the optimal ETL engine
    based on data characteristics, system resources, and performance requirements
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        self.engines: Dict[str, BaseETLEngine] = {}
        self.engine_performance_history: Dict[str, List[Dict]] = {}
        self.selection_strategy = SelectionStrategy(
            self.config.get('selection_strategy', 'balanced')
        )
        
    def register_engine(self, engine: BaseETLEngine):
        """Register an available ETL engine"""
        self.engines[engine.engine_name] = engine
        if engine.engine_name not in self.engine_performance_history:
            self.engine_performance_history[engine.engine_name] = []
        
        self.logger.info(f"Registered engine: {engine.engine_name}")
    
    def select_optimal_engine(self, context: TableProcessingContext, 
                            strategy: Optional[SelectionStrategy] = None) -> Tuple[str, EngineScore]:
        """
        Select the optimal engine for processing based on context and strategy
        Returns: (engine_name, score_details)
        """
        
        if not self.engines:
            raise ValueError("No engines registered for selection")
        
        strategy = strategy or self.selection_strategy
        
        # Get current system resources
        system_resources = self._get_system_resources()
        
        # Score all available engines
        engine_scores = []
        for engine_name, engine in self.engines.items():
            try:
                score = self._score_engine(engine, context, system_resources, strategy)
                engine_scores.append(score)
            except Exception as e:
                self.logger.warning(f"Failed to score engine {engine_name}: {str(e)}")
                continue
        
        if not engine_scores:
            raise RuntimeError("No engines available or capable of handling workload")
        
        # Filter engines that can handle the workload
        capable_engines = [score for score in engine_scores if score.can_handle]
        
        if not capable_engines:
            # Fall back to best effort with warning
            self.logger.warning("No engines marked as fully capable, selecting best available")
            capable_engines = engine_scores
        
        # Select engine based on strategy
        selected_score = self._apply_selection_strategy(capable_engines, strategy)
        
        self.logger.info(
            f"Selected engine '{selected_score.engine_name}' with score {selected_score.total_score:.2f} "
            f"(confidence: {selected_score.confidence:.2f}, estimated time: {selected_score.estimated_time_seconds:.1f}s)"
        )
        
        return selected_score.engine_name, selected_score
    
    def _score_engine(self, engine: BaseETLEngine, context: TableProcessingContext,
                     resources: SystemResources, strategy: SelectionStrategy) -> EngineScore:
        """Score an individual engine for the given context"""
        
        # Check if engine can handle the workload
        can_handle, capability_confidence = engine.can_handle_workload(context)
        
        # Calculate individual scores (0.0 - 1.0)
        performance_score = self._calculate_performance_score(engine, context)
        capability_score = self._calculate_capability_score(engine, context)
        resource_score = self._calculate_resource_score(engine, context, resources)
        cost_score = self._calculate_cost_score(engine, context)
        
        # Apply strategy-specific weights
        weights = self._get_strategy_weights(strategy)
        
        # Calculate total weighted score
        total_score = (
            performance_score * weights['performance'] +
            capability_score * weights['capability'] +
            resource_score * weights['resource'] +
            cost_score * weights['cost']
        )
        
        # Estimate execution time and cost
        estimated_time = engine.estimate_processing_time(context)
        estimated_cost = self._estimate_processing_cost(engine, context, estimated_time)
        
        # Generate reasoning
        reasoning = self._generate_selection_reasoning(
            engine, performance_score, capability_score, resource_score, cost_score
        )
        
        return EngineScore(
            engine_name=engine.engine_name,
            total_score=total_score,
            performance_score=performance_score,
            capability_score=capability_score,
            resource_score=resource_score,
            cost_score=cost_score,
            confidence=capability_confidence,
            can_handle=can_handle,
            estimated_time_seconds=estimated_time,
            estimated_cost_usd=estimated_cost,
            reasoning=reasoning
        )
    
    def _calculate_performance_score(self, engine: BaseETLEngine, 
                                   context: TableProcessingContext) -> float:
        """Calculate performance score based on historical data and engine characteristics"""
        
        # Base score from engine capabilities
        base_score = 0.5
        
        # Adjust based on data size
        if context.estimated_rows:
            if context.estimated_rows < 10000:  # Small data
                if 'python' in engine.engine_name.lower():
                    base_score += 0.3  # Python excels at small data
                elif 'spark' in engine.engine_name.lower():
                    base_score -= 0.2  # Spark overhead for small data
            elif context.estimated_rows > 1000000:  # Big data
                if 'spark' in engine.engine_name.lower():
                    base_score += 0.4  # Spark excels at big data
                elif 'python' in engine.engine_name.lower():
                    base_score -= 0.3  # Python struggles with big data
        
        # Adjust based on historical performance
        history = self.engine_performance_history.get(engine.engine_name, [])
        if history:
            # Calculate average performance from recent executions
            recent_history = history[-10:]  # Last 10 executions
            avg_performance = sum(h.get('performance_score', 0.5) for h in recent_history) / len(recent_history)
            base_score = base_score * 0.3 + avg_performance * 0.7  # Weight recent history heavily
        
        return max(0.0, min(1.0, base_score))
    
    def _calculate_capability_score(self, engine: BaseETLEngine, 
                                  context: TableProcessingContext) -> float:
        """Calculate how well engine capabilities match workload requirements"""
        
        score = 0.8  # Base capability score
        
        # Check specific capability requirements
        if context.has_complex_joins:
            if EngineCapability.COMPLEX_JOINS in engine.supported_capabilities:
                score += 0.1
            else:
                score -= 0.2
        
        if context.requires_streaming:
            if EngineCapability.REAL_TIME in engine.supported_capabilities:
                score += 0.2
            else:
                score -= 0.5  # Major penalty for missing streaming support
        
        # Data size capability matching
        if context.estimated_size_mb:
            if context.estimated_size_mb < 1000:  # < 1GB
                if EngineCapability.SMALL_DATA in engine.supported_capabilities:
                    score += 0.1
            elif context.estimated_size_mb > 100000:  # > 100GB
                if EngineCapability.BIG_DATA in engine.supported_capabilities:
                    score += 0.2
                else:
                    score -= 0.3
        
        return max(0.0, min(1.0, score))
    
    def _calculate_resource_score(self, engine: BaseETLEngine, context: TableProcessingContext,
                                resources: SystemResources) -> float:
        """Calculate resource utilization efficiency score"""
        
        score = 0.5
        
        # Memory considerations
        if 'spark' in engine.engine_name.lower():
            # Spark needs more memory
            if resources.memory_available_gb > 8:
                score += 0.2
            elif resources.memory_available_gb < 4:
                score -= 0.3
        
        elif 'python' in engine.engine_name.lower():
            # Python is more memory efficient for smaller datasets
            if resources.memory_available_gb > 2:
                score += 0.1
        
        # CPU considerations
        if engine.supports_parallel_processing():
            if resources.cpu_count > 4:
                score += 0.2
            else:
                score -= 0.1
        
        # Current resource usage
        if resources.cpu_usage_percent > 80:
            score -= 0.2
        if resources.memory_usage_percent > 85:
            score -= 0.3
        
        return max(0.0, min(1.0, score))
    
    def _calculate_cost_score(self, engine: BaseETLEngine, 
                            context: TableProcessingContext) -> float:
        """Calculate cost efficiency score"""
        
        # Base cost score (higher is better/cheaper)
        score = 0.7
        
        # SQL engines are typically most cost-effective
        if 'sql' in engine.engine_name.lower():
            score += 0.2
        
        # Spark can be expensive for small datasets
        if 'spark' in engine.engine_name.lower():
            if context.estimated_rows and context.estimated_rows < 100000:
                score -= 0.3  # High cost for small data
            else:
                score += 0.1  # Good value for large data
        
        # Python is cost-effective for medium datasets
        if 'python' in engine.engine_name.lower():
            if context.estimated_rows and 1000 <= context.estimated_rows <= 1000000:
                score += 0.2
        
        return max(0.0, min(1.0, score))
    
    def _get_strategy_weights(self, strategy: SelectionStrategy) -> Dict[str, float]:
        """Get scoring weights based on selection strategy"""
        
        if strategy == SelectionStrategy.PERFORMANCE_OPTIMIZED:
            return {'performance': 0.5, 'capability': 0.3, 'resource': 0.1, 'cost': 0.1}
        elif strategy == SelectionStrategy.COST_OPTIMIZED:
            return {'performance': 0.1, 'capability': 0.2, 'resource': 0.2, 'cost': 0.5}
        elif strategy == SelectionStrategy.RESOURCE_OPTIMIZED:
            return {'performance': 0.2, 'capability': 0.3, 'resource': 0.4, 'cost': 0.1}
        else:  # BALANCED
            return {'performance': 0.3, 'capability': 0.3, 'resource': 0.2, 'cost': 0.2}
    
    def _apply_selection_strategy(self, scores: List[EngineScore], 
                                strategy: SelectionStrategy) -> EngineScore:
        """Apply final selection logic based on strategy"""
        
        if strategy == SelectionStrategy.PERFORMANCE_OPTIMIZED:
            # Select engine with best performance score
            return max(scores, key=lambda s: s.performance_score)
        elif strategy == SelectionStrategy.COST_OPTIMIZED:
            # Select most cost-effective engine
            return max(scores, key=lambda s: s.cost_score)
        elif strategy == SelectionStrategy.RESOURCE_OPTIMIZED:
            # Select based on resource efficiency
            return max(scores, key=lambda s: s.resource_score)
        else:  # BALANCED or default
            # Select based on total weighted score
            return max(scores, key=lambda s: s.total_score)
    
    def _get_system_resources(self) -> SystemResources:
        """Get current system resource information"""
        
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return SystemResources(
            cpu_count=psutil.cpu_count(),
            cpu_usage_percent=psutil.cpu_percent(interval=1),
            memory_total_gb=memory.total / (1024**3),
            memory_available_gb=memory.available / (1024**3),
            memory_usage_percent=memory.percent,
            disk_usage_percent=disk.percent,
            spark_available=self._check_spark_available(),
            gpu_available=self._check_gpu_available()
        )
    
    def _check_spark_available(self) -> bool:
        """Check if Spark is available in the environment"""
        try:
            import pyspark
            return True
        except ImportError:
            return False
    
    def _check_gpu_available(self) -> bool:
        """Check if GPU resources are available"""
        try:
            import GPUtil
            gpus = GPUtil.getGPUs()
            return len(gpus) > 0
        except (ImportError, Exception):
            return False
    
    def _estimate_processing_cost(self, engine: BaseETLEngine, 
                                context: TableProcessingContext, 
                                estimated_time_seconds: float) -> float:
        """Estimate processing cost in USD (simplified model)"""
        
        # Simplified cost model - in production this would be more sophisticated
        base_cost_per_hour = {
            'python': 0.10,   # Low cost
            'sql': 0.05,      # Very low cost  
            'spark': 0.50     # Higher cost due to cluster resources
        }
        
        engine_type = 'python'  # Default
        for eng_type in base_cost_per_hour.keys():
            if eng_type in engine.engine_name.lower():
                engine_type = eng_type
                break
        
        hours = estimated_time_seconds / 3600
        base_cost = base_cost_per_hour[engine_type] * hours
        
        # Adjust for data size
        if context.estimated_size_mb:
            if context.estimated_size_mb > 10000:  # > 10GB
                base_cost *= 1.5
            elif context.estimated_size_mb > 100000:  # > 100GB
                base_cost *= 3.0
        
        return round(base_cost, 4)
    
    def _generate_selection_reasoning(self, engine: BaseETLEngine, 
                                    performance: float, capability: float,
                                    resource: float, cost: float) -> List[str]:
        """Generate human-readable reasoning for engine selection"""
        
        reasoning = []
        
        if performance > 0.7:
            reasoning.append(f"Strong performance profile for {engine.engine_name}")
        elif performance < 0.3:
            reasoning.append(f"Performance concerns for {engine.engine_name}")
        
        if capability > 0.8:
            reasoning.append("Excellent capability match for workload requirements")
        elif capability < 0.4:
            reasoning.append("Limited capability match - may struggle with requirements")
        
        if resource > 0.7:
            reasoning.append("Efficient resource utilization expected")
        elif resource < 0.3:
            reasoning.append("Resource constraints may impact performance")
        
        if cost > 0.7:
            reasoning.append("Cost-effective option")
        elif cost < 0.3:
            reasoning.append("Higher cost expected")
        
        # Add engine-specific reasoning
        if 'spark' in engine.engine_name.lower():
            reasoning.append("Spark engine - excellent for large-scale parallel processing")
        elif 'python' in engine.engine_name.lower():
            reasoning.append("Python engine - flexible and efficient for medium datasets")
        elif 'sql' in engine.engine_name.lower():
            reasoning.append("SQL engine - optimal for database-native operations")
        
        return reasoning
    
    def record_execution_result(self, engine_name: str, context: TableProcessingContext,
                              actual_time_seconds: float, success: bool,
                              performance_metrics: Dict[str, Any] = None):
        """Record actual execution results for improving future selections"""
        
        result = {
            'timestamp': time.time(),
            'context': {
                'estimated_rows': context.estimated_rows,
                'estimated_size_mb': context.estimated_size_mb,
                'has_complex_joins': context.has_complex_joins,
                'requires_streaming': context.requires_streaming
            },
            'actual_time_seconds': actual_time_seconds,
            'success': success,
            'performance_score': 1.0 if success else 0.0,
            'metrics': performance_metrics or {}
        }
        
        if engine_name not in self.engine_performance_history:
            self.engine_performance_history[engine_name] = []
        
        self.engine_performance_history[engine_name].append(result)
        
        # Keep only recent history (last 100 executions)
        if len(self.engine_performance_history[engine_name]) > 100:
            self.engine_performance_history[engine_name] = \
                self.engine_performance_history[engine_name][-100:]
        
        self.logger.info(f"Recorded execution result for {engine_name}: {actual_time_seconds:.1f}s")
    
    def get_selection_recommendations(self, context: TableProcessingContext) -> Dict[str, Any]:
        """Get detailed recommendations for engine selection"""
        
        if not self.engines:
            return {'error': 'No engines available'}
        
        recommendations = {
            'context_analysis': {},
            'engine_recommendations': [],
            'system_status': self._get_system_resources().__dict__,
            'optimization_tips': []
        }
        
        # Analyze context
        if context.estimated_rows:
            if context.estimated_rows < 10000:
                recommendations['context_analysis']['data_size'] = 'small'
                recommendations['optimization_tips'].append('Consider Python engine for small datasets')
            elif context.estimated_rows > 1000000:
                recommendations['context_analysis']['data_size'] = 'large'
                recommendations['optimization_tips'].append('Consider Spark engine for large datasets')
            else:
                recommendations['context_analysis']['data_size'] = 'medium'
        
        if context.has_complex_joins:
            recommendations['optimization_tips'].append('Complex joins detected - ensure adequate memory')
        
        if context.requires_streaming:
            recommendations['optimization_tips'].append('Streaming required - verify real-time capabilities')
        
        # Score all engines
        try:
            selected_engine, selected_score = self.select_optimal_engine(context)
            recommendations['recommended_engine'] = selected_engine
            recommendations['selection_confidence'] = selected_score.confidence
            recommendations['estimated_time_seconds'] = selected_score.estimated_time_seconds
            recommendations['estimated_cost_usd'] = selected_score.estimated_cost_usd
        except Exception as e:
            recommendations['error'] = str(e)
        
        return recommendations


# Factory function for easy instantiation
def create_engine_selector(config: Dict[str, Any] = None) -> EngineSelector:
    """Factory function to create configured engine selector"""
    return EngineSelector(config)
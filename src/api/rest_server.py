"""
src/api/rest_server.py
Production-ready FastAPI server for ETL Framework
Provides REST API endpoints for pipeline management, monitoring, and configuration
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
import asyncio
import json
import os
from contextlib import asynccontextmanager

# Import framework components
from core.unified_engine import UnifiedETLEngine, PipelineConfig
from core.engine_selector import EngineSelector
from quality.data_quality_framework import DataQualityFramework
from utils.notification_service import NotificationService
from utils.config_manager import ConfigManager


# ==========================================
# PYDANTIC MODELS
# ==========================================

class PipelineExecutionRequest(BaseModel):
    """Request model for pipeline execution"""
    tables: Optional[List[str]] = Field(None, description="Specific tables to process")
    engine: Optional[str] = Field(None, description="Force specific engine")
    parallel: bool = Field(False, description="Execute in parallel")
    max_parallel: int = Field(5, description="Maximum parallel executions")
    dry_run: bool = Field(False, description="Validate without execution")


class QualityCheckRequest(BaseModel):
    """Request model for quality checks"""
    table: str = Field(..., description="Table to check (database.schema.table)")
    rules: Optional[List[str]] = Field(None, description="Specific rules to run")
    layer: str = Field("bronze", description="Data layer (bronze/silver/gold)")


class TableRegistrationRequest(BaseModel):
    """Request model for table registration"""
    database: str
    schema: str
    table: str
    increment_column: Optional[str] = None
    business_key_columns: Optional[List[str]] = None
    is_scd_type_2_enabled: bool = False
    partition_column: Optional[str] = None


class APIResponse(BaseModel):
    """Standard API response model"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)


class HealthCheckResponse(BaseModel):
    """Health check response model"""
    status: str
    timestamp: datetime
    version: str
    engines: Dict[str, Dict[str, Any]]
    system_resources: Dict[str, Any]


# ==========================================
# GLOBAL STATE & DEPENDENCIES
# ==========================================

# Global application state
app_state = {
    "etl_engine": None,
    "config_manager": None,
    "quality_framework": None,
    "notification_service": None,
    "engine_selector": None
}

# Security
security = HTTPBearer(auto_error=False)

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Simple authentication dependency (extend for production)"""
    if credentials and credentials.credentials == os.getenv("API_TOKEN", "dev-token"):
        return {"user": "api_user"}
    return None  # Allow unauthenticated access for development


async def get_etl_engine() -> UnifiedETLEngine:
    """Dependency to get ETL engine"""
    if not app_state["etl_engine"]:
        raise HTTPException(status_code=500, detail="ETL engine not initialized")
    return app_state["etl_engine"]


# ==========================================
# APPLICATION LIFECYCLE
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    
    # Startup
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting ETL Framework API Server...")
    
    try:
        # Initialize configuration manager
        config_manager = ConfigManager(environment=os.getenv("ENVIRONMENT", "development"))
        app_state["config_manager"] = config_manager
        
        # Initialize ETL engine
        config_path = os.getenv("ETL_CONFIG_PATH", "config/production.yaml")
        if os.path.exists(config_path):
            etl_engine = UnifiedETLEngine(config_path)
            app_state["etl_engine"] = etl_engine
            logger.info("✅ ETL engine initialized")
        
        # Initialize quality framework
        quality_framework = DataQualityFramework()
        app_state["quality_framework"] = quality_framework
        
        # Initialize engine selector
        engine_selector = EngineSelector()
        if app_state["etl_engine"]:
            for engine in app_state["etl_engine"].engines.values():
                engine_selector.register_engine(engine)
        app_state["engine_selector"] = engine_selector
        
        # Initialize notification service
        notification_config = {
            "email": {"enabled": False},
            "slack": {"enabled": False},
            "webhook": {"enabled": False}
        }
        app_state["notification_service"] = NotificationService(notification_config)
        
        logger.info("🎉 API server startup complete")
        
        yield
        
    except Exception as e:
        logger.error(f"❌ Startup failed: {str(e)}")
        raise
    
    # Shutdown
    logger.info("🛑 Shutting down ETL Framework API Server...")
    if app_state["etl_engine"]:
        app_state["etl_engine"].stop_all_engines()
    logger.info("✅ Shutdown complete")


# ==========================================
# FASTAPI APPLICATION
# ==========================================

app = FastAPI(
    title="ETL Framework API",
    description="Production-ready metadata-driven ETL pipeline REST API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==========================================
# EXCEPTION HANDLERS
# ==========================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Unhandled exception in {request.url}: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "message": "Internal server error",
            "error": str(exc) if os.getenv("DEBUG") else "An unexpected error occurred"
        }
    )


# ==========================================
# HEALTH & STATUS ENDPOINTS
# ==========================================

@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """System health check endpoint"""
    
    engines_status = {}
    if app_state["etl_engine"]:
        for engine_name, engine in app_state["etl_engine"].engines.items():
            try:
                health = engine.health_check()
                engines_status[engine_name] = health
            except Exception as e:
                engines_status[engine_name] = {"status": "error", "error": str(e)}
    
    # Get system resources
    import psutil
    memory = psutil.virtual_memory()
    
    system_resources = {
        "cpu_count": psutil.cpu_count(),
        "cpu_usage_percent": psutil.cpu_percent(),
        "memory_total_gb": round(memory.total / (1024**3), 2),
        "memory_available_gb": round(memory.available / (1024**3), 2),
        "memory_usage_percent": memory.percent
    }
    
    # Determine overall status
    healthy_engines = sum(1 for status in engines_status.values() if status.get("status") == "healthy")
    total_engines = len(engines_status)
    overall_status = "healthy" if healthy_engines == total_engines else "degraded" if healthy_engines > 0 else "unhealthy"
    
    return HealthCheckResponse(
        status=overall_status,
        timestamp=datetime.now(),
        version="1.0.0",
        engines=engines_status,
        system_resources=system_resources
    )


@app.get("/status")
async def system_status():
    """Detailed system status"""
    
    status = {
        "api_server": {
            "status": "running",
            "startup_time": datetime.now().isoformat(),
            "environment": os.getenv("ENVIRONMENT", "development")
        },
        "components": {
            "etl_engine": app_state["etl_engine"] is not None,
            "quality_framework": app_state["quality_framework"] is not None,
            "notification_service": app_state["notification_service"] is not None,
            "engine_selector": app_state["engine_selector"] is not None
        }
    }
    
    return APIResponse(
        success=True,
        message="System status retrieved",
        data=status
    )


# ==========================================
# PIPELINE ENDPOINTS
# ==========================================

@app.post("/pipeline/execute")
async def execute_pipeline(
    request: PipelineExecutionRequest,
    background_tasks: BackgroundTasks,
    etl_engine: UnifiedETLEngine = Depends(get_etl_engine),
    current_user: Optional[Dict] = Depends(get_current_user)
):
    """Execute ETL pipeline"""
    
    try:
        if request.dry_run:
            # Validate pipeline configuration
            validation_results = etl_engine.validate_pipeline()
            return APIResponse(
                success=validation_results["valid"],
                message="Pipeline validation completed",
                data=validation_results
            )
        
        # Execute pipeline in background
        if request.tables:
            # Execute specific tables
            results = []
            for table in request.tables:
                parts = table.split('.')
                if len(parts) != 3:
                    raise HTTPException(status_code=400, detail=f"Invalid table format: {table}")
                
                database, schema, table_name = parts
                result = etl_engine.run_table_ingestion(database, schema, table_name, force_engine=request.engine)
                results.append({"table": table, **result})
            
            return APIResponse(
                success=True,
                message=f"Pipeline executed for {len(request.tables)} tables",
                data={"results": results}
            )
        
        elif request.parallel:
            # Execute all tables in parallel
            results = etl_engine.run_parallel_ingestion(
                max_parallel_tables=request.max_parallel,
                force_engine=request.engine
            )
            
            successful = [r for r in results if r.get('status') == 'SUCCESS']
            failed = [r for r in results if r.get('status') == 'FAILED']
            
            return APIResponse(
                success=len(failed) == 0,
                message=f"Pipeline completed: {len(successful)} successful, {len(failed)} failed",
                data={
                    "successful": len(successful),
                    "failed": len(failed),
                    "results": results
                }
            )
        
        else:
            # Execute all tables sequentially
            results = etl_engine.run_all_table_ingestions()
            
            successful = [r for r in results if r.get('status') == 'SUCCESS']
            failed = [r for r in results if r.get('status') == 'FAILED']
            
            return APIResponse(
                success=len(failed) == 0,
                message=f"Pipeline completed: {len(successful)} successful, {len(failed)} failed",
                data={
                    "successful": len(successful),
                    "failed": len(failed),
                    "results": results
                }
            )
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pipeline/status")
async def get_pipeline_status(
    days: int = 1,
    table: Optional[str] = None,
    failed_only: bool = False,
    etl_engine: UnifiedETLEngine = Depends(get_etl_engine)
):
    """Get pipeline execution status and history"""
    
    try:
        stats = etl_engine.control_audit.get_ingestion_statistics(days=days)
        
        if table:
            stats = [s for s in stats if table.lower() in s.get('source_table', '').lower()]
        
        if failed_only:
            stats = [s for s in stats if s.get('status') == 'FAILED']
        
        # Calculate summary
        total_executions = len(stats)
        successful = len([s for s in stats if s.get('status') == 'SUCCESS'])
        failed = len([s for s in stats if s.get('status') == 'FAILED'])
        
        summary = {
            "total_executions": total_executions,
            "successful": successful,
            "failed": failed,
            "success_rate": (successful / total_executions * 100) if total_executions > 0 else 0,
            "executions": stats[-50:]  # Return last 50 executions
        }
        
        return APIResponse(
            success=True,
            message=f"Pipeline status for last {days} day(s)",
            data=summary
        )
    
    except Exception as e:
        logger.error(f"Failed to get pipeline status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# DISCOVERY ENDPOINTS
# ==========================================

@app.post("/discovery/tables")
async def discover_tables(
    schema_patterns: Optional[List[str]] = None,
    register: bool = False,
    limit: Optional[int] = None,
    etl_engine: UnifiedETLEngine = Depends(get_etl_engine)
):
    """Discover tables from source databases"""
    
    try:
        discovered_tables = etl_engine.discover_tables(schema_patterns)
        
        if limit:
            discovered_tables = discovered_tables[:limit]
        
        registered_count = 0
        if register and discovered_tables:
            registered_count = etl_engine.register_discovered_tables(discovered_tables)
        
        return APIResponse(
            success=True,
            message=f"Discovered {len(discovered_tables)} tables" + (f", registered {registered_count}" if register else ""),
            data={
                "discovered_count": len(discovered_tables),
                "registered_count": registered_count if register else 0,
                "tables": discovered_tables
            }
        )
    
    except Exception as e:
        logger.error(f"Table discovery failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/tables/register")
async def register_table(
    request: TableRegistrationRequest,
    etl_engine: UnifiedETLEngine = Depends(get_etl_engine)
):
    """Register a table for ETL processing"""
    
    try:
        # Create table configuration
        table_config = {
            'source_database': request.database,
            'source_schema': request.schema,
            'source_table': request.table,
            'increment_column': request.increment_column,
            'business_key_columns': request.business_key_columns,
            'is_scd_type_2_enabled': request.is_scd_type_2_enabled,
            'partition_column': request.partition_column,
            'is_active': True
        }
        
        # Register table
        success = etl_engine.control_audit.register_source_table(table_config)
        
        return APIResponse(
            success=success,
            message=f"Table {request.database}.{request.schema}.{request.table} registered successfully",
            data=table_config
        )
    
    except Exception as e:
        logger.error(f"Table registration failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# QUALITY ENDPOINTS
# ==========================================

@app.post("/quality/check")
async def run_quality_check(
    request: QualityCheckRequest,
    quality_framework: DataQualityFramework = Depends(lambda: app_state["quality_framework"])
):
    """Run data quality checks on a table"""
    
    try:
        # For demo purposes, create sample data
        # In production, this would load actual table data
        import pandas as pd
        sample_data = pd.DataFrame({
            'id': range(1, 1001),
            'email': [f'user{i}@example.com' if i % 10 != 0 else f'invalid_email_{i}' for i in range(1, 1001)],
            'phone': [f'+1-555-{i:04d}' if i % 15 != 0 else f'invalid_phone_{i}' for i in range(1, 1001)],
            'age': [20 + (i % 60) if i % 20 != 0 else None for i in range(1, 1001)]
        })
        
        # Execute quality checks
        quality_report = quality_framework.execute_quality_checks(
            data=sample_data,
            table_name=request.table,
            layer=request.layer,
            rules_to_run=request.rules
        )
        
        # Convert report to dict for JSON response
        report_data = {
            'table_name': quality_report.table_name,
            'layer': quality_report.layer,
            'execution_timestamp': quality_report.execution_timestamp.isoformat(),
            'total_records': quality_report.total_records,
            'overall_quality_score': quality_report.overall_quality_score,
            'rules_passed': quality_report.rules_passed,
            'rules_failed': quality_report.rules_failed,
            'recommendations': quality_report.recommendations,
            'rule_results': [
                {
                    'rule_id': r.rule_id,
                    'rule_name': r.rule_name,
                    'rule_type': r.rule_type.value,
                    'severity': r.severity.value,
                    'is_passed': r.is_passed,
                    'pass_rate': r.pass_rate,
                    'failure_rate': r.failure_rate,
                    'failed_records': r.failed_records,
                    'sample_failures': r.sample_failures[:5],  # Limit samples
                    'execution_time_ms': r.execution_time_ms
                }
                for r in quality_report.rule_results
            ]
        }
        
        return APIResponse(
            success=True,
            message=f"Quality check completed with {quality_report.overall_quality_score:.1f}% score",
            data=report_data
        )
    
    except Exception as e:
        logger.error(f"Quality check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENGINE SELECTION ENDPOINTS
# ==========================================

@app.post("/engines/recommend")
async def recommend_engine(
    table: str,
    estimated_rows: Optional[int] = None,
    estimated_size_mb: Optional[float] = None,
    has_complex_joins: bool = False,
    requires_streaming: bool = False,
    engine_selector: EngineSelector = Depends(lambda: app_state["engine_selector"])
):
    """Get engine recommendation for a workload"""
    
    try:
        from core.engine_selector import TableProcessingContext
        
        context = TableProcessingContext(
            source_database=table.split('.')[0] if '.' in table else 'unknown',
            source_schema=table.split('.')[1] if table.count('.') >= 1 else 'unknown',
            source_table=table.split('.')[-1],
            estimated_rows=estimated_rows,
            estimated_size_mb=estimated_size_mb,
            has_complex_joins=has_complex_joins,
            requires_streaming=requires_streaming
        )
        
        recommendations = engine_selector.get_selection_recommendations(context)
        
        return APIResponse(
            success=True,
            message="Engine recommendations generated",
            data=recommendations
        )
    
    except Exception as e:
        logger.error(f"Engine recommendation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# CONFIGURATION ENDPOINTS
# ==========================================

@app.get("/config/current")
async def get_current_config():
    """Get current configuration"""
    
    try:
        if app_state["etl_engine"]:
            config_dict = {
                'name': app_state["etl_engine"].config.name,
                'description': app_state["etl_engine"].config.description,
                'default_engine': app_state["etl_engine"].config.default_engine.value,
                'auto_discovery': app_state["etl_engine"].config.auto_discovery,
                'schema_patterns': app_state["etl_engine"].config.schema_patterns,
                'engines': list(app_state["etl_engine"].config.engines.keys())
            }
            
            return APIResponse(
                success=True,
                message="Current configuration retrieved",
                data=config_dict
            )
        else:
            raise HTTPException(status_code=404, detail="No configuration loaded")
    
    except Exception as e:
        logger.error(f"Failed to get configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# MAIN APPLICATION ENTRY POINT
# ==========================================

if __name__ == "__main__":
    # Development server
    uvicorn.run(
        "rest_server:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        log_level="info"
    )


def create_app() -> FastAPI:
    """Factory function for creating the app (useful for testing)"""
    return app
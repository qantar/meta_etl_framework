# From current code:
#- class DataSourceManager (base structure)
#- get_last_update_timestamp() method

# Split into specialized classes:
#- class AbstractDataSource (base class)
#- class DataSourceFactory (factory pattern)
#- load_data() (generic method)

import json
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import delta
from delta.tables import DeltaTable

class DataSourceManager:
    """Manages different data source connections and data loading"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def get_last_update_timestamp(self, job_id: int, metadata_manager: MetadataManager) -> Optional[datetime]:
        """Get last successful update timestamp for incremental loads"""
        
        query = """
        SELECT MAX(end_time) as last_update
        FROM etl_job_executions 
        WHERE job_id = %s AND status = 'success'
        """
        
        with metadata_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (job_id,))
                result = cursor.fetchone()
                return result[0] if result[0] else None




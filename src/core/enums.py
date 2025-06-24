# Extract from current code:
# - class SourceType(Enum)
# - class LayerType(Enum) 
# - class JobStatus(Enum)

# # Additional enums:
# - class DataQualityStatus(Enum)
# - class TransformationType(Enum)
# - class NotificationLevel(Enum)



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


# Configuration Classes
class SourceType(Enum):
    POSTGRESQL = "postgresql"
    FILE = "file"
    
class LayerType(Enum):
    BRONZE = "bronze"
    SILVER = "silver" 
    GOLD = "gold"

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

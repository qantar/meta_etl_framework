# From current code:
#- class MedallionETLProcessor (main structure)
#- process_bronze_layer() method
#- process_silver_layer() method  
#- process_gold_layer() method
#- run_etl_job() method
#- run_all_jobs() method

# Additional methods:
#- validate_layer_transition()
#- handle_schema_evolution()
#- optimize_layer_performance()


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

class MedallionETLProcessor:
    """Main ETL processor implementing medallion architecture"""
    
    def __init__(self, spark: SparkSession, metadata_manager: MetadataManager, 
                 bronze_path: str, silver_path: str, gold_path: str):
        self.spark = spark
        self.metadata_manager = metadata_manager
        self.data_source_manager = DataSourceManager(spark)
        self.data_quality_manager = DataQualityManager()
        
        self.bronze_path = bronze_path
        self.silver_path = silver_path  
        self.gold_path = gold_path
        
        # Configure logging
        logging.basicConfig(level=logging.INFO,
                          format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def process_bronze_layer(self, job_config: Dict, execution_id: str) -> DataFrame:
        """Process bronze layer - raw data ingestion"""
        
        self.logger.info(f"Processing bronze layer for job: {job_config['job_name']}")
        
        # Get last update timestamp for incremental loads
        last_update = None
        if job_config['load_type'] == 'incremental':
            last_update = self.data_source_manager.get_last_update_timestamp(
                job_config['job_id'], self.metadata_manager
            )
        
        # Load data based on source type
        if job_config['source_type'] == SourceType.POSTGRESQL.value:
            df = self.data_source_manager.load_from_postgresql(job_config, job_config, last_update)
        elif job_config['source_type'] == SourceType.FILE.value:
            df = self.data_source_manager.load_from_file(job_config, job_config)
        else:
            raise ValueError(f"Unsupported source type: {job_config['source_type']}")
        
        # Add metadata columns
        df_with_metadata = df.withColumn("_bronze_timestamp", current_timestamp()) \
                            .withColumn("_source_system", lit(job_config['connection_name'])) \
                            .withColumn("_execution_id", lit(execution_id))
        
        # Write to bronze layer
        bronze_table_path = f"{self.bronze_path}/{job_config['target_table']}"
        
        if job_config['load_type'] == 'full':
            df_with_metadata.write.format("delta").mode("overwrite").save(bronze_table_path)
        else:
            # Incremental load with merge
            if DeltaTable.isDeltaTable(self.spark, bronze_table_path):
                delta_table = DeltaTable.forPath(self.spark, bronze_table_path)
                
                # Simple append for bronze layer incremental
                df_with_metadata.write.format("delta").mode("append").save(bronze_table_path)
            else:
                df_with_metadata.write.format("delta").mode("overwrite").save(bronze_table_path)
        
        # Data quality checks
        quality_metrics = self.data_quality_manager.check_data_quality(df_with_metadata, job_config['target_table'])
        self.metadata_manager.log_data_quality(execution_id, job_config['target_table'], 
                                             LayerType.BRONZE.value, quality_metrics)
        
        self.logger.info(f"Bronze layer processing completed. Records: {df_with_metadata.count()}")
        return df_with_metadata
    
    # Silver Layer Logic
    def process_silver_layer(self, job_config: Dict, execution_id: str) -> DataFrame:
        """Process silver layer - cleaned and validated data"""
        
        self.logger.info(f"Processing silver layer for job: {job_config['job_name']}")
        
        # Read from bronze layer
        bronze_table_path = f"{self.bronze_path}/{job_config['target_table']}"
        df_bronze = self.spark.read.format("delta").load(bronze_table_path)
        
        # Basic data cleaning and validation
        df_cleaned = df_bronze.dropDuplicates() \
                             .filter(col("_bronze_timestamp").isNotNull())
        
        # Add silver layer metadata
        df_silver = df_cleaned.withColumn("_silver_timestamp", current_timestamp()) \
                             .withColumn("_record_hash", 
                                       sha2(concat_ws("|", *[col(c) for c in df_cleaned.columns if not c.startswith("_")]), 256))
        
        # Write to silver layer
        silver_table_path = f"{self.silver_path}/{job_config['target_table']}"
        
        if job_config['business_key']:
            # Use SCD Type 1 logic for silver layer
            if DeltaTable.isDeltaTable(self.spark, silver_table_path):
                delta_table = DeltaTable.forPath(self.spark, silver_table_path)
                
                delta_table.alias("target").merge(
                    df_silver.alias("source"),
                    f"target.{job_config['business_key']} = source.{job_config['business_key']}"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
            else:
                df_silver.write.format("delta").mode("overwrite").save(silver_table_path)
        else:
            df_silver.write.format("delta").mode("overwrite").save(silver_table_path)
        
        # Data quality checks
        quality_metrics = self.data_quality_manager.check_data_quality(df_silver, job_config['target_table'])
        self.metadata_manager.log_data_quality(execution_id, job_config['target_table'], 
                                             LayerType.SILVER.value, quality_metrics)
        
        self.logger.info(f"Silver layer processing completed. Records: {df_silver.count()}")
        return df_silver
    
    
    
    # Gold Layer Logic
    def process_gold_layer(self, job_config: Dict, execution_id: str) -> DataFrame:
        """Process gold layer - business-ready aggregated data"""
        
        self.logger.info(f"Processing gold layer for job: {job_config['job_name']}")
        
        # Read from silver layer
        silver_table_path = f"{self.silver_path}/{job_config['target_table']}"
        df_silver = self.spark.read.format("delta").load(silver_table_path)
        
        # Apply transformation rules if specified
        df_transformed = df_silver
        if job_config.get('transformation_rules'):
            # Basic transformations - this can be extended
            rules = job_config['transformation_rules']
            if isinstance(rules, str):
                rules = json.loads(rules)
            
            # Example: Apply aggregations, business rules, etc.
            # This is a simplified example - extend based on requirements
            for rule in rules.get('aggregations', []):
                if rule['type'] == 'count_by_date':
                    df_transformed = df_transformed.groupBy(date_format(col(rule['date_column']), "yyyy-MM-dd")) \
                                                  .count() \
                                                  .withColumnRenamed("count", f"{rule['name']}_count")
        
        # Add gold layer metadata
        df_gold = df_transformed.withColumn("_gold_timestamp", current_timestamp()) \
                               .withColumn("_business_date", current_date())
        
        # Write to gold layer
        gold_table_path = f"{self.gold_path}/{job_config['target_table']}"
        
        if job_config.get('partition_columns'):
            df_gold.write.format("delta") \
                   .partitionBy(*job_config['partition_columns']) \
                   .mode("overwrite") \
                   .save(gold_table_path)
        else:
            df_gold.write.format("delta").mode("overwrite").save(gold_table_path)
        
        # Data quality checks
        quality_metrics = self.data_quality_manager.check_data_quality(df_gold, job_config['target_table'])
        self.metadata_manager.log_data_quality(execution_id, job_config['target_table'], 
                                             LayerType.GOLD.value, quality_metrics)
        
        self.logger.info(f"Gold layer processing completed. Records: {df_gold.count()}")
        return df_gold
    
    def run_etl_job(self, job_config: Dict):
        """Execute complete ETL job through all medallion layers"""
        
        execution_id = self.metadata_manager.create_job_execution(job_config['job_id'])
        
        try:
            self.logger.info(f"Starting ETL job: {job_config['job_name']} (Execution: {execution_id})")
            
            total_records = 0
            
            # Process through medallion layers
            if job_config['target_layer'] in [LayerType.BRONZE.value, 'all']:
                df_bronze = self.process_bronze_layer(job_config, execution_id)
                total_records = df_bronze.count()
            
            if job_config['target_layer'] in [LayerType.SILVER.value, 'all']:
                df_silver = self.process_silver_layer(job_config, execution_id)
                total_records = df_silver.count()
            
            if job_config['target_layer'] in [LayerType.GOLD.value, 'all']:
                df_gold = self.process_gold_layer(job_config, execution_id)
                total_records = df_gold.count()
            
            # Update execution status
            self.metadata_manager.update_job_execution(
                execution_id, JobStatus.SUCCESS.value, total_records,
                metadata={'layers_processed': job_config['target_layer']}
            )
            
            self.logger.info(f"ETL job completed successfully: {job_config['job_name']}")
            
        except Exception as e:
            error_message = f"ETL job failed: {str(e)}\n{traceback.format_exc()}"
            self.logger.error(error_message)
            
            self.metadata_manager.update_job_execution(
                execution_id, JobStatus.FAILED.value, 0, error_message
            )
            raise
    
    def run_all_jobs(self):
        """Execute all active ETL jobs"""
        
        jobs = self.metadata_manager.get_active_jobs()
        self.logger.info(f"Found {len(jobs)} active ETL jobs")
        
        for job in jobs:
            try:
                self.run_etl_job(job)
            except Exception as e:
                self.logger.error(f"Job {job['job_name']} failed: {str(e)}")
                continue


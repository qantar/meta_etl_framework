# From current code:
#- load_from_postgresql() method (move from DataSourceManager)

# Additional implementations:
#class PostgreSQLSource(AbstractDataSource):
#    - connect()
#    - execute_query()
#    - get_table_metadata()
#    - handle_connection_pooling()
#    - implement_cdc_logic()


def load_from_postgresql(self, config: Dict, table_config: Dict, 
                       last_update: Optional[datetime] = None) -> DataFrame:
    """Load data from PostgreSQL"""
    
    jdbc_url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database_name']}"
    
    properties = {
        "user": config['username'],
        "password": config['password_encrypted'],  # In production, decrypt this
        "driver": "org.postgresql.Driver"
    }
    
    # Build query with incremental logic
    if table_config['load_type'] == 'incremental' and table_config['delta_column'] and last_update:
        query = f"""
        (SELECT * FROM {table_config['source_schema']}.{table_config['source_table']} 
         WHERE {table_config['delta_column']} > '{last_update}') as incremental_data
        """
    else:
        query = f"SELECT * FROM  {table_config['source_schema']}.{table_config['source_table']}"
    
    return self.spark.read.jdbc(jdbc_url, query, properties=properties)
   
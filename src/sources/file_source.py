# From current code:
#- load_from_file() method (move from DataSourceManager)

# Additional implementations:
#class FileSource(AbstractDataSource):
#    - detect_file_format()
#    - handle_compressed_files()
#    - process_nested_directories()
#    - validate_file_integrity()

 
def load_from_file(self, config: Dict, table_config: Dict) -> DataFrame:
    """Load data from file"""
    
    file_format = config['file_format'].lower()
    file_path = config['file_path']
    
    if file_format == 'csv':
        return self.spark.read.option("header", "true").csv(file_path)
    elif file_format == 'json':
        return self.spark.read.json(file_path)
    elif file_format == 'parquet':
        return self.spark.read.parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

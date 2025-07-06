"""
Configuration Management System
Handles environment-specific configurations, secrets management, and configuration validation
"""

import os
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict, field
from enum import Enum
import keyring
import cryptography
from cryptography.fernet import Fernet
import base64
import getpass
from urllib.parse import urlparse, parse_qs


class Environment(Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)  # Hide password in string representation
    schema: str = "public"
    ssl_mode: str = "prefer"
    connection_timeout: int = 30
    pool_size: int = 10
    max_overflow: int = 20


@dataclass
class SparkConfig:
    """Spark configuration settings"""
    app_name: str = "ETL-Framework"
    master: str = "local[*]"
    executor_memory: str = "2g"
    executor_cores: int = 2
    driver_memory: str = "1g"
    max_result_size: str = "1g"
    adaptive_enabled: bool = True
    dynamic_allocation: bool = True
    sql_extensions: List[str] = field(default_factory=lambda: ["io.delta.sql.DeltaSparkSessionExtension"])
    additional_configs: Dict[str, str] = field(default_factory=dict)


@dataclass
class StorageConfig:
    """Storage configuration for data lakes and backups"""
    backup_root_path: str = "/data/backup"
    dwh_root_path: str = "/data/dwh"
    staging_root_path: str = "/data/staging"
    bronze_path: str = "/data/bronze"
    silver_path: str = "/data/silver"
    gold_path: str = "/data/gold"
    compression: str = "snappy"
    partition_strategy: str = "date"


@dataclass
class NotificationConfig:
    """Notification settings"""
    enabled: bool = True
    email_enabled: bool = False
    slack_enabled: bool = False
    webhook_enabled: bool = False
    email_smtp_host: str = ""
    email_smtp_port: int = 587
    email_username: str = ""
    email_password: str = field(repr=False)
    slack_webhook_url: str = field(repr=False)
    webhook_url: str = field(repr=False)
    notification_levels: List[str] = field(default_factory=lambda: ["ERROR", "SUCCESS"])


@dataclass
class SecurityConfig:
    """Security and encryption settings"""
    encryption_enabled: bool = True
    encryption_key: str = field(repr=False)
    use_keyring: bool = True
    password_min_length: int = 8
    session_timeout: int = 3600
    audit_enabled: bool = True


@dataclass
class PerformanceConfig:
    """Performance tuning settings"""
    max_parallel_tables: int = 5
    batch_size: int = 10000
    connection_pool_size: int = 10
    query_timeout: int = 300
    retry_attempts: int = 3
    retry_delay: int = 5
    cache_enabled: bool = True
    optimization_enabled: bool = True


@dataclass
class MaintenanceConfig:
    """Maintenance and cleanup settings"""
    retention_days: int = 30
    backup_retention_days: int = 90
    log_retention_days: int = 7
    auto_cleanup_enabled: bool = True
    auto_optimize_enabled: bool = True
    cleanup_schedule: str = "0 2 * * 0"  # Sunday at 2 AM


@dataclass
class ETLFrameworkConfig:
    """Complete ETL Framework configuration"""
    environment: Environment
    pipeline_name: str
    description: str = ""
    
    # Database configurations
    source_db: DatabaseConfig = None
    target_db: DatabaseConfig = None
    metadata_db: DatabaseConfig = None
    
    # Component configurations
    spark: SparkConfig = field(default_factory=SparkConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    maintenance: MaintenanceConfig = field(default_factory=MaintenanceConfig)
    
    # Engine settings
    enabled_engines: List[str] = field(default_factory=lambda: ["python"])
    default_engine: str = "python"
    
    # Discovery settings
    auto_discovery: bool = True
    schema_patterns: List[str] = field(default_factory=lambda: ["public"])
    
    # Custom settings
    custom_settings: Dict[str, Any] = field(default_factory=dict)


class ConfigurationManager:
    """
    Manages ETL Framework configurations with support for:
    - Environment-specific configurations
    - Secrets management and encryption
    - Configuration validation
    - Hot reloading
    """
    
    def __init__(self, config_dir: str = "config", environment: str = None):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        
        # Determine environment
        self.environment = Environment(environment or os.getenv("ETL_ENVIRONMENT", "development"))
        
        # Initialize encryption
        self.encryption_key = self._get_or_create_encryption_key()
        self.cipher = Fernet(self.encryption_key)
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Cache for loaded configurations
        self._config_cache: Dict[str, ETLFrameworkConfig] = {}
        
        self.logger.info(f"Configuration manager initialized for environment: {self.environment.value}")
    
    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for secrets management"""
        
        key_file = self.config_dir / ".encryption_key"
        
        if key_file.exists():
            with open(key_file, 'rb') as f:
                return f.read()
        else:
            # Generate new key
            key = Fernet.generate_key()
            
            # Save key securely
            key_file.write_bytes(key)
            key_file.chmod(0o600)  # Read/write for owner only
            
            self.logger.info("Generated new encryption key")
            return key
    
    def encrypt_value(self, value: str) -> str:
        """Encrypt a sensitive value"""
        if not value:
            return value
        
        encrypted = self.cipher.encrypt(value.encode())
        return base64.b64encode(encrypted).decode()
    
    def decrypt_value(self, encrypted_value: str) -> str:
        """Decrypt a sensitive value"""
        if not encrypted_value:
            return encrypted_value
        
        try:
            encrypted_bytes = base64.b64decode(encrypted_value.encode())
            decrypted = self.cipher.decrypt(encrypted_bytes)
            return decrypted.decode()
        except Exception:
            # Value might not be encrypted
            return encrypted_value
    
    def create_database_config(self, name: str, host: str, port: int, database: str, 
                             username: str, password: str = None, **kwargs) -> DatabaseConfig:
        """Create database configuration with optional password encryption"""
        
        if password is None:
            password = getpass.getpass(f"Enter password for {name} database: ")
        
        # Encrypt password
        encrypted_password = self.encrypt_value(password)
        
        return DatabaseConfig(
            host=host,
            port=port,
            database=database,
            username=username,
            password=encrypted_password,
            **kwargs
        )
    
    def get_connection_string(self, db_config: DatabaseConfig, decrypt_password: bool = True) -> str:
        """Generate connection string from database configuration"""
        
        password = self.decrypt_value(db_config.password) if decrypt_password else db_config.password
        
        return (f"postgresql://{db_config.username}:{password}@"
               f"{db_config.host}:{db_config.port}/{db_config.database}")
    
    def get_jdbc_connection_string(self, db_config: DatabaseConfig, decrypt_password: bool = True) -> str:
        """Generate JDBC connection string from database configuration"""
        
        password = self.decrypt_value(db_config.password) if decrypt_password else db_config.password
        
        return (f"jdbc:postgresql://{db_config.host}:{db_config.port}/{db_config.database}"
               f"?user={db_config.username}&password={password}")
    
    def load_environment_config(self, config_name: str = None) -> ETLFrameworkConfig:
        """Load configuration for the current environment"""
        
        if config_name is None:
            config_name = f"{self.environment.value}"
        
        # Check cache first
        cache_key = f"{config_name}_{self.environment.value}"
        if cache_key in self._config_cache:
            return self._config_cache[cache_key]
        
        # Load base configuration
        base_config_path = self.config_dir / "base.yaml"
        env_config_path = self.config_dir / f"{config_name}.yaml"
        
        config = ETLFrameworkConfig(
            environment=self.environment,
            pipeline_name=f"etl_{config_name}_{self.environment.value}"
        )
        
        # Load base configuration if exists
        if base_config_path.exists():
            base_data = self._load_yaml_file(base_config_path)
            config = self._merge_config_data(config, base_data)
        
        # Load environment-specific configuration
        if env_config_path.exists():
            env_data = self._load_yaml_file(env_config_path)
            config = self._merge_config_data(config, env_data)
        else:
            self.logger.warning(f"Environment config file not found: {env_config_path}")
        
        # Apply environment variable overrides
        config = self._apply_env_overrides(config)
        
        # Validate configuration
        self._validate_config(config)
        
        # Cache configuration
        self._config_cache[cache_key] = config
        
        self.logger.info(f"Loaded configuration: {config_name} for {self.environment.value}")
        return config
    
    def _load_yaml_file(self, file_path: Path) -> Dict:
        """Load YAML configuration file"""
        
        try:
            with open(file_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            self.logger.error(f"Failed to load config file {file_path}: {str(e)}")
            return {}
    
    def _merge_config_data(self, config: ETLFrameworkConfig, data: Dict) -> ETLFrameworkConfig:
        """Merge configuration data into config object"""
        
        # Basic settings
        if 'pipeline_name' in data:
            config.pipeline_name = data['pipeline_name']
        if 'description' in data:
            config.description = data['description']
        if 'enabled_engines' in data:
            config.enabled_engines = data['enabled_engines']
        if 'default_engine' in data:
            config.default_engine = data['default_engine']
        if 'auto_discovery' in data:
            config.auto_discovery = data['auto_discovery']
        if 'schema_patterns' in data:
            config.schema_patterns = data['schema_patterns']
        
        # Database configurations
        if 'databases' in data:
            db_configs = data['databases']
            
            if 'source' in db_configs:
                config.source_db = self._create_db_config_from_dict(db_configs['source'])
            if 'target' in db_configs:
                config.target_db = self._create_db_config_from_dict(db_configs['target'])
            if 'metadata' in db_configs:
                config.metadata_db = self._create_db_config_from_dict(db_configs['metadata'])
        
        # Component configurations
        if 'spark' in data:
            config.spark = self._update_dataclass_from_dict(config.spark, data['spark'])
        if 'storage' in data:
            config.storage = self._update_dataclass_from_dict(config.storage, data['storage'])
        if 'notifications' in data:
            config.notifications = self._update_dataclass_from_dict(config.notifications, data['notifications'])
        if 'security' in data:
            config.security = self._update_dataclass_from_dict(config.security, data['security'])
        if 'performance' in data:
            config.performance = self._update_dataclass_from_dict(config.performance, data['performance'])
        if 'maintenance' in data:
            config.maintenance = self._update_dataclass_from_dict(config.maintenance, data['maintenance'])
        
        # Custom settings
        if 'custom_settings' in data:
            config.custom_settings.update(data['custom_settings'])
        
        return config
    
    def _create_db_config_from_dict(self, db_data: Dict) -> DatabaseConfig:
        """Create DatabaseConfig from dictionary"""
        
        # Handle both encrypted and plain passwords
        password = db_data.get('password', '')
        if password and not password.startswith('gAAAAA'):  # Not encrypted
            password = self.encrypt_value(password)
        
        return DatabaseConfig(
            host=db_data.get('host', 'localhost'),
            port=db_data.get('port', 5432),
            database=db_data.get('database', ''),
            username=db_data.get('username', ''),
            password=password,
            schema=db_data.get('schema', 'public'),
            ssl_mode=db_data.get('ssl_mode', 'prefer'),
            connection_timeout=db_data.get('connection_timeout', 30),
            pool_size=db_data.get('pool_size', 10),
            max_overflow=db_data.get('max_overflow', 20)
        )
    
    def _update_dataclass_from_dict(self, dataclass_obj: Any, data: Dict) -> Any:
        """Update dataclass object with values from dictionary"""
        
        updated_data = asdict(dataclass_obj)
        updated_data.update(data)
        
        return type(dataclass_obj)(**updated_data)
    
    def _apply_env_overrides(self, config: ETLFrameworkConfig) -> ETLFrameworkConfig:
        """Apply environment variable overrides"""
        
        # Environment variable patterns:
        # ETL_SOURCE_HOST, ETL_SOURCE_PORT, etc.
        
        env_mappings = {
            'ETL_PIPELINE_NAME': 'pipeline_name',
            'ETL_DEFAULT_ENGINE': 'default_engine',
            'ETL_AUTO_DISCOVERY': 'auto_discovery',
        }
        
        for env_var, config_attr in env_mappings.items():
            if os.getenv(env_var):
                setattr(config, config_attr, os.getenv(env_var))
        
        # Database overrides
        if config.source_db:
            config.source_db = self._apply_db_env_overrides(config.source_db, 'SOURCE')
        if config.target_db:
            config.target_db = self._apply_db_env_overrides(config.target_db, 'TARGET')
        if config.metadata_db:
            config.metadata_db = self._apply_db_env_overrides(config.metadata_db, 'METADATA')
        
        return config
    
    def _apply_db_env_overrides(self, db_config: DatabaseConfig, prefix: str) -> DatabaseConfig:
        """Apply database-specific environment overrides"""
        
        overrides = {}
        
        env_mappings = {
            f'ETL_{prefix}_HOST': 'host',
            f'ETL_{prefix}_PORT': 'port',
            f'ETL_{prefix}_DATABASE': 'database',
            f'ETL_{prefix}_USERNAME': 'username',
            f'ETL_{prefix}_PASSWORD': 'password',
            f'ETL_{prefix}_SCHEMA': 'schema',
        }
        
        for env_var, attr in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                if attr == 'port':
                    overrides[attr] = int(value)
                elif attr == 'password':
                    overrides[attr] = self.encrypt_value(value)
                else:
                    overrides[attr] = value
        
        if overrides:
            updated_data = asdict(db_config)
            updated_data.update(overrides)
            return DatabaseConfig(**updated_data)
        
        return db_config
    
    def _validate_config(self, config: ETLFrameworkConfig):
        """Validate configuration for completeness and correctness"""
        
        errors = []
        
        # Required fields
        if not config.pipeline_name:
            errors.append("Pipeline name is required")
        
        # Database validations
        if config.source_db:
            if not config.source_db.host or not config.source_db.database:
                errors.append("Source database host and database name are required")
        
        if config.target_db:
            if not config.target_db.host or not config.target_db.database:
                errors.append("Target database host and database name are required")
        
        # Engine validations
        if not config.enabled_engines:
            errors.append("At least one engine must be enabled")
        
        if config.default_engine not in config.enabled_engines:
            errors.append(f"Default engine '{config.default_engine}' must be in enabled engines")
        
        # Performance validations
        if config.performance.max_parallel_tables < 1:
            errors.append("Max parallel tables must be at least 1")
        
        if config.performance.batch_size < 1000:
            errors.append("Batch size should be at least 1000 for performance")
        
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
        
        self.logger.info("Configuration validation passed")
    
    def save_config(self, config: ETLFrameworkConfig, config_name: str = None):
        """Save configuration to file"""
        
        if config_name is None:
            config_name = config.environment.value
        
        config_path = self.config_dir / f"{config_name}.yaml"
        
        # Convert to dictionary for serialization
        config_dict = self._config_to_dict(config)
        
        with open(config_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)
        
        self.logger.info(f"Configuration saved: {config_path}")
    
    def _config_to_dict(self, config: ETLFrameworkConfig) -> Dict:
        """Convert configuration to dictionary for serialization"""
        
        result = {
            'pipeline_name': config.pipeline_name,
            'description': config.description,
            'enabled_engines': config.enabled_engines,
            'default_engine': config.default_engine,
            'auto_discovery': config.auto_discovery,
            'schema_patterns': config.schema_patterns,
            'databases': {},
            'spark': asdict(config.spark),
            'storage': asdict(config.storage),
            'notifications': asdict(config.notifications),
            'security': asdict(config.security),
            'performance': asdict(config.performance),
            'maintenance': asdict(config.maintenance),
            'custom_settings': config.custom_settings
        }
        
        # Add database configurations
        if config.source_db:
            result['databases']['source'] = asdict(config.source_db)
        if config.target_db:
            result['databases']['target'] = asdict(config.target_db)
        if config.metadata_db:
            result['databases']['metadata'] = asdict(config.metadata_db)
        
        return result
    
    def create_sample_config(self, config_name: str = "development"):
        """Create a sample configuration file"""
        
        sample_config = ETLFrameworkConfig(
            environment=Environment(config_name),
            pipeline_name=f"sample_etl_{config_name}",
            description="Sample ETL Pipeline Configuration",
            enabled_engines=["python", "pyspark"],
            default_engine="python",
            auto_discovery=True,
            schema_patterns=["public", "sales"],
        )
        
        # Add sample database configurations
        sample_config.source_db = DatabaseConfig(
            host="localhost",
            port=5432,
            database="source_db",
            username="etl_user",
            password=self.encrypt_value("sample_password"),
            schema="public"
        )
        
        sample_config.target_db = DatabaseConfig(
            host="localhost",
            port=5432,
            database="target_db",
            username="etl_user",
            password=self.encrypt_value("sample_password"),
            schema="public"
        )
        
        # Customize for environment
        if config_name == "production":
            sample_config.performance.max_parallel_tables = 10
            sample_config.maintenance.retention_days = 90
            sample_config.security.encryption_enabled = True
            sample_config.notifications.enabled = True
        
        self.save_config(sample_config, config_name)
        self.logger.info(f"Sample configuration created: {config_name}")
    
    def get_secrets_manager(self) -> 'SecretsManager':
        """Get secrets manager instance"""
        return SecretsManager(self.cipher)


class SecretsManager:
    """Manages sensitive configuration values"""
    
    def __init__(self, cipher: Fernet):
        self.cipher = cipher
        self.logger = logging.getLogger(__name__)
    
    def store_secret(self, key: str, value: str, use_keyring: bool = True):
        """Store a secret value"""
        
        if use_keyring:
            try:
                keyring.set_password("etl_framework", key, value)
                self.logger.info(f"Secret stored in keyring: {key}")
            except Exception as e:
                self.logger.warning(f"Failed to store in keyring, using encryption: {e}")
                encrypted = self.cipher.encrypt(value.encode())
                # Could store encrypted value in config file or environment
        else:
            encrypted = self.cipher.encrypt(value.encode())
            # Store encrypted value
    
    def retrieve_secret(self, key: str, encrypted_value: str = None, use_keyring: bool = True) -> str:
        """Retrieve a secret value"""
        
        if use_keyring:
            try:
                value = keyring.get_password("etl_framework", key)
                if value:
                    return value
            except Exception as e:
                self.logger.warning(f"Failed to retrieve from keyring: {e}")
        
        if encrypted_value:
            try:
                decrypted = self.cipher.decrypt(base64.b64decode(encrypted_value))
                return decrypted.decode()
            except Exception as e:
                self.logger.error(f"Failed to decrypt value: {e}")
                return encrypted_value
        
        return ""


# Usage Example
if __name__ == "__main__":
    # Initialize configuration manager
    config_manager = ConfigurationManager(environment="development")
    
    # Create sample configurations
    config_manager.create_sample_config("development")
    config_manager.create_sample_config("production")
    
    # Load configuration
    config = config_manager.load_environment_config()
    
    print(f"Loaded configuration: {config.pipeline_name}")
    print(f"Enabled engines: {config.enabled_engines}")
    
    # Get connection strings
    if config.source_db:
        source_conn = config_manager.get_connection_string(config.source_db)
        print(f"Source connection: {source_conn[:50]}...")
    
    # Use secrets manager
    secrets = config_manager.get_secrets_manager()
    secrets.store_secret("api_key", "my_secret_api_key")
    retrieved = secrets.retrieve_secret("api_key")
    print(f"Retrieved secret: {retrieved}")
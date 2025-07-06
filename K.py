"""
Advanced Data Quality Framework for ETL Pipeline
Comprehensive data validation, profiling, and quality scoring system
"""

import re
import json
import logging
import statistics
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Union, Callable, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import great_expectations as ge
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
import phonenumbers
import validators


class QualityRuleType(Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"
    INTEGRITY = "integrity"
    BUSINESS_RULE = "business_rule"


class Severity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ValidationResult(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    SKIP = "SKIP"


@dataclass
class QualityRuleResult:
    """Result of a single quality rule execution"""
    rule_id: str
    rule_name: str
    rule_type: QualityRuleType
    severity: Severity
    result: ValidationResult
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    records_checked: int = 0
    records_failed: int = 0
    execution_time_seconds: float = 0.0
    
    @property
    def success_rate(self) -> float:
        if self.records_checked == 0:
            return 0.0
        return ((self.records_checked - self.records_failed) / self.records_checked) * 100


@dataclass
class DataProfileResult:
    """Data profiling results for a dataset"""
    table_name: str
    total_records: int
    total_columns: int
    numeric_columns: int
    text_columns: int
    date_columns: int
    null_percentage: float
    duplicate_percentage: float
    column_profiles: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    data_types: Dict[str, str] = field(default_factory=dict)
    sample_data: List[Dict] = field(default_factory=list)
    
    def get_quality_score(self) -> float:
        """Calculate overall quality score based on profile"""
        score = 100.0
        
        # Deduct for nulls
        score -= min(self.null_percentage * 2, 30)
        
        # Deduct for duplicates
        score -= min(self.duplicate_percentage * 3, 40)
        
        # Bonus for data completeness
        if self.total_records > 1000:
            score += 5
        
        return max(0, min(100, score))


@dataclass
class QualityRule:
    """Definition of a data quality rule"""
    rule_id: str
    name: str
    description: str
    rule_type: QualityRuleType
    severity: Severity
    enabled: bool = True
    
    # Target specification
    table_pattern: Optional[str] = None
    column_pattern: Optional[str] = None
    where_clause: Optional[str] = None
    
    # Rule parameters
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Thresholds
    fail_threshold: Optional[float] = None
    warning_threshold: Optional[float] = None
    
    # Metadata
    created_by: str = "system"
    created_at: datetime = field(default_factory=datetime.now)
    tags: List[str] = field(default_factory=list)


class BaseQualityCheck(ABC):
    """Base class for quality checks"""
    
    def __init__(self, rule: QualityRule):
        self.rule = rule
        self.logger = logging.getLogger(__name__)
    
    @abstractmethod
    def execute(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> QualityRuleResult:
        """Execute the quality check"""
        pass
    
    def _create_result(self, result: ValidationResult, message: str, 
                      records_checked: int = 0, records_failed: int = 0,
                      details: Dict[str, Any] = None) -> QualityRuleResult:
        """Helper to create quality rule result"""
        
        return QualityRuleResult(
            rule_id=self.rule.rule_id,
            rule_name=self.rule.name,
            rule_type=self.rule.rule_type,
            severity=self.rule.severity,
            result=result,
            message=message,
            details=details or {},
            records_checked=records_checked,
            records_failed=records_failed
        )


class CompletenessCheck(BaseQualityCheck):
    """Check for data completeness (null values)"""
    
    def execute(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> QualityRuleResult:
        column = self.rule.parameters.get('column')
        max_null_percentage = self.rule.parameters.get('max_null_percentage', 5.0)
        
        if not column or column not in df.columns:
            return self._create_result(
                ValidationResult.SKIP,
                f"Column '{column}' not found in dataset"
            )
        
        total_records = len(df)
        null_count = df[column].isnull().sum()
        null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
        
        if null_percentage > max_null_percentage:
            result = ValidationResult.FAIL
            message = f"Column '{column}' has {null_percentage:.1f}% null values (limit: {max_null_percentage}%)"
        elif null_percentage > (max_null_percentage * 0.7):  # Warning at 70% of threshold
            result = ValidationResult.WARNING
            message = f"Column '{column}' has {null_percentage:.1f}% null values (approaching limit)"
        else:
            result = ValidationResult.PASS
            message = f"Column '{column}' completeness check passed"
        
        return self._create_result(
            result, message,
            records_checked=total_records,
            records_failed=null_count,
            details={
                'column': column,
                'null_count': int(null_count),
                'null_percentage': null_percentage,
                'threshold': max_null_percentage
            }
        )


class UniquenessCheck(BaseQualityCheck):
    """Check for data uniqueness"""
    
    def execute(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> QualityRuleResult:
        columns = self.rule.parameters.get('columns', [])
        if isinstance(columns, str):
            columns = [columns]
        
        if not columns or not all(col in df.columns for col in columns):
            return self._create_result(
                ValidationResult.SKIP,
                f"One or more columns {columns} not found in dataset"
            )
        
        total_records = len(df)
        unique_records = len(df[columns].drop_duplicates())
        duplicate_count = total_records - unique_records
        duplicate_percentage = (duplicate_count / total_records) * 100 if total_records > 0 else 0
        
        max_duplicate_percentage = self.rule.parameters.get('max_duplicate_percentage', 1.0)
        
        if duplicate_percentage > max_duplicate_percentage:
            result = ValidationResult.FAIL
            message = f"Columns {columns} have {duplicate_percentage:.1f}% duplicates (limit: {max_duplicate_percentage}%)"
        elif duplicate_percentage > (max_duplicate_percentage * 0.7):
            result = ValidationResult.WARNING
            message = f"Columns {columns} have {duplicate_percentage:.1f}% duplicates (approaching limit)"
        else:
            result = ValidationResult.PASS
            message = f"Columns {columns} uniqueness check passed"
        
        return self._create_result(
            result, message,
            records_checked=total_records,
            records_failed=duplicate_count,
            details={
                'columns': columns,
                'duplicate_count': duplicate_count,
                'duplicate_percentage': duplicate_percentage,
                'threshold': max_duplicate_percentage
            }
        )


class ValidityCheck(BaseQualityCheck):
    """Check data validity using various validators"""
    
    def execute(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> QualityRuleResult:
        column = self.rule.parameters.get('column')
        validation_type = self.rule.parameters.get('validation_type')
        pattern = self.rule.parameters.get('pattern')
        
        if not column or column not in df.columns:
            return self._create_result(
                ValidationResult.SKIP,
                f"Column '{column}' not found in dataset"
            )
        
        # Remove null values for validation
        non_null_data = df[column].dropna()
        total_records = len(non_null_data)
        
        if total_records == 0:
            return self._create_result(
                ValidationResult.SKIP,
                f"No non-null values in column '{column}' to validate"
            )
        
        failed_count = 0
        
        try:
            if validation_type == 'email':
                failed_count = sum(1 for email in non_null_data if not validators.email(str(email)))
            
            elif validation_type == 'url':
                failed_count = sum(1 for url in non_null_data if not validators.url(str(url)))
            
            elif validation_type == 'phone':
                failed_count = self._validate_phone_numbers(non_null_data)
            
            elif validation_type == 'regex' and pattern:
                regex = re.compile(pattern)
                failed_count = sum(1 for value in non_null_data if not regex.match(str(value)))
            
            elif validation_type == 'numeric':
                failed_count = sum(1 for value in non_null_data if not pd.api.types.is_numeric_dtype(type(value)))
            
            elif validation_type == 'date':
                failed_count = self._validate_dates(non_null_data)
            
            else:
                return self._create_result(
                    ValidationResult.SKIP,
                    f"Unknown validation type: {validation_type}"
                )
            
            failure_percentage = (failed_count / total_records) * 100
            max_failure_percentage = self.rule.parameters.get('max_failure_percentage', 5.0)
            
            if failure_percentage > max_failure_percentage:
                result = ValidationResult.FAIL
                message = f"Column '{column}' {validation_type} validation failed for {failure_percentage:.1f}% of records"
            elif failure_percentage > (max_failure_percentage * 0.7):
                result = ValidationResult.WARNING
                message = f"Column '{column}' {validation_type} validation warning: {failure_percentage:.1f}% failures"
            else:
                result = ValidationResult.PASS
                message = f"Column '{column}' {validation_type} validation passed"
            
            return self._create_result(
                result, message,
                records_checked=total_records,
                records_failed=failed_count,
                details={
                    'column': column,
                    'validation_type': validation_type,
                    'failure_percentage': failure_percentage,
                    'threshold': max_failure_percentage
                }
            )
            
        except Exception as e:
            return self._create_result(
                ValidationResult.FAIL,
                f"Validation error: {str(e)}"
            )
    
    def _validate_phone_numbers(self, data: pd.Series) -> int:
        """Validate phone numbers"""
        failed_count = 0
        for phone in data:
            try:
                phonenumbers.parse(str(phone), None)
            except:
                failed_count += 1
        return failed_count
    
    def _validate_dates(self, data: pd.Series) -> int:
        """Validate date formats"""
        failed_count = 0
        for date_value in data:
            try:
                pd.to_datetime(date_value)
            except:
                failed_count += 1
        return failed_count


class RangeCheck(BaseQualityCheck):
    """Check if numeric values are within expected ranges"""
    
    def execute(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> QualityRuleResult:
        column = self.rule.parameters.get('column')
        min_value = self.rule.parameters.get('min_value')
        max_value = self.rule.parameters.get('max_value')
        
        if not column or column not in df.columns:
            return self._create_result(
                ValidationResult.SKIP,
                f"Column '{column}' not found in dataset"
            )
        
        # Convert to numeric, coercing errors to NaN
        numeric_data = pd.to_numeric(df[column], errors='coerce').dropna()
        total_records = len(numeric_data)
        
        if total_records == 0:
            return self._create_result(
                ValidationResult.SKIP,
                f"No numeric values in column '{column}'"
            )
        
        failed_count = 0
        
        if min_value is not None:
            failed_count += sum(numeric_data < min_value)
        
        if max_value is not None:
            failed_count += sum(numeric_data > max_value)
        
        failure_percentage = (failed_count / total_records) * 100
        max_failure_percentage = self.rule.parameters.get('max_failure_percentage', 1.0)
        
        if failure_percentage > max_failure_percentage:
            result = ValidationResult.FAIL
            message = f"Column '{column}' range check failed for {failure_percentage:.1f}% of records"
        elif failure_percentage > (max_failure_percentage * 0.7):
            result = ValidationResult.WARNING
            message = f"Column '{column}' range check warning: {failure_percentage:.1f}% out of range"
        else:
            result = ValidationResult.PASS
            message = f"Column '{column}' range check passed"
        
        return self._create_result(
            result, message,
            records_checked=total_records,
            records_failed=failed_count,
            details={
                'column': column,
                'min_value': min_value,
                'max_value': max_value,
                'actual_min': float(numeric_data.min()),
                'actual_max': float(numeric_data.max()),
                'failure_percentage': failure_percentage
            }
        )


class BusinessRuleCheck(BaseQualityCheck):
    """Custom business rule validation"""
    
    def execute(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> QualityRuleResult:
        rule_expression = self.rule.parameters.get('expression')
        
        if not rule_expression:
            return self._create_result(
                ValidationResult.SKIP,
                "No business rule expression provided"
            )
        
        try:
            # Evaluate the expression (be careful with eval in production!)
            # In production, use a safer expression evaluator
            result_series = df.eval(rule_expression)
            
            total_records = len(result_series)
            failed_count = sum(~result_series)  # Count False values
            
            failure_percentage = (failed_count / total_records) * 100 if total_records > 0 else 0
            max_failure_percentage = self.rule.parameters.get('max_failure_percentage', 5.0)
            
            if failure_percentage > max_failure_percentage:
                result = ValidationResult.FAIL
                message = f"Business rule failed for {failure_percentage:.1f}% of records"
            elif failure_percentage > (max_failure_percentage * 0.7):
                result = ValidationResult.WARNING
                message = f"Business rule warning: {failure_percentage:.1f}% violations"
            else:
                result = ValidationResult.PASS
                message = f"Business rule validation passed"
            
            return self._create_result(
                result, message,
                records_checked=total_records,
                records_failed=failed_count,
                details={
                    'expression': rule_expression,
                    'failure_percentage': failure_percentage,
                    'threshold': max_failure_percentage
                }
            )
            
        except Exception as e:
            return self._create_result(
                ValidationResult.FAIL,
                f"Business rule evaluation error: {str(e)}"
            )


class DataQualityFramework:
    """
    Comprehensive data quality framework with profiling, validation, and scoring
    """
    
    def __init__(self, rules_config_path: str = None):
        self.logger = logging.getLogger(__name__)
        
        # Quality check implementations
        self.check_implementations = {
            QualityRuleType.COMPLETENESS: CompletenessCheck,
            QualityRuleType.UNIQUENESS: UniquenessCheck,
            QualityRuleType.VALIDITY: ValidityCheck,
            QualityRuleType.BUSINESS_RULE: BusinessRuleCheck,
        }
        
        # Load quality rules
        self.quality_rules: Dict[str, QualityRule] = {}
        if rules_config_path:
            self.load_rules_from_file(rules_config_path)
        else:
            self._create_default_rules()
        
        self.logger.info(f"Data Quality Framework initialized with {len(self.quality_rules)} rules")
    
    def _create_default_rules(self):
        """Create default quality rules"""
        
        default_rules = [
            QualityRule(
                rule_id="completeness_primary_key",
                name="Primary Key Completeness",
                description="Primary key columns should not have null values",
                rule_type=QualityRuleType.COMPLETENESS,
                severity=Severity.CRITICAL,
                column_pattern=".*_id$|.*id$|key$",
                parameters={'max_null_percentage': 0.0},
                tags=['primary_key', 'critical']
            ),
            QualityRule(
                rule_id="completeness_general",
                name="General Column Completeness",
                description="Columns should have minimal null values",
                rule_type=QualityRuleType.COMPLETENESS,
                severity=Severity.MEDIUM,
                parameters={'max_null_percentage': 5.0},
                tags=['general']
            ),
            QualityRule(
                rule_id="uniqueness_primary_key",
                name="Primary Key Uniqueness",
                description="Primary key values should be unique",
                rule_type=QualityRuleType.UNIQUENESS,
                severity=Severity.CRITICAL,
                column_pattern=".*_id$|.*id$|key$",
                parameters={'max_duplicate_percentage': 0.0},
                tags=['primary_key', 'critical']
            ),
            QualityRule(
                rule_id="validity_email",
                name="Email Format Validation",
                description="Email columns should contain valid email addresses",
                rule_type=QualityRuleType.VALIDITY,
                severity=Severity.MEDIUM,
                column_pattern=".*email.*",
                parameters={
                    'validation_type': 'email',
                    'max_failure_percentage': 2.0
                },
                tags=['email', 'format']
            ),
            QualityRule(
                rule_id="validity_phone",
                name="Phone Number Validation",
                description="Phone columns should contain valid phone numbers",
                rule_type=QualityRuleType.VALIDITY,
                severity=Severity.MEDIUM,
                column_pattern=".*phone.*|.*mobile.*|.*tel.*",
                parameters={
                    'validation_type': 'phone',
                    'max_failure_percentage': 5.0
                },
                tags=['phone', 'format']
            )
        ]
        
        for rule in default_rules:
            self.quality_rules[rule.rule_id] = rule
    
    def load_rules_from_file(self, file_path: str):
        """Load quality rules from configuration file"""
        
        try:
            with open(file_path, 'r') as f:
                if file_path.endswith('.json'):
                    rules_data = json.load(f)
                else:
                    import yaml
                    rules_data = yaml.safe_load(f)
            
            for rule_data in rules_data.get('rules', []):
                rule = QualityRule(**rule_data)
                self.quality_rules[rule.rule_id] = rule
            
            self.logger.info(f"Loaded {len(rules_data.get('rules', []))} quality rules from {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load rules from {file_path}: {str(e)}")
    
    def add_rule(self, rule: QualityRule):
        """Add a quality rule"""
        self.quality_rules[rule.rule_id] = rule
        self.logger.info(f"Added quality rule: {rule.rule_id}")
    
    def remove_rule(self, rule_id: str):
        """Remove a quality rule"""
        if rule_id in self.quality_rules:
            del self.quality_rules[rule_id]
            self.logger.info(f"Removed quality rule: {rule_id}")
    
    def get_applicable_rules(self, table_name: str, columns: List[str]) -> List[QualityRule]:
        """Get rules applicable to a specific table and columns"""
        
        applicable_rules = []
        
        for rule in self.quality_rules.values():
            if not rule.enabled:
                continue
            
            # Check table pattern
            if rule.table_pattern:
                if not re.match(rule.table_pattern, table_name):
                    continue
            
            # Check column pattern
            if rule.column_pattern:
                matching_columns = [col for col in columns if re.match(rule.column_pattern, col)]
                if not matching_columns:
                    continue
                
                # Add column parameter for rules that need it
                if rule.rule_type in [QualityRuleType.COMPLETENESS, QualityRuleType.VALIDITY]:
                    for column in matching_columns:
                        column_rule = QualityRule(
                            rule_id=f"{rule.rule_id}_{column}",
                            name=f"{rule.name} - {column}",
                            description=rule.description,
                            rule_type=rule.rule_type,
                            severity=rule.severity,
                            parameters={**rule.parameters, 'column': column},
                            fail_threshold=rule.fail_threshold,
                            warning_threshold=rule.warning_threshold
                        )
                        applicable_rules.append(column_rule)
                else:
                    applicable_rules.append(rule)
            else:
                applicable_rules.append(rule)
        
        return applicable_rules
    
    def profile_data(self, df: pd.DataFrame, table_name: str, sample_size: int = 100) -> DataProfileResult:
        """Generate comprehensive data profile"""
        
        start_time = datetime.now()
        
        total_records = len(df)
        total_columns = len(df.columns)
        
        # Analyze column types
        numeric_columns = len(df.select_dtypes(include=[np.number]).columns)
        text_columns = len(df.select_dtypes(include=['object', 'string']).columns)
        date_columns = len(df.select_dtypes(include=['datetime64', 'timedelta64']).columns)
        
        # Calculate null percentage
        null_count = df.isnull().sum().sum()
        total_cells = total_records * total_columns
        null_percentage = (null_count / total_cells) * 100 if total_cells > 0 else 0
        
        # Calculate duplicate percentage
        duplicate_count = len(df) - len(df.drop_duplicates())
        duplicate_percentage = (duplicate_count / total_records) * 100 if total_records > 0 else 0
        
        # Profile each column
        column_profiles = {}
        data_types = {}
        
        for column in df.columns:
            column_profile = self._profile_column(df[column])
            column_profiles[column] = column_profile
            data_types[column] = str(df[column].dtype)
        
        # Get sample data
        sample_data = []
        if total_records > 0:
            sample_df = df.head(min(sample_size, total_records))
            sample_data = sample_df.to_dict('records')
        
        profile_result = DataProfileResult(
            table_name=table_name,
            total_records=total_records,
            total_columns=total_columns,
            numeric_columns=numeric_columns,
            text_columns=text_columns,
            date_columns=date_columns,
            null_percentage=null_percentage,
            duplicate_percentage=duplicate_percentage,
            column_profiles=column_profiles,
            data_types=data_types,
            sample_data=sample_data
        )
        
        execution_time = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Data profiling completed for {table_name} in {execution_time:.2f} seconds")
        
        return profile_result
    
    def _profile_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile a single column"""
        
        profile = {
            'name': series.name,
            'type': str(series.dtype),
            'count': len(series),
            'null_count': series.isnull().sum(),
            'unique_count': series.nunique(),
            'null_percentage': (series.isnull().sum() / len(series)) * 100 if len(series) > 0 else 0
        }
        
        # Non-null values for analysis
        non_null = series.dropna()
        
        if len(non_null) > 0:
            if pd.api.types.is_numeric_dtype(series):
                profile.update({
                    'min': float(non_null.min()),
                    'max': float(non_null.max()),
                    'mean': float(non_null.mean()),
                    'median': float(non_null.median()),
                    'std': float(non_null.std()) if len(non_null) > 1 else 0,
                    'q25': float(non_null.quantile(0.25)),
                    'q75': float(non_null.quantile(0.75))
                })
            
            elif pd.api.types.is_string_dtype(series):
                profile.update({
                    'min_length': non_null.str.len().min(),
                    'max_length': non_null.str.len().max(),
                    'avg_length': non_null.str.len().mean(),
                    'most_common': non_null.value_counts().head(5).to_dict()
                })
            
            elif pd.api.types.is_datetime64_any_dtype(series):
                profile.update({
                    'min_date': non_null.min(),
                    'max_date': non_null.max(),
                    'date_range_days': (non_null.max() - non_null.min()).days
                })
        
        return profile
    
    def validate_data(self, df: pd.DataFrame, table_name: str, 
                     custom_rules: List[QualityRule] = None) -> List[QualityRuleResult]:
        """Run quality validation on a dataset"""
        
        start_time = datetime.now()
        
        # Get applicable rules
        if custom_rules:
            applicable_rules = custom_rules
        else:
            applicable_rules = self.get_applicable_rules(table_name, df.columns.tolist())
        
        results = []
        
        for rule in applicable_rules:
            try:
                # Get check implementation
                check_class = self.check_implementations.get(rule.rule_type)
                if not check_class:
                    self.logger.warning(f"No implementation for rule type: {rule.rule_type}")
                    continue
                
                # Execute check
                check_instance = check_class(rule)
                rule_start = datetime.now()
                result = check_instance.execute(df)
                rule_end = datetime.now()
                
                result.execution_time_seconds = (rule_end - rule_start).total_seconds()
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Error executing rule {rule.rule_id}: {str(e)}")
                error_result = QualityRuleResult(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    rule_type=rule.rule_type,
                    severity=rule.severity,
                    result=ValidationResult.FAIL,
                    message=f"Execution error: {str(e)}"
                )
                results.append(error_result)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Quality validation completed for {table_name}: "
                        f"{len(results)} rules executed in {execution_time:.2f} seconds")
        
        return results
    
    def calculate_quality_score(self, validation_results: List[QualityRuleResult], 
                               profile_result: DataProfileResult = None) -> Dict[str, Any]:
        """Calculate overall quality score"""
        
        if not validation_results:
            return {'overall_score': 0.0, 'breakdown': {}}
        
        # Weight by severity
        severity_weights = {
            Severity.CRITICAL: 4.0,
            Severity.HIGH: 3.0,
            Severity.MEDIUM: 2.0,
            Severity.LOW: 1.0
        }
        
        total_weight = 0.0
        weighted_score = 0.0
        
        # Category scores
        category_scores = {}
        
        for result in validation_results:
            weight = severity_weights[result.severity]
            total_weight += weight
            
            # Calculate rule score
            if result.result == ValidationResult.PASS:
                rule_score = 100.0
            elif result.result == ValidationResult.WARNING:
                rule_score = 70.0
            elif result.result == ValidationResult.FAIL:
                rule_score = max(0.0, result.success_rate)  # Use success rate for partial credit
            else:  # SKIP
                continue  # Don't count skipped rules
            
            weighted_score += rule_score * weight
            
            # Track by category
            category = result.rule_type.value
            if category not in category_scores:
                category_scores[category] = {'total_weight': 0, 'weighted_score': 0}
            
            category_scores[category]['total_weight'] += weight
            category_scores[category]['weighted_score'] += rule_score * weight
        
        # Calculate final scores
        overall_score = weighted_score / total_weight if total_weight > 0 else 0.0
        
        # Calculate category averages
        category_averages = {}
        for category, scores in category_scores.items():
            if scores['total_weight'] > 0:
                category_averages[category] = scores['weighted_score'] / scores['total_weight']
        
        # Factor in profile-based score if available
        if profile_result:
            profile_score = profile_result.get_quality_score()
            overall_score = (overall_score * 0.7) + (profile_score * 0.3)  # 70/30 weight
        
        return {
            'overall_score': round(overall_score, 2),
            'breakdown': {
                'validation_score': round(weighted_score / total_weight if total_weight > 0 else 0, 2),
                'profile_score': round(profile_result.get_quality_score() if profile_result else 0, 2),
                'category_scores': {k: round(v, 2) for k, v in category_averages.items()},
                'rules_executed': len(validation_results),
                'rules_passed': len([r for r in validation_results if r.result == ValidationResult.PASS]),
                'rules_failed': len([r for r in validation_results if r.result == ValidationResult.FAIL]),
                'rules_warning': len([r for r in validation_results if r.result == ValidationResult.WARNING])
            }
        }
    
    def generate_quality_report(self, table_name: str, validation_results: List[QualityRuleResult],
                               profile_result: DataProfileResult = None) -> Dict[str, Any]:
        """Generate comprehensive quality report"""
        
        quality_score = self.calculate_quality_score(validation_results, profile_result)
        
        # Group results by severity and outcome
        by_severity = {}
        by_outcome = {}
        
        for result in validation_results:
            # By severity
            sev = result.severity.value
            if sev not in by_severity:
                by_severity[sev] = []
            by_severity[sev].append(result)
            
            # By outcome
            outcome = result.result.value
            if outcome not in by_outcome:
                by_outcome[outcome] = []
            by_outcome[outcome].append(result)
        
        # Recommendations
        recommendations = self._generate_recommendations(validation_results, profile_result)
        
        report = {
            'table_name': table_name,
            'report_timestamp': datetime.now().isoformat(),
            'quality_score': quality_score,
            'profile_summary': asdict(profile_result) if profile_result else None,
            'validation_summary': {
                'total_rules': len(validation_results),
                'by_severity': {k: len(v) for k, v in by_severity.items()},
                'by_outcome': {k: len(v) for k, v in by_outcome.items()}
            },
            'failed_rules': [
                {
                    'rule_id': r.rule_id,
                    'rule_name': r.rule_name,
                    'severity': r.severity.value,
                    'message': r.message,
                    'details': r.details
                }
                for r in validation_results if r.result == ValidationResult.FAIL
            ],
            'recommendations': recommendations,
            'detailed_results': [asdict(r) for r in validation_results]
        }
        
        return report
    
    def _generate_recommendations(self, validation_results: List[QualityRuleResult],
                                 profile_result: DataProfileResult = None) -> List[str]:
        """Generate actionable recommendations based on quality results"""
        
        recommendations = []
        
        # Check for critical failures
        critical_failures = [r for r in validation_results 
                           if r.severity == Severity.CRITICAL and r.result == ValidationResult.FAIL]
        
        if critical_failures:
            recommendations.append(
                f"🚨 Address {len(critical_failures)} critical data quality issues immediately"
            )
        
        # Check for high null percentages
        if profile_result and profile_result.null_percentage > 20:
            recommendations.append(
                f"📊 High null percentage ({profile_result.null_percentage:.1f}%) - "
                "review data collection processes"
            )
        
        # Check for duplicate data
        if profile_result and profile_result.duplicate_percentage > 5:
            recommendations.append(
                f"🔍 High duplicate percentage ({profile_result.duplicate_percentage:.1f}%) - "
                "implement deduplication logic"
            )
        
        # Check for validation failures
        validity_failures = [r for r in validation_results 
                            if r.rule_type == QualityRuleType.VALIDITY and r.result == ValidationResult.FAIL]
        
        if validity_failures:
            recommendations.append(
                f"✅ Fix {len(validity_failures)} data format validation issues"
            )
        
        # Add general recommendations based on score
        overall_score = self.calculate_quality_score(validation_results, profile_result)['overall_score']
        
        if overall_score < 50:
            recommendations.append("🛠️ Data quality is poor - consider comprehensive data cleansing")
        elif overall_score < 80:
            recommendations.append("📈 Data quality needs improvement - focus on top issues")
        else:
            recommendations.append("✨ Good data quality - maintain current standards")
        
        return recommendations


# Usage Example
if __name__ == "__main__":
    # Create sample data
    sample_data = pd.DataFrame({
        'customer_id': [1, 2, 3, 4, 5, None],
        'email': ['john@example.com', 'invalid-email', 'jane@test.com', 'bob@company.org', None, 'alice@domain.com'],
        'phone': ['+1234567890', '555-0123', 'invalid', '+9876543210', None, '+1122334455'],
        'age': [25, 30, -5, 150, 35, 28],  # Some invalid ages
        'created_date': pd.to_datetime(['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01'])
    })
    
    # Initialize quality framework
    quality_framework = DataQualityFramework()
    
    # Profile the data
    profile = quality_framework.profile_data(sample_data, "customers")
    print(f"Data Profile Quality Score: {profile.get_quality_score():.1f}")
    
    # Add custom business rule
    business_rule = QualityRule(
        rule_id="age_range_check",
        name="Valid Age Range",
        description="Age should be between 0 and 120",
        rule_type=QualityRuleType.BUSINESS_RULE,
        severity=Severity.HIGH,
        parameters={
            'expression': 'age >= 0 and age <= 120',
            'max_failure_percentage': 5.0
        }
    )
    quality_framework.add_rule(business_rule)
    
    # Run validation
    validation_results = quality_framework.validate_data(sample_data, "customers")
    
    # Generate quality report
    report = quality_framework.generate_quality_report("customers", validation_results, profile)
    
    print(f"\nQuality Report:")
    print(f"Overall Score: {report['quality_score']['overall_score']}")
    print(f"Rules Executed: {report['validation_summary']['total_rules']}")
    print(f"Failed Rules: {len(report['failed_rules'])}")
    
    print(f"\nRecommendations:")
    for rec in report['recommendations']:
        print(f"  {rec}")
    
    # Print failed rules
    if report['failed_rules']:
        print(f"\nFailed Rules:")
        for rule in report['failed_rules']:
            print(f"  - {rule['rule_name']}: {rule['message']}")
"""
Database Validator for OMOP CDM
Validates database connection and OMOP schema presence
"""

import logging
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logger = logging.getLogger(__name__)


class DatabaseValidator:
    """Validates database connection and OMOP CDM schema"""
    
    # Core OMOP CDM tables that should exist
    REQUIRED_TABLES = [
        'person',
        'observation_period',
        'visit_occurrence',
        'condition_occurrence',
        'drug_exposure',
        'procedure_occurrence',
        'measurement',
        'observation',
        'concept',
        'vocabulary',
        'concept_relationship'
    ]
    
    def __init__(self, db_config: Dict[str, Any]):
        """Initialize validator with database configuration
        
        Args:
            db_config: Database configuration dictionary
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Establish database connection
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Build connection string based on dialect
            if self.db_config.get('dialect') == 'postgresql':
                self.connection = psycopg2.connect(
                    host=self.db_config['host'],
                    port=self.db_config.get('port', 5432),
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password']
                )
                self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                self.cursor = self.connection.cursor()
                logger.info(f"Connected to PostgreSQL database: {self.db_config['database']}")
                return True
            else:
                logger.error(f"Unsupported database dialect: {self.db_config.get('dialect')}")
                return False
                
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
    
    def validate_schema_exists(self) -> bool:
        """Check if the specified schema exists
        
        Returns:
            True if schema exists, False otherwise
        """
        if not self.cursor:
            logger.error("No database connection")
            return False
        
        schema = self.db_config.get('schema', 'public')
        
        try:
            if self.db_config.get('dialect') == 'postgresql':
                self.cursor.execute(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
                    (schema.lower(),)
                )
                result = self.cursor.fetchone()
                
                if result:
                    logger.info(f"Schema '{schema}' exists")
                    return True
                else:
                    logger.error(f"Schema '{schema}' not found")
                    return False
                    
        except Exception as e:
            logger.error(f"Error checking schema: {e}")
            return False
    
    def validate_omop_tables(self) -> Dict[str, bool]:
        """Check for presence of required OMOP CDM tables
        
        Returns:
            Dictionary mapping table name to existence status
        """
        if not self.cursor:
            logger.error("No database connection")
            return {}
        
        schema = self.db_config.get('schema', 'public')
        results = {}
        
        try:
            for table in self.REQUIRED_TABLES:
                if self.db_config.get('dialect') == 'postgresql':
                    self.cursor.execute(
                        """
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                        """,
                        (schema.lower(), table.lower())
                    )
                    result = self.cursor.fetchone()
                    results[table] = result is not None
                    
                    if not result:
                        logger.warning(f"OMOP table '{schema}.{table}' not found")
                    
        except Exception as e:
            logger.error(f"Error checking OMOP tables: {e}")
        
        return results
    
    def validate_table_columns(self, table: str, required_columns: List[str]) -> Dict[str, bool]:
        """Validate that a table has required columns
        
        Args:
            table: Table name
            required_columns: List of required column names
            
        Returns:
            Dictionary mapping column name to existence status
        """
        if not self.cursor:
            logger.error("No database connection")
            return {}
        
        schema = self.db_config.get('schema', 'public')
        results = {}
        
        try:
            for column in required_columns:
                if self.db_config.get('dialect') == 'postgresql':
                    self.cursor.execute(
                        """
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = %s 
                        AND table_name = %s 
                        AND column_name = %s
                        """,
                        (schema.lower(), table.lower(), column.lower())
                    )
                    result = self.cursor.fetchone()
                    results[column] = result is not None
                    
        except Exception as e:
            logger.error(f"Error checking columns for table {table}: {e}")
        
        return results
    
    def get_table_row_count(self, table: str) -> Optional[int]:
        """Get row count for a table
        
        Args:
            table: Table name
            
        Returns:
            Row count or None if error
        """
        if not self.cursor:
            logger.error("No database connection")
            return None
        
        schema = self.db_config.get('schema', 'public')
        
        try:
            qualified_table = f"{schema}.{table}"
            self.cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}")
            result = self.cursor.fetchone()
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting row count for {table}: {e}")
            return None
    
    def validate_full(self) -> Dict[str, Any]:
        """Perform full validation of database and OMOP schema
        
        Returns:
            Validation results dictionary
        """
        results = {
            'connection': False,
            'schema_exists': False,
            'tables': {},
            'table_counts': {},
            'summary': {
                'total_required_tables': len(self.REQUIRED_TABLES),
                'tables_found': 0,
                'tables_missing': 0,
                'tables_with_data': 0
            }
        }
        
        # Test connection
        if not self.connect():
            results['error'] = 'Failed to connect to database'
            return results
        
        results['connection'] = True
        
        # Check schema
        results['schema_exists'] = self.validate_schema_exists()
        
        if not results['schema_exists']:
            self.disconnect()
            results['error'] = f"Schema '{self.db_config.get('schema', 'public')}' not found"
            return results
        
        # Check OMOP tables
        results['tables'] = self.validate_omop_tables()
        
        # Count tables found/missing
        for table, exists in results['tables'].items():
            if exists:
                results['summary']['tables_found'] += 1
                
                # Get row count
                count = self.get_table_row_count(table)
                if count is not None:
                    results['table_counts'][table] = count
                    if count > 0:
                        results['summary']['tables_with_data'] += 1
            else:
                results['summary']['tables_missing'] += 1
        
        # Clean up
        self.disconnect()
        
        # Overall validation status
        results['valid'] = (
            results['connection'] and
            results['schema_exists'] and
            results['summary']['tables_found'] >= len(self.REQUIRED_TABLES) * 0.8  # 80% of tables present
        )
        
        return results
    
    def test_query(self, sql: str) -> Dict[str, Any]:
        """Test a SQL query against the database
        
        Args:
            sql: SQL query to test
            
        Returns:
            Query results or error information
        """
        if not self.connect():
            return {'error': 'Failed to connect to database'}
        
        try:
            self.cursor.execute(sql)
            
            # Get column names
            columns = [desc[0] for desc in self.cursor.description] if self.cursor.description else []
            
            # Get first few rows
            rows = self.cursor.fetchmany(10)
            
            result = {
                'success': True,
                'columns': columns,
                'row_count': len(rows),
                'sample_rows': rows
            }
            
        except Exception as e:
            result = {
                'success': False,
                'error': str(e)
            }
        
        finally:
            self.disconnect()
        
        return result
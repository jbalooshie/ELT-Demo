from django.db import connection
from django.conf import settings
from typing import List, Dict, Any
import logging
from ..models import DataFile, ValidationReport

logger = logging.getLogger(__name__)

class DataMover:
    def __init__(self, data_file: DataFile, validation_report: ValidationReport):
        self.data_file = data_file
        self.validation_report = validation_report
        self.source_schema = settings.DATABASE_SCHEMAS['RAW']
        self.target_schema = settings.DATABASE_SCHEMAS['VALIDATED']
        
    def move_validated_data(self) -> Dict[str, Any]:
        """
        Move data that passed validation to the validated schema
        """
        if not self.validation_report.passed:
            return {
                'success': False,
                'error': 'Cannot move data that failed validation'
            }
            
        try:
            # Get table names
            table_name = f"raw_{self.data_file.file_name.split('.')[0].lower()}"
            validated_table_name = f"validated_{self.data_file.file_name.split('.')[0].lower()}"
            
            with connection.cursor() as cursor:
                # Drop the validated table if it exists
                cursor.execute(f"""
                    DROP TABLE IF EXISTS {self.target_schema}."{validated_table_name}" CASCADE;
                """)
                
                # Create the table in validated schema
                cursor.execute(f"""
                    CREATE TABLE {self.target_schema}."{validated_table_name}" (
                        LIKE {self.source_schema}."{table_name}" INCLUDING ALL
                    );
                """)
                
                # Create a new sequence for the validated table
                cursor.execute(f"""
                    CREATE SEQUENCE IF NOT EXISTS {self.target_schema}."{validated_table_name}_id_seq";
                """)
                
                # Set the sequence as the default for the id column
                cursor.execute(f"""
                    ALTER TABLE {self.target_schema}."{validated_table_name}"
                    ALTER COLUMN id SET DEFAULT nextval('{self.target_schema}.{validated_table_name}_id_seq');
                """)
                
                # Move the data with a fresh ID sequence
                cursor.execute(f"""
                    INSERT INTO {self.target_schema}."{validated_table_name}" 
                    (SELECT (ROW_NUMBER() OVER ())::integer as id, 
                            {', '.join(f'"{col}"' for col in self._get_columns(table_name) if col != 'id')}
                     FROM {self.source_schema}."{table_name}");
                """)
                
                # Get the number of rows moved
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {self.target_schema}."{validated_table_name}";
                """)
                rows_moved = cursor.fetchone()[0]
                
            return {
                'success': True,
                'rows_moved': rows_moved,
                'source_table': table_name,
                'target_table': validated_table_name
            }
            
        except Exception as e:
            logger.error(f"Error moving data: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _get_columns(self, table_name: str) -> List[str]:
        """Get column names for a table"""
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{self.source_schema}' 
                AND table_name = '{table_name}'
                ORDER BY ordinal_position;
            """)
            return [row[0] for row in cursor.fetchall()]
            
    def cleanup_raw_data(self) -> Dict[str, Any]:
        """
        Cleanup data from raw schema after successful move
        """
        try:
            table_name = f"raw_{self.data_file.file_name.split('.')[0].lower()}"
            
            with connection.cursor() as cursor:
                # Drop the table and its dependent objects
                cursor.execute(f"""
                    DROP TABLE IF EXISTS {self.source_schema}."{table_name}" CASCADE;
                """)
                
            return {
                'success': True,
                'message': f'Successfully cleaned up table {table_name} from raw schema'
            }
            
        except Exception as e:
            logger.error(f"Error cleaning up raw data: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
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
            # Get table name
            table_name = f"raw_{self.data_file.file_name.split('.')[0].lower()}"
            validated_table_name = f"validated_{self.data_file.file_name.split('.')[0].lower()}"
            
            with connection.cursor() as cursor:
                # Create the table in validated schema if it doesn't exist
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.target_schema}."{validated_table_name}" (
                        LIKE {self.source_schema}."{table_name}" INCLUDING ALL
                    );
                """)
                
                # Move the data
                cursor.execute(f"""
                    INSERT INTO {self.target_schema}."{validated_table_name}"
                    SELECT * FROM {self.source_schema}."{table_name}";
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
    
    def cleanup_raw_data(self) -> Dict[str, Any]:
        """
        Optionally cleanup data from raw schema after successful move
        """
        try:
            table_name = f"raw_{self.data_file.file_name.split('.')[0].lower()}"
            
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    DROP TABLE IF EXISTS {self.source_schema}."{table_name}";
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
import pandas as pd
import numpy as np
from django.conf import settings
from django.db import connection
import logging
from typing import List, Dict, Any
import os
import chardet

logger = logging.getLogger(__name__)

class CSVProcessor:
    def __init__(self, file_path: str, chunk_size: int = 10000):
        """
        Initialize the CSV processor
        
        Args:
            file_path: Path to the CSV file
            chunk_size: Number of rows to process at once
        """
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.total_rows = 0
        self.processed_rows = 0
        self.schema_name = settings.DATABASE_SCHEMAS['RAW']
        
    def _detect_encoding(self) -> str:
        """Detect the file encoding"""
        with open(self.file_path, 'rb') as file:
            raw_data = file.read()
            result = chardet.detect(raw_data)
            return result['encoding'] or 'utf-8'
    
    def _read_csv_with_encoding(self) -> pd.DataFrame:
        """Try to read CSV with detected encoding"""
        try:
            encoding = self._detect_encoding()
            logger.info(f"Detected encoding: {encoding}")
            
            return pd.read_csv(
                self.file_path,
                encoding=encoding,
                sep=None,  # Let pandas detect the separator
                engine='python',  # More flexible but slower engine
                encoding_errors='replace'  # Replace invalid characters
            )
        except Exception as e:
            logger.error(f"Error reading CSV with detected encoding: {str(e)}")
            # Fallback to common encodings
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
            
            for enc in encodings:
                try:
                    logger.info(f"Trying encoding: {enc}")
                    return pd.read_csv(
                        self.file_path,
                        encoding=enc,
                        sep=None,
                        engine='python',
                        encoding_errors='replace'
                    )
                except Exception as e:
                    logger.error(f"Failed with encoding {enc}: {str(e)}")
                    continue
            
            raise ValueError("Unable to read CSV file with any supported encoding")

class CSVProcessor:
    def __init__(self, file_path: str, chunk_size: int = 10000):
        """
        Initialize the CSV processor
        
        Args:
            file_path: Path to the CSV file
            chunk_size: Number of rows to process at once
        """
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.total_rows = 0
        self.processed_rows = 0
        self.schema_name = settings.DATABASE_SCHEMAS['RAW']
        
    def _create_temp_table(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Create a temporary table based on DataFrame structure
        """
        dtype_mapping = {
            'object': 'TEXT',
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN'
        }
        
        columns = []
        for col, dtype in df.dtypes.items():
            sql_type = dtype_mapping.get(str(dtype), 'TEXT')
            columns.append(f'"{col}" {sql_type}')
            
        create_table_sql = f"""
        DROP TABLE IF EXISTS {self.schema_name}."{table_name}";
        CREATE TABLE {self.schema_name}."{table_name}" (
            id SERIAL PRIMARY KEY,
            {', '.join(columns)},
            _processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
    
    def _get_table_name(self) -> str:
        """Generate table name from file name"""
        base_name = os.path.splitext(os.path.basename(self.file_path))[0]
        return f"raw_{base_name.lower().replace(' ', '_')}"
        
    def process_file(self) -> Dict[str, Any]:
        """
        Process the CSV file in chunks and load into raw schema
        
        Returns:
            Dict containing processing statistics
        """
        try:
            # First pass to get total rows and create table
            total_rows = sum(1 for _ in open(self.file_path)) - 1  # subtract header
            self.total_rows = total_rows
            
            # Create iterator for processing in chunks
            chunks = pd.read_csv(
                self.file_path,
                chunksize=self.chunk_size,
                low_memory=False,
                on_bad_lines='warn',
                sep=';'
            )
            
            # Process first chunk to get structure and create table
            first_chunk = next(chunks)
            table_name = self._get_table_name()
            self._create_temp_table(first_chunk, table_name)
            
            # Process first chunk
            self._process_chunk(first_chunk, table_name)
            self.processed_rows += len(first_chunk)
            
            # Process remaining chunks
            for chunk in chunks:
                self._process_chunk(chunk, table_name)
                self.processed_rows += len(chunk)
                
            return {
                'success': True,
                'total_rows': self.total_rows,
                'processed_rows': self.processed_rows,
                'table_name': table_name
            }
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _process_chunk(self, df: pd.DataFrame, table_name: str) -> None:
        """Process a single chunk of data"""
        # Replace NaN with None for proper SQL NULL values
        df = df.replace({np.nan: None})
        
        # Prepare the INSERT statement
        columns = [f'"{col}"' for col in df.columns]
        placeholders = ["%s" for _ in columns]
        
        insert_sql = f"""
        INSERT INTO {self.schema_name}."{table_name}"
        ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        """
        
        # Execute the INSERT
        with connection.cursor() as cursor:
            cursor.executemany(insert_sql, df.values.tolist())
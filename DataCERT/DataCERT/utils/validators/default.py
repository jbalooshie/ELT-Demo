from typing import Dict, Any
import pandas as pd
import numpy as np
from .base import BaseValidator

class DefaultValidator(BaseValidator):
    """
    Default validator that checks for:
    - Missing values
    - Data type consistency
    - Basic data quality rules
    """
    def __init__(self, data_file, expected_types: Dict[str, str] = None):
        super().__init__(data_file)
        self.expected_types = {
            'Username': 'text',
            'Identifier': 'numeric',
            'First name': 'text',
            'Last name': 'text'
        }
    
    def validate(self) -> bool:
        """
        Perform validation on the data
        """
        chunk_size = 1000
        for chunk in self.get_table_data(chunk_size):
            self._validate_chunk(chunk)
            self.processed_rows += len(chunk)
            
        return len(self.errors) == 0
    
    def _validate_chunk(self, df: pd.DataFrame) -> None:
        """Validate a chunk of data"""
        # Check for missing values
        self._check_missing_values(df)
        
        # Check data types
        self._check_data_types(df)
    
    def _check_missing_values(self, df: pd.DataFrame) -> None:
        """Check for missing values in the DataFrame"""
        for column in df.columns:
            mask = df[column].isna()
            if mask.any():
                missing_rows = df[mask].index.tolist()
                for row_idx in missing_rows:
                    self.add_error(
                        row_number=row_idx + 1,  # +1 for human-readable row numbers
                        column_name=column,
                        error_message=f"Missing value in column {column}",
                        raw_data=df.iloc[row_idx].to_dict()
                    )
    
    def _check_data_types(self, df: pd.DataFrame) -> None:
        """Check data types against expected types"""
        for column, expected_type in self.expected_types.items():
            if column not in df.columns:
                continue
                
            try:
                if expected_type == 'numeric':
                    pd.to_numeric(df[column], errors='raise')
                elif expected_type == 'datetime':
                    pd.to_datetime(df[column], errors='raise')
                elif expected_type == 'boolean':
                    df[column].astype(bool)
            except (ValueError, TypeError) as e:
                invalid_rows = df[~df[column].isna()].index.tolist()
                for row_idx in invalid_rows:
                    self.add_error(
                        row_number=row_idx + 1,
                        column_name=column,
                        error_message=f"Invalid {expected_type} value: {df.iloc[row_idx][column]}",
                        raw_data=df.iloc[row_idx].to_dict()
                    )
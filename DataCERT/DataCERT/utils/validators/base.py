from abc import ABC, abstractmethod
from typing import Dict, List, Any
import pandas as pd
from django.db import connection
from ...models import ValidationReport, ValidationError, DataFile

class BaseValidator(ABC):
    """
    Abstract base class for all validators
    """
    def __init__(self, data_file: DataFile):
        self.data_file = data_file
        self.errors: List[Dict[str, Any]] = []
        self.processed_rows = 0
        
    @abstractmethod
    def validate(self) -> bool:
        """
        Implement validation logic here
        Returns: bool indicating if validation passed
        """
        pass
    
    def add_error(self, row_number: int, column_name: str, error_message: str, raw_data: Dict):
        """Add an error to the error list"""
        self.errors.append({
            'row_number': row_number,
            'column_name': column_name,
            'error_message': error_message,
            'raw_data': raw_data
        })
    
    def save_validation_results(self) -> ValidationReport:
        """Save validation results to database"""
        # Create validation report
        report = ValidationReport.objects.create(
            data_file=self.data_file,
            passed=len(self.errors) == 0,
            error_count=len(self.errors),
            summary=f"Processed {self.processed_rows} rows, found {len(self.errors)} errors."
        )
        
        # Create validation errors
        ValidationError.objects.bulk_create([
            ValidationError(
                report=report,
                row_number=error['row_number'],
                column_name=error['column_name'],
                error_message=error['error_message'],
                raw_data=error['raw_data']
            ) for error in self.errors
        ])
        
        return report

    def get_table_data(self, chunk_size: int = 1000) -> pd.DataFrame:
        """Get data from the raw schema in chunks"""
        table_name = f"raw_{self.data_file.file_name.split('.')[0].lower()}"
        query = f"""
        SELECT * FROM raw."{table_name}"
        """
        return pd.read_sql(query, connection, chunksize=chunk_size)
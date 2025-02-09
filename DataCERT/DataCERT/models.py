from django.db import models
from django.db.models import JSONField
from django.utils import timezone

class DataFile(models.Model):
    """
    Tracks uploaded files and their processing status
    """
    file_name = models.CharField(max_length=255)
    upload_date = models.DateTimeField(default=timezone.now)
    status = models.CharField(
        max_length=20,
        choices=[
            ('uploaded', 'Uploaded'),
            ('validating', 'Validation In Progress'),
            ('validated', 'Validation Complete'),
            ('failed', 'Validation Failed'),
        ],
        default='uploaded'
    )
    row_count = models.IntegerField(null=True)
    
    def __str__(self):
        return f"{self.file_name} ({self.status})"

class ValidationReport(models.Model):
    """
    Stores validation results for each file
    """
    data_file = models.ForeignKey(DataFile, on_delete=models.CASCADE)
    validation_date = models.DateTimeField(default=timezone.now)
    passed = models.BooleanField()
    error_count = models.IntegerField(default=0)
    summary = models.TextField()

class ValidationError(models.Model):
    """
    Stores individual validation errors
    """
    report = models.ForeignKey(ValidationReport, on_delete=models.CASCADE)
    row_number = models.IntegerField()
    column_name = models.CharField(max_length=255)
    error_message = models.TextField()
    raw_data = JSONField()  # Stores the problematic row as JSON
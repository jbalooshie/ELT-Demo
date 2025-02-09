from django.shortcuts import render, redirect, get_object_or_404, get_list_or_404
from django.views import View
from django.views.generic import DetailView
from django.contrib import messages
from .utils.validators.base import BaseValidator
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.db.models import Count
from django.core.files.storage import FileSystemStorage
from .forms import CSVUploadForm, ValidationForm
from .models import DataFile, ValidationError, ValidationReport
from .utils.csv_processor import CSVProcessor
from .utils.validators.default import DefaultValidator
from .utils.data_mover import DataMover
import importlib.util
import sys
from io import StringIO
import os

class CSVUploadView(View):
    template_name = 'upload.html'

    def get(self, request):
        form = CSVUploadForm()
        return render(request, self.template_name, {'form': form})

    def post(self, request):
        form = CSVUploadForm(request.POST, request.FILES)
        if form.is_valid():
            # Create a temporary storage for the file
            fs = FileSystemStorage()
            
            # Get the uploaded file
            file = request.FILES['file']
            
            # Save the file to disk
            filename = fs.save(f'csv_uploads/{file.name}', file)
            
            # Create DataFile record
            data_file = DataFile.objects.create(
                file_name=file.name,
                status='uploaded',
            )
            
            # Process the file
            try:
                processor = CSVProcessor(fs.path(filename))
                result = processor.process_file()
                
                if result['success']:
                    data_file.status = 'uploaded'
                    data_file.row_count = result['processed_rows']
                    data_file.save()
                    
                    messages.success(
                        request,
                        f'File processed successfully. {result["processed_rows"]} rows imported.'
                    )
                else:
                    data_file.status = 'failed'
                    data_file.save()
                    messages.error(
                        request,
                        f'Error processing file: {result.get("error", "Unknown error")}'
                    )
            except Exception as e:
                data_file.status = 'failed'
                data_file.save()
                messages.error(request, f'Error processing file: {str(e)}')
            return redirect('file_list')  # We'll create this view later
            
        return render(request, self.template_name, {'form': form})
    
class ValidationView(View):
    template_name = 'validate.html'
    
    def get(self, request):
        form = ValidationForm()
        return render(request, self.template_name, {'form': form})
    
    def post(self, request):
        form = ValidationForm(request.POST)
        if form.is_valid():
            data_file = form.cleaned_data['data_file']
            validator_type = form.cleaned_data['validator_type']
            
            try:
                if validator_type == 'default':
                    validator = DefaultValidator(data_file)
                else:
                    # Create custom validator from code
                    custom_code = form.cleaned_data['custom_validator_code']
                    validator = self._create_custom_validator(custom_code, data_file)
                
                # Update file status
                data_file.status = 'validating'
                data_file.save()
                
                # Run validation
                validation_passed = validator.validate()
                
                # Save results
                report = validator.save_validation_results()
                
                # Update file status
                data_file.status = 'validated' if validation_passed else 'failed'
                data_file.save()
                
                return redirect('validation_report', report_id=report.id)
                
            except Exception as e:
                messages.error(request, f'Validation error: {str(e)}')
                data_file.status = 'failed'
                data_file.save()
        
        return render(request, self.template_name, {'form': form})
    
    def _create_custom_validator(self, code: str, data_file) -> BaseValidator:
        """Create a custom validator from code string"""
        # Create a unique module name
        module_name = f'custom_validator_{data_file.id}'
        
        # Create a module spec
        spec = importlib.util.spec_from_loader(
            module_name,
            loader=None,
            origin='custom validator'
        )
        
        # Create a new module based on the spec
        module = importlib.util.module_from_spec(spec)
        
        # Add necessary imports to the code
        code = f"""
from typing import Dict, Any
import pandas as pd
import numpy as np
from app.utils.validators.base import BaseValidator

{code}
        """
        
        # Execute the code in the module
        exec(code, module.__dict__)
        
        # Find the validator class in the module
        validator_class = None
        for item in module.__dict__.values():
            if (isinstance(item, type) and 
                issubclass(item, BaseValidator) and 
                item != BaseValidator):
                validator_class = item
                break
        
        if not validator_class:
            raise ValueError('No valid validator class found in custom code')
        
        return validator_class(data_file)

class ValidationReportView(DetailView):
    model = ValidationReport
    template_name = 'validation_report.html'
    context_object_name = 'report'
    pk_url_kwarg = 'report_id'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Get validation errors with pagination
        page = self.request.GET.get('page', 1)
        errors_per_page = 50
        
        # Get all errors for this report
        errors = ValidationError.objects.filter(
            report=self.object
        ).order_by('row_number')
        
        # Create paginator
        paginator = Paginator(errors, errors_per_page)
        try:
            validation_errors = paginator.page(page)
        except PageNotAnInteger:
            validation_errors = paginator.page(1)
        except EmptyPage:
            validation_errors = paginator.page(paginator.num_pages)

        # Add error summary by column
        error_summary = (
            errors.values('column_name')
            .annotate(error_count=Count('id'))
            .order_by('-error_count')
        )

        context.update({
            'validation_errors': validation_errors,
            'error_summary': error_summary,
        })
        
        return context

class MoveToValidatedView(View):
    def post(self, request, report_id):
        report = get_object_or_404(ValidationReport, id=report_id)
        
        # Only move data that passed validation
        if not report.passed:
            messages.error(
                request,
                'Cannot move data to validated schema. Validation failed.'
            )
            return redirect('validation_report', report_id=report_id)
        
        # Move the data
        mover = DataMover(report.data_file, report)
        result = mover.move_validated_data()
        
        if result['success']:
            # Optionally cleanup raw data
            cleanup_result = mover.cleanup_raw_data()
            if cleanup_result['success']:
                messages.success(
                    request,
                    f"Successfully moved {result['rows_moved']} rows to validated schema "
                    f"and cleaned up raw data."
                )
            else:
                messages.warning(
                    request,
                    f"Data moved successfully but cleanup failed: {cleanup_result['error']}"
                )
        else:
            messages.error(
                request,
                f"Failed to move data: {result.get('error', 'Unknown error')}"
            )
            
        return redirect('validation_report', report_id=report_id)
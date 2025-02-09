from django import forms
from .models import DataFile

class CSVUploadForm(forms.ModelForm):
    file = forms.FileField(
        label='Select a CSV file',
        help_text='Max file size: 2GB. CSV files only.',
        widget=forms.FileInput(attrs={
            'class': 'form-control',
            'accept': '.csv'
        })
    )

    class Meta:
        model = DataFile
        fields = ['file_name']
        widgets = {
            'file_name': forms.TextInput(attrs={'class': 'form-control'})
        }

    def clean_file(self):
        file = self.cleaned_data.get('file')
        if file:
            if not file.name.endswith('.csv'):
                raise forms.ValidationError('Only CSV files are allowed.')
            if file.size > 2 * 1024 * 1024 * 1024:  # 2GB limit
                raise forms.ValidationError('File size must be under 2GB.')
        return file

class ValidationForm(forms.Form):
    data_file = forms.ModelChoiceField(
        queryset=DataFile.objects.filter(status='uploaded'),
        label='Select File to Validate',
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    
    validator_type = forms.ChoiceField(
        choices=[
            ('default', 'Default Validator (Types & Missing Data)'),
            ('custom', 'Custom Validator')
        ],
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    
    custom_validator_code = forms.CharField(
        widget=forms.Textarea(attrs={
            'class': 'form-control',
            'rows': 10,
            'placeholder': 'Paste your custom validator code here...'
        }),
        required=False,
        help_text='If using custom validator, paste your code here.'
    )
    
    def clean(self):
        cleaned_data = super().clean()
        validator_type = cleaned_data.get('validator_type')
        custom_code = cleaned_data.get('custom_validator_code')
        
        if validator_type == 'custom' and not custom_code:
            raise forms.ValidationError(
                'Custom validator code is required when using custom validator type.'
            )
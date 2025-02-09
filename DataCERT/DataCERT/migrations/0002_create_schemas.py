from django.db import migrations

class Migration(migrations.Migration):
    dependencies = [
        ('DataCERT', '0001_initial'),  # Updated from 'your_app_name' to 'DataCERT'
    ]

    def create_schemas(apps, schema_editor):
        with schema_editor.connection.cursor() as cursor:
            cursor.execute('CREATE SCHEMA IF NOT EXISTS raw;')
            cursor.execute('CREATE SCHEMA IF NOT EXISTS validated;')

    def reverse_schemas(apps, schema_editor):
        with schema_editor.connection.cursor() as cursor:
            cursor.execute('DROP SCHEMA IF EXISTS raw CASCADE;')
            cursor.execute('DROP SCHEMA IF EXISTS validated CASCADE;')

    operations = [
        migrations.RunPython(create_schemas, reverse_schemas)
    ]
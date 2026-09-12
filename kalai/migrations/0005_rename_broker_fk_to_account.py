# Generated manually for renaming broker FK to account in ProcessedTickStore and AlgoInfo

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('kalai', '0004_account_api_provider_support'),
    ]

    operations = [
        migrations.RenameField(
            model_name='processedtickstore',
            old_name='broker',
            new_name='account',
        ),
        migrations.RenameField(
            model_name='algoinfo',
            old_name='broker',
            new_name='account',
        ),
        migrations.AlterUniqueTogether(
            name='algoinfo',
            unique_together={('account', 'tablename')},
        ),
    ]

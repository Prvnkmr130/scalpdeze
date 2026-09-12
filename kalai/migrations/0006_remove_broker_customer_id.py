# Generated manually to remove redundant customer_id field from Broker model

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('kalai', '0005_rename_broker_fk_to_account'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='broker',
            name='customer_id',
        ),
    ]

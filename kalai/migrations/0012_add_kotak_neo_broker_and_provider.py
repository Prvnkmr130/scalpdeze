from django.db import migrations


def populate_kotak_choices(apps, schema_editor):
    BrokerType = apps.get_model('kalai', 'BrokerType')
    ApiProvider = apps.get_model('kalai', 'ApiProvider')

    # Ensure only Kotak Neo is present
    BrokerType.objects.get_or_create(code='kotak_neo', defaults={'name': 'Kotak Neo'})
    ApiProvider.objects.get_or_create(code='kotak_neo', defaults={'name': 'Kotak Neo API'})

    # Clean up redundant 'kotak' entries if they exist
    BrokerType.objects.filter(code='kotak').delete()
    ApiProvider.objects.filter(code='kotak').delete()


class Migration(migrations.Migration):

    dependencies = [
        ('kalai', '0011_broker_models_update'),
    ]

    operations = [
        migrations.RunPython(populate_kotak_choices, reverse_code=migrations.RunPython.noop),
    ]

from django.db import migrations


def populate_delta_exchange_choices(apps, schema_editor):
    BrokerType = apps.get_model('kalai', 'BrokerType')
    ApiProvider = apps.get_model('kalai', 'ApiProvider')

    BrokerType.objects.get_or_create(code='delta_exchange', defaults={'name': 'Delta Exchange'})
    BrokerType.objects.get_or_create(code='delta', defaults={'name': 'Delta Exchange (Global)'})
    BrokerType.objects.get_or_create(code='delta_india', defaults={'name': 'Delta Exchange (India)'})

    ApiProvider.objects.get_or_create(code='delta_exchange', defaults={'name': 'Delta Exchange API'})
    ApiProvider.objects.get_or_create(code='delta', defaults={'name': 'Delta Exchange API (Global)'})
    ApiProvider.objects.get_or_create(code='delta_india', defaults={'name': 'Delta Exchange API (India)'})


def remove_delta_exchange_choices(apps, schema_editor):
    BrokerType = apps.get_model('kalai', 'BrokerType')
    ApiProvider = apps.get_model('kalai', 'ApiProvider')

    BrokerType.objects.filter(code__in=['delta_exchange', 'delta', 'delta_india']).delete()
    ApiProvider.objects.filter(code__in=['delta_exchange', 'delta', 'delta_india']).delete()


class Migration(migrations.Migration):

    dependencies = [
        ('kalai', '0023_exchangemasterdata'),
    ]

    operations = [
        migrations.RunPython(populate_delta_exchange_choices, reverse_code=remove_delta_exchange_choices),
    ]

from django.db import migrations


def populate_coinswitch_choices(apps, schema_editor):
    BrokerType = apps.get_model('kalai', 'BrokerType')
    ApiProvider = apps.get_model('kalai', 'ApiProvider')

    BrokerType.objects.get_or_create(code='coinswitch', defaults={'name': 'CoinSwitch PRO'})
    ApiProvider.objects.get_or_create(code='coinswitch', defaults={'name': 'CoinSwitch PRO API'})


def remove_coinswitch_choices(apps, schema_editor):
    BrokerType = apps.get_model('kalai', 'BrokerType')
    ApiProvider = apps.get_model('kalai', 'ApiProvider')

    BrokerType.objects.filter(code='coinswitch').delete()
    ApiProvider.objects.filter(code='coinswitch').delete()


class Migration(migrations.Migration):

    dependencies = [
        ('kalai', '0013_algolog_and_more'),
    ]

    operations = [
        migrations.RunPython(populate_coinswitch_choices, reverse_code=remove_coinswitch_choices),
    ]

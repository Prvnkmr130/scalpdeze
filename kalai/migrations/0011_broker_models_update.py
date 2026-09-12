from django.db import migrations, models
import django.db.models.deletion

def populate_broker_choices(apps, schema_editor):
    BrokerType = apps.get_model('kalai', 'BrokerType')
    ApiProvider = apps.get_model('kalai', 'ApiProvider')
    
    BROKER_CHOICES = [
        ('zerodha', 'Zerodha'),
        ('uplink', 'Uplink'),
        ('angel', 'Angel Broking'),
        ('coindcx', 'CoinDCX'),
        ('delta', 'Delta Exchange'),
        ('tradovate', 'Tradovate'),
        ('edgeclear', 'EdgeClear'),
        ('apex', 'Apex Trader Funding'),
        ('other', 'Other Broker'),
    ]
    API_PROVIDER_CHOICES = [
        ('zerodha', 'Zerodha Kite API'),
        ('uplink', 'Uplink API'),
        ('angel', 'Angel Broking API'),
        ('coindcx', 'CoinDCX API'),
        ('delta', 'Delta Exchange API'),
        ('tradovate', 'Tradovate API'),
    ]
    
    for code, name in BROKER_CHOICES:
        BrokerType.objects.get_or_create(code=code, defaults={'name': name})
        
    for code, name in API_PROVIDER_CHOICES:
        ApiProvider.objects.get_or_create(code=code, defaults={'name': name})

class Migration(migrations.Migration):

    dependencies = [
        ('kalai', '0010_alter_broker_ws_operating_days'),
    ]

    operations = [
        migrations.CreateModel(
            name='ApiProvider',
            fields=[
                ('code', models.CharField(help_text="e.g. 'zerodha', 'angel'", max_length=50, primary_key=True, serialize=False)),
                ('name', models.CharField(help_text="e.g. 'Zerodha Kite API', 'Angel Broking API'", max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='BrokerType',
            fields=[
                ('code', models.CharField(help_text="e.g. 'zerodha', 'angel'", max_length=50, primary_key=True, serialize=False)),
                ('name', models.CharField(help_text="e.g. 'Zerodha', 'Angel Broking'", max_length=100)),
            ],
        ),
        migrations.RunPython(populate_broker_choices, reverse_code=migrations.RunPython.noop),
        migrations.AlterField(
            model_name='broker',
            name='api_provider',
            field=models.ForeignKey(blank=True, db_column='api_provider', help_text='Select API feed provider. If left unselected, trading and websocket connections will be disabled.', null=True, on_delete=django.db.models.deletion.SET_NULL, to='kalai.apiprovider'),
        ),
        migrations.AlterField(
            model_name='broker',
            name='broker_name',
            field=models.ForeignKey(db_column='broker_name', default='zerodha', help_text='Brokerage firm name', on_delete=django.db.models.deletion.PROTECT, to='kalai.brokertype'),
        ),
    ]

# Generated manually for Account & API Provider support

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('kalai', '0003_broker_access_token_required_alter_broker_name'),
    ]

    operations = [
        migrations.AddField(
            model_name='broker',
            name='account_id',
            field=models.CharField(blank=True, help_text='Unique Account ID', max_length=100, null=True, unique=True),
        ),
        migrations.AddField(
            model_name='broker',
            name='broker_name',
            field=models.CharField(choices=[('zerodha', 'Zerodha'), ('uplink', 'Uplink'), ('angel', 'Angel Broking'), ('coindcx', 'CoinDCX'), ('delta', 'Delta Exchange'), ('tradovate', 'Tradovate'), ('edgeclear', 'EdgeClear'), ('apex', 'Apex Trader Funding'), ('other', 'Other Broker')], default='zerodha', help_text='Brokerage firm name', max_length=100),
        ),
        migrations.AddField(
            model_name='broker',
            name='api_provider',
            field=models.CharField(blank=True, choices=[('zerodha', 'Zerodha Kite API'), ('uplink', 'Uplink API'), ('angel', 'Angel Broking API'), ('coindcx', 'CoinDCX API'), ('delta', 'Delta Exchange API'), ('tradovate', 'Tradovate API')], help_text='Select API feed provider. If left unselected, trading and websocket connections will be disabled.', max_length=100, null=True),
        ),
        migrations.AlterField(
            model_name='broker',
            name='api_key',
            field=models.CharField(blank=True, max_length=500, null=True),
        ),
        migrations.AlterField(
            model_name='broker',
            name='name',
            field=models.CharField(help_text='Account name or identifier', max_length=100, unique=True),
        ),
    ]

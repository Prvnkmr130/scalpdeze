from django.apps import AppConfig

class KalaiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'kalai'

    def ready(self):
        import sys
        # Skip DB queries during non-database management commands or test runs
        if any(cmd in " ".join(sys.argv) for cmd in ['collectstatic', 'compilemessages', 'makemigrations', 'pytest', 'test']):
            return

        try:
            from django_q.models import Schedule
            from django.utils import timezone
            from datetime import timedelta, time, datetime
            import zoneinfo

            def _get_next_ist_run(hour: int, minute: int = 0):
                tz = zoneinfo.ZoneInfo("Asia/Kolkata")
                now_ist = timezone.now().astimezone(tz)
                target_today = datetime.combine(now_ist.date(), time(hour, minute), tzinfo=tz)
                if now_ist >= target_today:
                    return target_today + timedelta(days=1)
                return target_today

            # Clean up legacy daily cleanup schedules if present
            Schedule.objects.filter(name__in=[
                'Daily Processed Ticks Cleanup',
                'Daily System Logs Cleanup',
                'Evening Processed Ticks Cleanup (04:00 PM IST)',
                'Evening System Logs Cleanup (04:05 PM IST)',
            ]).delete()

            # 1. Morning Pre-Market Cleanup (08:30 AM & 08:35 AM IST)
            ticks_morning, _ = Schedule.objects.get_or_create(
                name='Morning Processed Ticks Cleanup (08:30 AM IST)',
                defaults={
                    'func': 'kalai.tasks.clear_old_ticks',
                    'kwargs': 'days=2',
                    'schedule_type': Schedule.DAILY,
                    'repeats': -1,
                    'next_run': _get_next_ist_run(8, 30),
                }
            )
            if not ticks_morning.kwargs:
                ticks_morning.kwargs = 'days=2'
                ticks_morning.save(update_fields=['kwargs'])

            logs_morning, _ = Schedule.objects.get_or_create(
                name='Morning System Logs Cleanup (08:35 AM IST)',
                defaults={
                    'func': 'kalai.tasks.clear_old_logs',
                    'kwargs': 'days=2',
                    'schedule_type': Schedule.DAILY,
                    'repeats': -1,
                    'next_run': _get_next_ist_run(8, 35),
                }
            )
            if not logs_morning.kwargs:
                logs_morning.kwargs = 'days=2'
                logs_morning.save(update_fields=['kwargs'])

            # 2. Midnight Post-Market Cleanup (12:00 AM & 12:15 AM IST)
            ticks_midnight, _ = Schedule.objects.get_or_create(
                name='Midnight Processed Ticks Cleanup (12:00 AM IST)',
                defaults={
                    'func': 'kalai.tasks.clear_old_ticks',
                    'kwargs': 'days=2',
                    'schedule_type': Schedule.DAILY,
                    'repeats': -1,
                    'next_run': _get_next_ist_run(0, 0),
                }
            )
            if not ticks_midnight.kwargs:
                ticks_midnight.kwargs = 'days=2'
                ticks_midnight.save(update_fields=['kwargs'])

            logs_midnight, _ = Schedule.objects.get_or_create(
                name='Midnight System Logs Cleanup (12:15 AM IST)',
                defaults={
                    'func': 'kalai.tasks.clear_old_logs',
                    'kwargs': 'days=2',
                    'schedule_type': Schedule.DAILY,
                    'repeats': -1,
                    'next_run': _get_next_ist_run(0, 15),
                }
            )
            if not logs_midnight.kwargs:
                logs_midnight.kwargs = 'days=2'
                logs_midnight.save(update_fields=['kwargs'])

            # 3. Schedule the create_daily_pnl_snapshots task (Daily at 00:05 AM IST)
            pnl_sched, created = Schedule.objects.get_or_create(
                func='kalai.tasks.create_daily_pnl_snapshots',
                defaults={
                    'name': 'Daily Multi-Broker P&L Snapshot Archival',
                    'schedule_type': Schedule.DAILY,
                    'repeats': -1,
                    'next_run': _get_next_ist_run(0, 5),
                }
            )

            # 4. Schedule the evaluate_websocket_schedules task (every 5 minutes)
            import os
            check_interval = int(os.getenv("WS_SCHEDULER_INTERVAL_MINUTES", "5"))
            ws_sched, created = Schedule.objects.get_or_create(
                func='kalai.tasks.evaluate_websocket_schedules',
                defaults={
                    'name': 'WebSocket Time-Based Scheduler',
                    'schedule_type': Schedule.MINUTES,
                    'minutes': check_interval,
                    'repeats': -1,
                }
            )

            # 5. Schedule the sync_all_broker_positions task (every 1 minute)
            pos_sched, _ = Schedule.objects.get_or_create(
                func='kalai.tasks.sync_all_broker_positions',
                defaults={
                    'name': '1-Minute Multi-Broker Positions Sync',
                    'schedule_type': Schedule.MINUTES,
                    'minutes': 1,
                    'repeats': -1,
                }
            )
        except Exception:
            # Safely ignore during initial migrations or if django_q tables are missing
            pass

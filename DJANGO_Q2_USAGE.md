# Django Q2 Scheduled Tasks Usage Guide

This document explains how to use Django Q2 in this project to schedule recurring background tasks.

## 1. Accessing Scheduled Tasks
You can view, edit, or create scheduled tasks directly from the Django Admin Panel:
1. Navigate to your Django Admin URL (e.g., `http://<server-ip>/d4f8g9h2j1m5k8p3/`).
2. Scroll down to the **Django Q** section.
3. Click on **Scheduled tasks**.

## 2. Creating a New Scheduled Task (UI)
When you click **"Add scheduled task"** in the Django Admin, you will see a form. The essential fields are:

### Mandatory Fields
- **Name**: A human-readable identifier (e.g., `Daily Log Cleanup`).
- **Func**: The full Python import path to the function to execute (e.g., `kalai.tasks.clear_old_logs`). This function must exist in your codebase.
- **Schedule Type**: How often the task should run (Options: Once, Hourly, Daily, Weekly, Monthly, Quarterly, Yearly, or Cron).

### Important Optional Fields
- **Repeats**: Enter `-1` if you want the task to run indefinitely (forever). If left blank for a "Once" task, it runs just 1 time.
- **Next Run**: The date and time for the first execution. Defaults to the current time if left blank.
- **Cron**: A standard cron expression (e.g., `0 8 * * 1-5`) if you selected "Cron" as the Schedule Type.
- **Args / Kwargs**: If your Python function requires inputs, enter them here (e.g., `kwargs`: `user_id=1, force=True`).

## 3. Scheduled Tasks Configuration
The default recurring tasks are automatically bootstrapped and maintained in [`kalai/apps.py`](file:///c:/Users/Admin/Documents/deltazero26/kalai/apps.py) on application startup:

- **Morning Pre-Market Cleanup (08:30 AM & 08:35 AM IST)**:
  - `Morning Processed Ticks Cleanup`: Runs at 08:30 AM IST (clears old tick records before market open).
  - `Morning System Logs Cleanup`: Runs at 08:35 AM IST (clears old system & algo logs before market open).
- **Midnight Post-Market Cleanup (12:00 AM & 12:15 AM IST)**:
  - `Midnight Processed Ticks Cleanup`: Runs at 12:00 AM midnight IST (prunes accumulated ticks after all market sessions close).
  - `Midnight System Logs Cleanup`: Runs at 12:15 AM midnight IST (prunes intraday session logs).
- **Multi-Broker P&L Snapshot Archival**: Runs daily at 00:05 AM IST.
- **WebSocket Time-Based Scheduler**: Evaluates active websocket connections every 5 minutes.
- **Positions Sync**: Synchronizes broker open positions every 1 minute.

## 4. Chunked Database Cleanup & CPU Throttling
DeltaZero26 tasks prune old logs and processed ticks in **bounded chunks with sleep pauses** to prevent CPU spikes, table locks, and WAL saturation on PostgreSQL:
- **`kalai.tasks.clear_old_logs`**:
  - `days`: Retention threshold in days (default: 2).
  - `chunk_size`: Number of records deleted per batch (default: 5000).
  - `sleep_interval`: Seconds paused between batches (default: 0.05s).
  - Deletes from `system_logs` via physical `ctid` (Tid Scan) and `kalai_algolog` via indexed `id`.
- **`kalai.tasks.clear_old_ticks`**:
  - `days`: Retention threshold in days (default: 2).
  - `chunk_size`: Number of records deleted per batch (default: 5000).
  - `sleep_interval`: Seconds paused between batches (default: 0.05s).
  - Deletes from `kalai_processedtickstore` via indexed `id`.

## 5. Architecture Notes
- The Q cluster runs as a background process (`qcluster`) managed by `supervisord` inside the monolithic Docker container.
- It uses the local PostgreSQL database (ORM broker) as its message queue, requiring zero external dependencies like Redis.


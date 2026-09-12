"""
algo_trading.brokers
────────────────────
Async multi-broker WebSocket engine with per-broker pipeline isolation.

Each enabled broker gets its own:
    Feed  →  asyncio.Queue  →  TickConsumer  →  {broker}_stream_kv table

All brokers share a single log pipeline → system_logs table.

Start via:
    python manage.py run_ws_engine
"""

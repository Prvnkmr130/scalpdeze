# DeltaZero26 Documentation

Welcome to the documentation for **DeltaZero26**, a multi-broker algorithmic trading platform engineered for high performance and low latency, specifically optimized for deployment on resource-constrained cloud instances.

This documentation serves as a comprehensive reference guide to the system architecture, environment configuration, feature set, and internal code mechanics.

## Table of Contents

1.  **[System Architecture](architecture.md)**
    - Overview of the monolithic design.
    - Process management with `supervisord`.
    - Service interactions (Nginx, PostgreSQL, Django, WebSockets, Algo Engine).

2.  **[Environment & Deployment](environment_and_deployment.md)**
    - Dependencies and requirements.
    - Environment variable configuration (`.env`).
    - Automated deployment and server hardening scripts.
    - Docker containerization setup.
    - **[Complete Server Installation Guide](installation_guide.md)**: VPS setup, permissions, passphrase-free SSH deploy keys, and troubleshooting.

3.  **Features & Code Flow**
    - **[Broker Management](features/broker_management.md)**: OAuth authentication, API credentials, and trading schedules.
    - **[WebSocket Engine](features/websocket_engine.md)**: Real-time tick data ingestion and processing pipeline.
    - **[Algorithmic Engine Suite](features/algo_engine.md)**: Polars trading strategy architecture and multi-broker execution engines.
    - **[CoinSwitch PRO Polars Options Engine](features/coinswitch_opt_engine.md)**: 24/7 crypto options, perpetual futures, and spot trading with Polars vectorized analytics.
    - **[Delta Exchange Polars Options Engine](features/delta_opt_engine.md)**: 24/7 crypto options & perpetual futures execution with dynamic strike resolution on Delta Exchange.
    - **[Zerodha Order Utilities](features/zerodha_utils.md)**: Order execution, portfolio inspection, and margin calculations.
    - **[Kotak Neo Order Utilities](features/kotak_utils.md)**: Kotak Neo REST order execution, ScripMaster token normalization, and portfolio inspections.
    - **[CoinDCX Order Utilities](features/coindcx_utils.md)**: Crypto Spot & Futures order execution, portfolio balances, and candles.
    - **[CoinSwitch Order Utilities](features/coinswitch_utils.md)**: CoinSwitch PRO Spot, Perpetual Futures, and DMA Options execution, Ed25519 authentication, and WebSocket streams.
    - **[Delta Exchange Order Utilities](features/delta_utils.md)**: Delta Exchange REST execution, clock drift synchronization, position tracking, and real-time tick streaming.
    - **[Multi-Broker Positions & Historical P&L Analytics](features/positions_and_pnl.md)**: Real-time positions tracking, 1-minute throttled zero-API sync, 1+ year trade audit retention, and multi-period P&L inference.
    - **[Data Hub & Cloud Synchronizer](features/data_hub_and_sync.md)**: Unified multi-dataset exporter, zero-lock PostgreSQL streaming, ExchangeMasterData isolation, and rate-limited Cloud-to-Local cloner.
    - **[Lightweight Candle Chart Visualizer](features/candle_visualizer.md)**: Offline, zero-API financial candle workstation (TradingView Lightweight Charts v4) with pure symbol mapping via `cum_table`, fullscreen mode, and strict `_all` table filtering.
    - **[Cloud Deployment & Git Operations Hub](features/deployment_hub.md)**: Out-of-container Linux host deployment workstation with live Git repository telemetry, ANSI terminal log streaming, and zero-impact manual deployment preservation.
    - **[Certbot, SSL & Domain Configuration Guide](features/certbot_and_ssl.md)**: Zero-downtime dual-stage SSL termination, DNS prerequisites, Let's Encrypt automated verification, and renewal commands.
    - **[Django Admin Dashboard & Live Telemetry Event Hub](features/admin_dashboard_hub.md)**: Modernized full-width admin dashboard with collapsible module sections, real-time search filtering, live system telemetry, and responsive, lightweight mobile layout optimizations.
    - **[Adding a New Broker Guide](features/adding_new_broker.md)**: Step-by-step developer procedure for adding new brokers, WebSockets, order utilities, and Django migrations.

4.  **[Code Reference](code_reference.md)**
    - Detailed documentation of core utility classes, middlewares, algorithms, and the broker registry.

5.  **[Database Schema](database_schema.md)**
    - Detailed breakdown of all database models, composite indexes, and data relationships.

6.  **[System Guardrails (Do's & Don'ts)](guardrails.md)**
    - Mandatory architecture guidelines, coding patterns, logging standards, performance thresholds, and security guardrails.

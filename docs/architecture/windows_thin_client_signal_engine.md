# Windows Thin Client Quantitative Signal Engine (U-Exchange)
## Architectural Blueprint & Technical Specification

- **Module**: DeltaZero26 Quantitative Intelligence Engine
- **Target Platform**: Windows 10/11 Local Thin Client PC
- **Target Market**: U-Exchange (US Equities & Options — `America/New_York`)
- **Execution Mode**: Pure Market Intelligence & Multi-Account Signal Generation (Zero Order API, Zero Paper Trading)
- **Data Ingestion**: Scrapling Stealth Browser Sniffing (Patchright/Playwright)

---

## 1. Executive Summary

This architecture adapts DeltaZero26 into an autonomous, resilient, multi-account **Quantitative Market Intelligence and Signal Generation Engine** engineered to run locally on a **Windows Thin Client PC**. 

The system operates without external broker WebSocket streaming feeds and without broker Order Management APIs. Instead, it deploys a stealth browser sniffer powered by **`scrapling`** (built on top of Patchright/Playwright) to continuously capture market trades, order books, option chains, and quotes from a user-configured web portal URL.

### Key Tenets
1. **Multi-Broker, Multi-Account Structure Preserved**: Supports multiple brokers with multiple linked user accounts through isolated browser contexts pooled within a single Chromium instance.
2. **U-Exchange Operating Timing**: Enforces US market hours (`America/New_York`, standard trading 09:30–16:00 ET, pre-market 04:00–09:30 ET, Mon–Fri, US holiday calendar).
3. **Dual-Network Interface Failover**: Monitors internet health every 5 seconds and dynamically switches Windows network adapter routing (Primary Ethernet ↔ Backup Wi-Fi/4G) with anti-flapping hysteresis.
4. **12+ Hour Session Durability**: Automates login with TOTP 2FA (`pyotp`), manages persistent user profiles, and performs staggered soft memory recycles to prevent Chromium RAM leaks.
5. **PC Power Lifecycle**: Coordinates Windows Task Scheduler RTC Wake Timers (`WakeToRun`) to wake the PC before market open and automatically hibernates the PC (`shutdown /h`) post-market.
6. **Windows Desktop Integration**: Features a native System Tray (SysTray) applet, audible Windows Toast notifications, and an emergency global hotkey (`Ctrl + Alt + B`) for instant browser visibility toggling.
7. **Thin Client Hardware Protection**: Employs WMI CPU thermal monitoring with adaptive loop throttling and Playwright media/asset stripping to minimize RAM and CPU load.

---

## 2. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph Power & System Watchdog [Windows OS Layer]
        RTC["Windows Task Scheduler (RTC Wake 09:15 ET)"] -->|Wake PC| Watchdog["run_watchdog.ps1 (Supervisor)"]
        Watchdog --> EngineBoot["Boot Django & Signal Engine"]
        EOD_Event["Market Close & Post-Trade Complete (16:15 ET)"] --> Hibernate["Windows Hibernate (shutdown /h)"]
    end

    subgraph Network Resiliency [Dual-NIC Failover Manager]
        Probe["Connectivity Health Probe (5s Interval)"]
        NIC_Primary["Primary Adapter (Ethernet LAN)"]
        NIC_Backup["Backup Adapter (Wi-Fi / 4G Dongle)"]
        Probe -->|Ping Test| GateCheck{"Primary Healthy?"}
        GateCheck -->|Yes| NIC_Primary
        GateCheck -->|Fail (3 retries)| Failover["PowerShell Metric Switch (Set-NetIPInterface)"]
        Failover --> NIC_Backup
        Failover --> TriggerReconnect["Signal Browser Session Reconnect"]
        Probe -->|Stable 60s| RevertPrimary["Revert Metric to Primary NIC"]
    end

    subgraph Desktop Experience [Windows Native UX]
        SysTray["SysTray Applet (pystray)"]
        ToastAlert["Windows Toast Notifications (windows-toasts)"]
        HotKey["Global Hotkey: Ctrl+Alt+B"] -->|Toggle Show/Hide| BrowserInstance
        SysTray -->|Status Tooltip / Menu| EngineBoot
    end

    subgraph Browser Ingestion Layer [Scrapling Stealth Engine]
        BrowserInstance["Single Chromium Process (Asset Stripped)"]
        BrowserInstance --> Ctx1["Context: Broker A (Account 1 Profile)"]
        BrowserInstance --> Ctx2["Context: Broker B (Account 2 Profile)"]
        Ctx1 & Ctx2 --> AuthCheck{"Authenticated?"}
        AuthCheck -->|No| AutoLogin["Automated Login + TOTP 2FA (pyotp)"]
        AuthCheck -->|Yes| Sniffer["Multi-Channel Sniffer"]
        AutoLogin --> Sniffer
        TriggerReconnect --> Sniffer
        Sniffer -->|page.on('websocket')| WS_Frames["WebSocket Frames (Trades/LTP)"]
        Sniffer -->|page.on('response')| HTTP_JSON["XHR/REST JSON (Option Chain)"]
        Sniffer -->|Adaptive Selector| DOM_Fallback["DOM Extractor (Fallback)"]
        BrowserInstance -->|Every 4 Hours| SoftRecycle["Soft Page/Memory Recycle"]
    end

    subgraph Quantitative Engine [DeltaZero26 Core Analytics]
        WS_Frames & HTTP_JSON & DOM_Fallback --> Normalizer["Canonical Polars Normalizer"]
        Normalizer --> Tier1["Tier 1: Shared Market Signals"]
        Tier1 --> SIMD_Candles["SIMD O(1) Candle Engine (3m base to 1D)"]
        SIMD_Candles --> StrikeDetect["Dynamic Strike Detection (est_strike = avg + dist)"]
        SIMD_Candles --> HeikinAshi["Heikin-Ashi & Momentum Paths 1 to 13_2"]
        StrikeDetect & HeikinAshi --> Tier2["Tier 2: Multi-Account Signal Fan-Out"]
    end

    subgraph Output & Dispatch [Local Sinks]
        Tier2 --> ToastAlert
        Tier2 --> SysTray
        Tier2 --> Webhook["Telegram / Discord Webhooks (Optional)"]
        Tier2 --> DB_Log["kalai_algolog (ISO Standardized Telemetry)"]
        SIMD_Candles --> DB_Info["kalai_algoinfo (Multi-Timeframe Candles)"]
        DB_Info --> Visualizer["Django Admin Candle Visualizer (localhost:8000)"]
        EOD_Event --> ExcelReport["Daily EOD Excel Report Export"]
    end
```

---

## 3. Multi-Broker & Multi-Account Architecture

DeltaZero26 maintains its multi-broker, multi-account capability without spinning up heavy separate browser instances.

### 3.1 Isolated Browser Context Pooling
Instead of launching separate Chromium instances for each broker or account, the sniffer uses Playwright's **Isolated Browser Contexts** (`BrowserContext`):
* **Single Chromium Process**: Only one underlying `chrome.exe` process runs, saving up to 70% RAM and CPU on the thin client.
* **Complete Session Isolation**: Each broker/account has its own dedicated context with separate cookies, local storage, and cache (`storage_state=f"profiles/{account_id}.json"`).
* **Zero Session Bleed**: Broker A (e.g. Account 1) and Broker B (e.g. Account 2) never share state, cookies, or credentials.

### 3.2 Two-Tier Analytical Model
1. **Tier 1 (Shared Market Calculation)**:
   * Underlying reference quotes (SPY, QQQ, AAPL, etc.) and master option chains are scraped and normalized once per 2.0s cycle.
   * Resampled candles (3m base, 5m, 10m, 15m, 30m, 60m, 1D) are calculated in Polars SIMD.
   * Shared momentum evaluation and baseline ATM strike detection execute centrally.
2. **Tier 2 (Per-Account Rule Matching & Signal Fan-Out)**:
   * Each active account registered in Django (`Broker` / `UserAccount` model) evaluates the Tier 1 market signals against its own parameters:
     * **Preference & Capital Share**: Configured via `token_ref.xlsx` or Django Admin.
     * **Strike Distance**: Specific OTM/ATM offsets (`Strike_dist_CE`, `Strike_dist_PE`).
     * **Filter Thresholds**: Minimum/maximum lot sizing, lower/upper price limits.
     * **Account Notification Routing**: Assigns target account tags to generated alerts.

---

## 4. Hardware & Thermal Protection for Thin Clients

Thin client PCs (e.g. Intel Celeron, Pentium, Core i3/i5 T-series, or AMD Ryzen Embedded) often use compact cases or passive heatsinks. The engine includes safeguards to prevent hardware fatigue.

### 4.1 Multi-Threaded Polars SIMD Tuning
* Eliminates restrictive cloud container ceilings (`POLARS_MAX_THREADS=2`, `OPENBLAS_NUM_THREADS=1`).
* Unlocks physical CPU multi-threading across available cores for sub-second Polars downsampling and indicator evaluations.
* Allocates dedicated lightweight threads for:
  1. Main Analytical & Resampling Loop (2.0s interval).
  2. Browser Network Sniffer & Frame Processing.
  3. Network Failover Watchdog & Health Prober (5.0s interval).

### 4.2 Proactive Thermal Watchdog & Adaptive Throttling
* Queries Windows WMI (`root\wmi:MSAcpi_ThermalZoneTemperature`) or `psutil` sensors every 30 seconds.
* **Adaptive Throttle Matrix**:
  * **Normal (<75°C)**: Standard 2.0s execution cycle.
  * **Elevated (75°C–82°C)**: Logs thermal warning; relaxes execution loop to 3.5s.
  * **Critical (>82°C)**: Temporarily pauses non-essential DOM parsing, extends loop to 5.0s, and issues a desktop thermal alert.

### 4.3 Aggressive Resource & Media Stripping
* Intercepts network routes in Chromium via `page.route("**/*", handler)`:
  * Aborts: `images`, `media`, `fonts`, `stylesheets (non-critical)`, `trackers`, and `ad scripts`.
  * Reduces network bandwidth usage by up to **80%**.
  * Eliminates GPU rasterization overhead, keeping thin client RAM usage sub-800MB.

---

## 5. Dual-Network Interface Failover with Hysteresis

To protect against local broadband or Wi-Fi interruptions during market hours:

### 5.1 Failover Mechanism
* **Interfaces**:
  * **Primary (Metric 10)**: Ethernet LAN (High speed, wired).
  * **Backup (Metric 50)**: Wi-Fi or 4G/5G USB Dongle (Secondary uplink).
* **Continuous Health Probing**:
  * Probes external DNS (`8.8.8.8`, `1.1.1.1`) and target exchange endpoints every 5 seconds.
  * If 3 consecutive probes fail on Primary:
    1. Executes PowerShell via `subprocess`:
       ```powershell
       Set-NetIPInterface -InterfaceAlias "$PrimaryAlias" -InterfaceMetric 60
       Set-NetIPInterface -InterfaceAlias "$BackupAlias" -InterfaceMetric 10
       Clear-DnsClientCache
       ```
    2. Emits an urgent log: `[NETWORK_FAILOVER] Primary connection dropped. Switched to backup adapter: $BackupAlias`.
    3. Notifies `ScraplingBrowserSniffer` to re-establish WebSocket and network listeners.

### 5.2 Anti-Flapping Hysteresis
* Avoids rapidly toggling between adapters if the primary connection is fluttering.
* When the primary adapter recovers, it must sustain **60 continuous seconds of zero packet loss** before the engine gracefully restores primary metric priority.

---

## 6. 12+ Hour Session Durability & Auto-Login

To sustain uninterrupted streaming over 12+ hour market days (including pre-market and post-market):

### 6.1 Automated Login with TOTP 2FA (`pyotp`)
* Detects login screens (via URL redirection or landing DOM selectors).
* Injects encrypted credentials from local `.env`.
* Generates time-based 6-digit 2FA tokens on the fly using `pyotp.TOTP(secret).now()`.
* Automatically submits 2FA inputs and confirms landing page readiness.

### 6.2 Staggered Soft Memory Garbage Collection
* Chromium processes naturally accumulate memory leaks over 12 hours from high-volume DOM mutations and WebSocket buffers.
* **Soft Recycle Routine** (every 4 hours, or during 12:00–12:30 ET low-volatility period):
  1. Engine preserves current rolling candles and strike tables in `kalai_algoinfo`.
  2. Reloads the target page (`page.reload(wait_until="domcontentloaded")`) or recycles the specific `BrowserContext`.
  3. Re-attaches network sniffers within 1.5 seconds, clearing the Chromium heap without missing candle bars.

---

## 7. Windows Power Lifecycle: Auto-Wake & Auto-Hibernate

Eliminates the need for manual power management on the thin client PC:

### 7.1 RTC Auto-Wake (Pre-Market Preparation)
* Registers a Windows Task Scheduler task using `Register-ScheduledTask` with:
  ```powershell
  $Settings = New-ScheduledTaskSettingsSet -WakeToRun -AllowStartIfOnBatteries
  ```
* Configured to wake the PC on trading days (Mon–Fri) at **09:15 ET** (or **03:45 ET** if pre-market analysis is enabled).
* Automatically checks the U-Exchange holiday calendar to keep the PC asleep on exchange holidays.

### 7.2 Post-Market Auto-Hibernate (`shutdown /h`)
* When the session reaches post-market conclusion (**16:15 ET**):
  1. Signals browser contexts to close cleanly.
  2. Flushes all pending logs to `kalai_algolog` and persists EOD candles to `kalai_algoinfo`.
  3. Exports the Daily EOD Excel Report.
  4. Computes the wake timestamp for the next trading day and updates the Task Scheduler wake timer.
  5. Puts the PC into hibernation via `shutdown /h` or `rundll32.exe powrprof.dll,SetSuspendState 0,1,0`.

### 7.3 Startup Supervisor (`run_watchdog.ps1`)
* Placed in Windows Startup (`shell:startup`) or invoked by Task Scheduler on boot/wake:
  * Verifies local PostgreSQL service readiness.
  * Launches `python manage.py run_scraped_algo`.
  * Monitors the Python process; automatically restarts it if an unexpected exception occurs.

---

## 8. Windows-Native Desktop Integration & Alerting

### 8.1 System Tray (SysTray) Applet (`pystray`)
* Resides in the Windows notification area near the taskbar clock.
* **Visual States**:
  * 🟢 **Green**: Market Open, active streaming, network healthy.
  * 🟡 **Yellow**: Standby (Pre-market / Lunch / Post-market).
  * 🔴 **Red**: Network failover triggered / Browser disconnected.
* **Right-Click Context Menu**:
  * *Toggle Browser Window (Show / Hide)*
  * *Switch Network Interface (Primary ↔ Backup)*
  * *Open Live Candle Visualizer (Browser)*
  * *Recycle Browser Memory Now*
  * *Force PC Hibernate*

### 8.2 Windows Toast Notifications with Audio Cues
* Pushes native Windows 10/11 desktop notifications using `windows-toasts` on high-conviction signals:
  * **Notification Content**:
    * Title: `[SIGNAL ALERT] SPY — CALL BUY DETECTED`
    * Body: `Strike: 550 CE | LTP: $3.20 | Signal: +1 | Path: 13_1 | Account: ACC_01`
    * Audio: System alert chime to ensure immediate awareness.
    * Buttons: `[Open Visualizer]` and `[Dismiss]`.

### 8.3 Global Emergency Hotkey (`Ctrl + Alt + B`)
* Powered by `keyboard` or Windows `RegisterHotKey` API:
  * Instantly toggles the Chromium browser window between **Visible** (`headful`) and **Hidden** (`headless`).
  * If a broker or cloud security provider suddenly displays an unexpected interactive CAPTCHA, pressing `Ctrl + Alt + B` reveals the browser window so you can solve it manually. Pressing it again hides the window without interrupting execution.

---

## 9. U-Exchange Session & Market Hours Engine

### 9.1 Timing Bounds (`America/New_York`)
* **Pre-Market Window**: `04:00:00` to `09:30:00` ET (Optional warm-up and gap analysis).
* **Regular Trading Hours (RTH)**: `09:30:00` to `16:00:00` ET (Active 2.0s analytical loop).
* **Post-Market Settlement**: `16:00:00` to `16:15:00` ET (Final candle close, EOD sync, report generation).
* **Off-Hours / Night**: `16:15:00` to `09:15:00` ET next day (PC in hibernation).

### 9.2 Holiday & Weekend Gate
* Handles automatic Daylight Saving Time shifts (EST / EDT).
* Skips Saturdays and Sundays.
* Skips US market holidays: New Year's Day, Martin Luther King Jr. Day, Washington's Birthday, Good Friday, Memorial Day, Juneteenth, Independence Day, Labor Day, Thanksgiving Day, Christmas Day.

---

## 10. Quantitative Analytics & Telemetry Parity

The core algorithmic mathematics from DeltaZero26 remain 100% intact:

### 10.1 SIMD Multi-Timeframe Candle Resampling
* Ingests sniffed raw ticks directly into 3-minute base candles (`fwd_3_all`).
* Incrementally aggregates higher timeframes ($O(1)$ memory):
  $$\text{fwd\_5}, \text{fwd\_10}, \text{fwd\_15}, \text{fwd\_30}, \text{fwd\_60}, \text{day\_cdl\_all}$$
* Strips broker summary rolling OHLC to prevent daily price skew.

### 10.2 Dynamic Strike Detection
* Calculates candle average (`candle_avg`) across reference 3m bars.
* Adds directional distance offsets:
  $$\text{est\_strike\_CE} = \text{candle\_avg} + \text{Strike\_dist\_CE}$$
  $$\text{est\_strike\_PE} = \text{candle\_avg} + \text{Strike\_dist\_PE}$$
* Selects candidate ATM/OTM Call (`CE`) and Put (`PE`) strikes from the scraped option chain table.

### 10.3 Technical Momentum Evaluation
* Computes Heikin-Ashi candles and rolling price extremes (`olhc_max`, `olhc_min`).
* Evaluates directional momentum (Paths 1 to 13_2) to resolve:
  * `buy_signal_CE` (-1, 0, 1)
  * `buy_signal_PE` (-1, 0, 1)
  * `CE_jump` (0 or 1)
  * `PE_jump` (0 or 1)

### 10.4 Standardized ISO Algologs & Django Visualizer
* Emits ISO-timestamped records with prefix tags:
  * `[MOM_SIGNAL]`: Instrument LTP, High/Low range, CE/PE signal values.
  * `[MARKET_SIGNALS]`: Ingested ticks, active contracts evaluated.
  * `[CYCLE_SUMMARY]`: Iteration latency, active accounts fanned out.
* Updates `kalai_algoinfo` so the Django Admin Candle Visualizer (`/admin/kalai/candle-visualizer/`) renders live IST/ET charts and signal markers.

### 10.5 Daily EOD Excel Auto-Export
* At 16:05 ET, generates `C:\deltazero\reports\YYYY-MM-DD_U_Exchange_Signals.xlsx` containing:
  * **Signals Sheet**: All generated buy/sell/jump signals with timestamps and account tags.
  * **Candles Summary**: Session Open, High, Low, Close, and Heikin-Ashi values.
  * **Network & Health Log**: Uptime, failover switches, and thermal metrics.

---

## 11. Module Architecture & File Layout

```
deltazero26/
├── algo_trading/
│   ├── algos/
│   │   ├── indian_candle_engine.py      # SIMD incremental O(1) multi-timeframe candle engine
│   │   ├── u_exchange_session.py        # [NEW] US market hours, holidays, and session bounds
│   │   ├── scraped_signal_engine.py     # [NEW] Multi-account quantitative momentum & strike engine
│   │   └── logger.py                    # ISO timestamped batch database logger
│   └── brokers/
│       └── sniffer/
│           ├── __init__.py
│           ├── browser_sniffer.py       # [NEW] Scrapling stealth sniffer & context pooling
│           ├── network_failover.py      # [NEW] Dual-NIC health prober & metric switcher
│           ├── power_manager.py         # [NEW] Windows RTC wake & hibernate automation
│           ├── thermal_guard.py         # [NEW] WMI CPU thermal monitor & loop throttler
│           └── tray_applet.py           # [NEW] Windows System Tray icon & desktop toast dispatcher
├── docs/
│   └── architecture/
│       └── windows_thin_client_signal_engine.md  # [THIS SPECIFICATION]
├── kalai/
│   └── management/
│       └── commands/
│           └── run_scraped_algo.py      # [NEW] Unified launcher command
├── run_watchdog.ps1                     # [NEW] Windows PowerShell startup supervisor
├── pyproject.toml                       # Dependencies (scrapling, pyotp, windows-toasts, pystray)
└── .env.example                         # Hardware, network, and exchange configuration
```

---

## 12. Implementation Roadmap

| Phase | Milestone | Deliverables |
| :---: | :--- | :--- |
| **1** | **Dependencies & Environment Setup** | Add `scrapling[fetchers]`, `pyotp`, `windows-toasts`, `pystray`, `psutil` to `pyproject.toml`. Update `.env.example`. |
| **2** | **U-Exchange Session & Power Engine** | Implement `u_exchange_session.py` (ET market bounds, holidays) and `power_manager.py` (RTC wake timer, hibernation). |
| **3** | **Dual-NIC Network Failover Manager** | Implement `network_failover.py` with 5s ping probing, PowerShell metric switching, and 60s stability hysteresis. |
| **4** | **Scrapling Browser Sniffer & Context Pool** | Implement `browser_sniffer.py` with context pooling, media stripping, WebSocket/HTTP frame interception, and TOTP auto-login. |
| **5** | **Quantitative Signal Engine (Decoupled)** | Implement `scraped_signal_engine.py` using SIMD candles, dynamic strike detection, momentum paths, and Tier-2 account fan-out. |
| **6** | **Windows Desktop Integration (SysTray & Toasts)** | Implement `tray_applet.py` (SysTray icon, hotkey `Ctrl+Alt+B`, Windows Toast alerts, EOD Excel export). |
| **7** | **Orchestrator & Supervisor Script** | Implement `run_scraped_algo.py` and `run_watchdog.ps1` for autonomous startup, recovery, and shutdown. |
| **8** | **Integration Testing & Benchmarking** | Execute test suite on local thin client, verifying failover, thermal throttling, and visualizer rendering. |

---

## 13. Verification & Acceptance Criteria

1. **Dual-NIC Failover**: Disconnecting the Ethernet cable switches traffic to Wi-Fi within 15 seconds; browser stream recovers seamlessly; reconnecting Ethernet cleanly restores primary metric after 60s.
2. **Session Durability (12+ Hours)**: Thin client runs continuously from 04:00 to 16:15 ET without memory sprawl (Chromium RAM stays sub-800MB via soft recycles).
3. **Power Lifecycle**: PC automatically hibernates after post-market settlement (16:15 ET) and wakes via RTC timer at 09:15 ET on the next trading day.
4. **Desktop Interactivity**: System Tray icon reflects accurate market states; toast notifications chime on high-conviction signals; `Ctrl + Alt + B` instantly toggles browser visibility.
5. **Analytical Precision**: Strike detection and momentum signals match expected rules; multi-timeframe candles render with exact ET session alignment in Django Admin Visualizer.

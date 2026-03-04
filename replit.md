# NOC-IS Analytics Platform

## Overview
The NOC-IS Analytics Platform is a web-based Network Operations Center (NOC) analytics platform designed to monitor, analyze, and predict network operational performance. It integrates diverse data sources like site master data, ticket information, and external factors such as weather and power outages. The platform aims to improve operational efficiency, facilitate proactive maintenance, and enhance decision-making through advanced analytics and predictive capabilities. Key functionalities include data ingestion, multi-dimensional entity profiling with KPIs, predictive analytics for risk scoring and SLA breach, master data management, and data quality monitoring.

## User Preferences
Not specified.

## System Architecture

The platform employs a client-server architecture, featuring a Python/FastAPI backend and a React/Vite frontend. DuckDB is utilized for data persistence and analytical processing.

**UI/UX Decisions:**
- **Visual Design**: The platform adheres to a "Muted Professional" theme, using a specific color palette (e.g., `--text-primary #0F172A`, `--accent-brand #1E40AF`) and 8px CSS dots for status indicators. No emojis are used.
- **Frontend Technologies**: React 18 with Vite for development, Tailwind CSS for styling, Zustand for state management, and Recharts for interactive data visualizations (ComposedChart, RadarChart, ScatterChart).
- **Common UI Patterns**: Includes universal loading/error/empty states and a "Danger Zone" component for sensitive actions.
- **Navigation**: A dashboard provides a central hub, with dedicated pages for data upload, master data management, profiling, external data, and settings.

**Technical Implementations:**
- **Backend (FastAPI)**: Provides a RESTful API with structured routers for various functionalities, including data upload, administration, schema management, threshold configuration, master data, data quality, external data, profiling, and predictive analytics. Business logic is encapsulated in services for tasks like backup, upload processing, site enrichment, SLA resolution, and predictive model execution. DuckDB is used as an embedded OLAP database with a single-writer lock for concurrency.
- **Frontend (React)**: Manages routing (`App.jsx`), global layout (`Layout.jsx`), and API interactions via Axios. Dedicated pages and components handle specific features such as data upload, master data management, profiler analytics, predictive insights, and external data management. Zustand stores manage application state and cache data with a 5-minute TTL.

**Feature Specifications:**
- **Dashboard Overview**: Displays overall status, KPI snapshots with Month-over-Month deltas, entity status, a recommendation panel, and quick charts (SLA trend, volume trend, risk, behavior distribution).
- **Report Card**: Generates printable per-entity report cards with KPIs, trends, child rankings, and recommendations, supporting various hierarchical levels.
- **Recommendation Engine**: Implements 20 rules covering SLA, MTTR, Volume, Risk, Escalation, Trend, Device, and Positive categories, with support for chain merging and priority-based deduplication.
- **Threshold Settings**: Configurable parameters across 12 groups (e.g., MTTR, Escalation, Risk) accessible via the Settings tab.
- **Saved Views**: Allows users to save Profiler configurations, compare them with current KPIs, and track access.
- **Comparison Mode**: Enables side-by-side comparison of two entity profiles, including KPI delta analysis, radar overlay charts, child entity delta tables, and AI-generated narratives.
- **Report Generator & Export**: Generates reports in 5 template types (Daily, Weekly, Monthly, Quarterly, Annual) with PDF export (WeasyPrint, matplotlib charts) and Excel export (openpyxl). Includes report history and HTML preview.
- **Data Ingestion**: Supports single and chunked file uploads with auto-detection of 5 file types and header normalization.
- **Data Processing**:
    - **Site Master**: Auto-populates hierarchy and enriches sites using bulk DuckDB operations for efficiency.
    - **Tickets**: Calculates 17 derived columns, resolves hierarchy, and handles duplicate detection.
    - **Summarization**: Refreshes monthly and weekly summary tables.
- **Data Management**: Provides CRUD operations for master data entities, server-side pagination, export, import history, granular data deletion, and orphan record management. A "Sinkronisasi Hierarki" feature re-resolves ticket hierarchies against master data using atomic table swaps in DuckDB, with post-resync orphan delta display showing before/after changes.
- **Analytics & Profiling**:
    - **Profiler Engine**: Generates entity profiles, including KPIs, behavior labels, narratives, and recommendations, supporting temporal analysis, peer ranking, and cross-dimensional disruption analysis.
    - **Predictive Analytics**: Includes risk scoring (site-level based on 7 components, aggregated hierarchically), volume forecasting (WMA), SLA breach prediction, pattern detection, and maintenance calendar scheduling.
- **Authentication & 2FA**: Platform access protected by password authentication with email-based 2FA. Login flow: password verification → 6-digit code sent to admin email → code verification → session cookie set. Session-based auth using signed cookies with `SESSION_SECRET`. Auth middleware protects all `/api/*` routes except `/api/auth/*` and `/api/health`. 2FA emails sent via Replit's built-in mail service (no SendGrid needed). Files: `backend/services/auth_service.py`, `backend/routers/auth.py`, `frontend/src/pages/LoginPage.jsx`, `frontend/src/stores/authStore.js`.
- **NDC Knowledge Base**: Auto-generates Network Diagnostic Codes (NDCs) based on unique ticket patterns, providing 4 enrichment tabs per entry: Alarm Snapshot, Symptoms, Diagnostic Tree, and SOP. Features include confusion matrix, site distribution (with cascading Regional → NOP → TO → Site dropdowns), and a curation workflow. Enrichment functions log errors at debug level instead of silently swallowing exceptions. All enrichment tables (ndc_alarm_snapshot, ndc_symptoms, ndc_diagnostic_steps, ndc_resolution_paths, ndc_co_occurring_alarms, ndc_escalation_matrix) are populated during full refresh. NDC detail view uses enhanced visual contrast (white card with shadow, left accent border, gradient dividers, card-wrapped sections). NDC refresh protected behind multi-step confirmation (modal + type "REFRESH") and all changes recorded in `ndc_changelog` audit table.
- **Hierarchy Resync**: Uses DROP TABLE + RENAME approach instead of RENAME-swap to avoid DuckDB dependency errors when views reference the swapped table.
- **External Data**: Manages and integrates data from BMKG (weather), PLN (power outages), and a calendar (holidays), supporting custom annotations and correlation analysis.
- **System Administration**: Offers database backup/restore, schema initialization, seeding reset, and data deletion functionalities.

**System Design Choices:**
- **DuckDB**: Selected for efficient OLAP capabilities directly on the dataset.
- **Single-Writer Lock**: Ensures data integrity for DuckDB write operations.
- **Schema Auto-Initialization**: Prepares the database on application startup.
- **Configurable Thresholds**: Allows dynamic adjustment of analytical parameters.
- **Priority-based SLA Resolution**: Determines SLA targets based on a defined hierarchy.
- **Production Deployment**: FastAPI serves both the API and the built Vite frontend from a single process. Uses a two-stage ASGI startup: a raw ASGI handler responds to health checks instantly (0.15s import), while the full FastAPI app + all routers load in a background thread (~8-10s). No build step in deployment config.
- **DuckDB Persistence**: Production stores data in `~/noc_data/` (persistent across VM restarts). Dev uses `.data/`. Auto-restore on startup: if database is empty but backups exist, restores from latest backup automatically. Auto-backup runs after every startup to maintain baseline.
- **Email Integration**: Uses Replit's built-in mail service for 2FA emails (no external provider like SendGrid needed). The Replit mail service sends to the user's verified Replit email and also supports sending to specific addresses via the connectors API.

## External Dependencies

- **Python Libraries**: `fastapi`, `uvicorn`, `duckdb`, `python-multipart`, `aiofiles`, `psutil`, `pandas`, `openpyxl`, `jinja2`.
- **Frontend Libraries**: `react`, `react-dom`, `react-router-dom`, `axios`, `zustand`, `recharts`, `lucide-react`, `vite`, `tailwindcss`.
- **External Data Sources**: BMKG (weather data), PLN (power outage data).

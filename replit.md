# NOC-IS Analytics Platform

## Overview
The NOC-IS Analytics Platform is a web-based Network Operations Center (NOC) analytics platform. It provides comprehensive tools for monitoring, analyzing, and predicting network operational performance. The platform integrates various data sources, including site master data, ticket data, and external information like weather and PLN outages, to offer insights into network health, SLA adherence, and potential risks. Its core purpose is to enhance operational efficiency, enable proactive maintenance, and improve decision-making through advanced analytics and predictive capabilities.

Key capabilities include:
- Data ingestion and processing pipeline with auto-detection and enrichment.
- Multi-dimensional profiling of network entities (Area, Regional, NOP, TO, Site) with KPIs, behavior analysis, and narrative generation.
- Predictive analytics for risk scoring, volume forecasting, SLA breach prediction, and maintenance scheduling.
- Comprehensive master data management for hierarchy, sites, SLA targets, and thresholds.
- Data quality monitoring and orphan record management.
- Integration of external data sources for enriched analysis and correlation.

## User Preferences
Not specified.

## System Architecture

The platform follows a client-server architecture with a Python/FastAPI backend and a React/Vite frontend. Data persistence and analytical processing are handled by DuckDB.

**UI/UX Decisions:**
- The frontend uses React 18 with Vite for a fast development experience and Tailwind CSS for utility-first styling.
- Zustand is used for state management, and Recharts is employed for data visualization, providing interactive and informative charts (e.g., ComposedChart for trends, RadarChart for risk, ScatterChart for patterns).
- Common UI patterns include universal loading/error/empty states, and a "Danger Zone" component with confirmation for sensitive operations.
- The dashboard provides health information and serves as a central point for navigation. Specialized pages exist for data upload, master data management, profiling, external data, and settings.

**Technical Implementations:**
- **Backend (FastAPI)**:
    - Provides a RESTful API with structured routers for health checks, data upload, administration, schema management, threshold configuration, master data (hierarchy, site, SLA targets), data quality, external data, profiling, and predictive analytics.
    - Utilizes services for encapsulating business logic such as backup, upload processing, schema initialization, site enrichment, SLA resolution, file detection, header normalization, data processing (site master, tickets), summary generation, calendar management, behavior analysis, and predictive model execution.
    - Employs DuckDB as an embedded OLAP database, managed with a single-writer lock for concurrency control.
- **Frontend (React)**:
    - `App.jsx` handles routing. `Layout.jsx` defines the global structure with sidebar, header, and main content.
    - API interactions are managed via Axios, including an error interceptor.
    - Specific pages and components are dedicated to various functionalities, such as `UploadPage` for the processing pipeline, `MasterDataPage` for managing hierarchical and site data, `ProfilerPage` for multi-dimensional analytics, `PredictivePanel` for displaying predictive insights (risk, forecast, SLA breach, patterns, maintenance calendar), and `ExternalDataPage` for external data management.
    - Zustand stores (`cacheStore`, `profilerStore`) manage application state and cache data with a 5-minute TTL.

**Feature Specifications:**
- **Dashboard Overview**: Full operational dashboard with period/level/filter selectors. Shows overall status (KRITIS/PERLU PERHATIAN/BAIK/SANGAT BAIK), KPI snapshot (Volume, SLA, MTTR, Escalation, Auto-resolve with MoM deltas), entity status table with sorting and "Lihat Profil" drill-down, recommendation panel (R01-R20 rules with chaining/dedup, prioritized as SEGERA/MINGGU_INI/BULAN_INI/RUTIN), and quick charts (SLA trend, volume trend, risk distribution, behavior distribution).
- **Report Card**: Generates per-entity report cards with printable layout. Includes entity profile, KPI table with deltas, 3-month trend sparklines, child ranking table, and recommendations. Supports Area/Regional/NOP/TO levels.
- **Recommendation Engine**: 20 rules (R01-R20) covering SLA, MTTR, Volume, Risk, Escalation, Trend, Device, and Positive categories. Supports chain merging (e.g., R01+R05+R14 = single combined recommendation) and category-priority deduplication. Max 5 recommendations per entity.
- **Threshold Settings**: Configurable threshold parameters organized in 12 groups (MTTR, Escalation, Auto-resolve, Repeat, Trend, Anomaly, Risk, Behavior, Seasonal, Pattern, Capacity, Display). Accessible via Settings → Threshold tab.
- **Data Ingestion**: Supports single and chunked file uploads. Auto-detects 5 file types based on filename and headers. Normalizes headers to snake_case.
- **Data Processing**:
    - **Site Master**: Auto-populates hierarchy (Regional, NOP, TO) and enriches sites based on rules. Supports bulk updates and imports.
    - **Tickets**: Calculates 17 derived columns (time metrics, SLA status, classification, hierarchy, source). Resolves hierarchy and handles duplicate detection (key: `ticket_number_inap` + `calc_source`).
    - **Summarization**: Refreshes `summary_monthly` and `summary_weekly` tables for affected periods.
- **Data Management**:
    - CRUD operations for `Area`, `Regional`, `NOP`, `TO`, `Site`, `SLA Target`, and `Threshold`.
    - Server-side pagination and export functionality for site master data.
    - Import history tracking and granular data deletion by import ID or period.
    - Orphan management for unmapped tickets.
- **Analytics & Profiling**:
    - **Profiler Engine**: Generates entity profiles including KPIs, behavior labels (6 types), narrative, and recommendations. Supports temporal analysis (trend, heatmap, child decomposition), peer ranking, and cross-dimensional `gangguan` (disruption) analysis.
    - **Predictive Analytics**:
        - **Risk Score**: Calculates site-level risk based on 7 weighted components. Aggregates risk at higher hierarchical levels.
        - **Forecast**: Uses WMA with trend and seasonal components to predict volume.
        - **SLA Breach Prediction**: Projects SLA performance and identifies potential breach weeks for entities.
        - **Pattern Detection**: Identifies recurring patterns in ticket data.
        - **Maintenance Calendar**: Schedules maintenance based on predictive insights.
- **External Data**: Manages and integrates weather (BMKG), PLN outage, and calendar data (holidays, Ramadan). Supports custom annotations and correlation analysis against ticket data.
- **System Administration**: Provides functionalities for database backup/restore, schema initialization/status check, seeding reset, and data deletion.

**System Design Choices:**
- **DuckDB**: Chosen for its file-based OLAP capabilities, enabling efficient analytical queries directly on the dataset.
- **Single-Writer Lock**: Implemented for DuckDB write operations to ensure data integrity due to its embedded nature.
- **Schema Auto-Initialization**: Ensures the database is ready on application startup if empty.
- **Configurable Thresholds**: Allows dynamic adjustment of analytical parameters.
- **Priority-based SLA Resolution**: Determines SLA targets based on a defined priority hierarchy.

## External Dependencies

- **Python Libraries**:
    - `fastapi`: Web framework for the backend API.
    - `uvicorn`: ASGI server for running FastAPI.
    - `duckdb`: Embedded analytical database.
    - `python-multipart`: For handling form data, especially file uploads.
    - `aiofiles`: Asynchronous file operations.
    - `psutil`: System utility for process information (e.g., memory usage).
    - `pandas`: Data manipulation and analysis.
    - `openpyxl`: For reading/writing Excel files.
- **Frontend Libraries**:
    - `react`, `react-dom`: JavaScript library for building user interfaces.
    - `react-router-dom`: Declarative routing for React.
    - `axios`: Promise-based HTTP client for the browser and Node.js.
    - `zustand`: Small, fast, and scalable bear-bones state-management solution.
    - `recharts`: Composable charting library built with React and D3.
    - `lucide-react`: A collection of open-source icons.
    - `vite`: Next-generation frontend tooling.
    - `tailwindcss`: Utility-first CSS framework.
- **External Data Sources**:
    - BMKG (Badan Meteorologi, Klimatologi, dan Geofisika): For weather data.
    - PLN (Perusahaan Listrik Negara): For power outage data.
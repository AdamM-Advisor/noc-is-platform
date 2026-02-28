# NOC-IS Analytics Platform v1.0

Platform analitik NOC (Network Operations Center) berbasis web.

## Tech Stack
- **Backend**: Python 3.11 / FastAPI (port 8000)
- **Frontend**: React 18 + Vite (port 5000) + Tailwind CSS + Zustand + Recharts
- **Database**: DuckDB (file-based OLAP, persistent at `data/noc_analytics.duckdb`)

## Project Structure
```
backend/
  main.py              - FastAPI entry point (auto-init schema on startup)
  config.py            - Paths, limits, constants
  database.py          - DuckDB connection manager (single-writer lock)
  routers/
    health.py          - GET /api/health
    upload.py          - Upload endpoints (single + chunked)
    admin.py           - Backup, restore, danger zone
    schema.py          - Schema init, status, seed reset
    threshold.py       - Threshold CRUD
  services/
    backup_service.py  - Auto-backup logic (retain 3)
    upload_service.py  - Chunk assembly + validation
    schema_service.py  - Schema DDL, seed data, status check
    enrichment_service.py - Site enrichment rules (class x flag)
    sla_service.py     - SLA target resolution (priority-based)

frontend/
  src/
    App.jsx            - Router setup
    main.jsx           - Entry point
    api/client.js      - Axios instance + error interceptor
    components/
      Layout.jsx       - Sidebar + header + main content
      LoadingWrapper.jsx - Universal loading/error/empty state
      DangerZone.jsx   - Danger zone UI pattern with confirmation
    pages/
      Dashboard.jsx    - Placeholder with health info
      UploadPage.jsx   - Drag-drop + chunked upload + progress
      SettingsPage.jsx - Schema status, DB info, backup/restore, danger zone
    stores/
      cacheStore.js    - Zustand cache with 5-min TTL

data/                  - Database directory (persistent)
  backups/             - Auto-backup directory (max 3)
uploads/               - Uploaded files
temp_chunks/           - Temp chunks during upload
exports/               - Generated reports
```

## Database Schema (15 tables)
### Master Tables
- `master_area` (4 rows seed) - Area hierarchy
- `master_regional` - Regional hierarchy (FK to area)
- `master_nop` - NOP hierarchy (FK to regional)
- `master_to` - TO hierarchy (FK to nop)
- `master_site` - Site master (78K+ rows, enrichment-derived columns)
- `master_sla_target` (9 rows seed) - Priority-based SLA targets
- `master_threshold` (38 rows seed) - Configurable analytics parameters

### Data Tables
- `noc_tickets` - Raw ticket data (72 raw + 17 calculated columns)
- `summary_monthly` - Monthly aggregated KPIs
- `summary_weekly` - Weekly aggregated KPIs
- `risk_score_history` - Site risk scores over time

### System Tables
- `saved_views`, `report_history`, `import_logs`, `orphan_log`

### Views
- `v_hierarchy` - Denormalized TO->NOP->Regional->Area join

## API Endpoints
- `GET /api/health` - System health check
- `POST /api/upload/single` - Single file upload (< 10MB)
- `POST /api/upload/chunk` - Upload chunk
- `POST /api/upload/chunk/complete` - Assemble chunks
- `GET /api/upload/chunk/status/{id}` - Chunk status
- `GET /api/admin/backups` - List backups
- `POST /api/admin/backup` - Create backup
- `POST /api/admin/restore` - Restore from backup
- `GET /api/admin/db-info` - Database info
- `POST /api/admin/delete-data` - Delete non-master data
- `POST /api/admin/reset-database` - Full database reset
- `POST /api/schema/init` - Initialize all tables + seed data
- `GET /api/schema/status` - Check table existence and row counts
- `POST /api/schema/seed-reset` - Reset threshold & SLA seeds
- `GET /api/threshold` - All thresholds grouped by category
- `GET /api/threshold/{key}` - Single threshold
- `PUT /api/threshold/{key}` - Update threshold value

## Key Constraints
- DuckDB single-writer: only 1 write connection via threading.Lock
- Memory limit: 512MB, Threads: 2
- Chunk size: 5MB, Max single upload: 10MB
- Max backups retained: 3
- Frontend proxies /api/* to backend via Vite config
- Schema auto-initializes on startup if database is empty

## Workflows
- **Backend API**: uvicorn on port 8000 with --reload
- **Start application**: Vite dev server on port 5000

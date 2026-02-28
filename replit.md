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
    upload.py          - Upload + detect + process endpoints
    admin.py           - Backup, restore, danger zone
    schema.py          - Schema init, status, seed reset
    threshold.py       - Threshold CRUD
    imports.py         - Import history CRUD + delete
    orphans.py         - Orphan management + resolve
    data.py            - Granular ticket delete
    hierarchy.py       - Area/Regional/NOP/TO CRUD + tree + stats
    site.py            - Site CRUD w/ server-side pagination + export
    sla_target.py      - SLA target rules CRUD + resolver + impact
    data_quality.py    - Data quality summary endpoint
  services/
    backup_service.py  - Auto-backup logic (retain 3)
    upload_service.py  - Chunk assembly + validation
    schema_service.py  - Schema DDL, seed data, status check
    enrichment_service.py - Site enrichment rules (class x flag)
    sla_service.py     - SLA target resolution (priority-based)
    file_detector.py   - Auto-detect 5 file types by filename + headers
    header_normalizer.py - Header normalization (Title Case/UPPER_CASE → snake_case)
    site_master_processor.py - Process site master + auto-populate hierarchy
    ticket_processor.py - Process ticket data (17 calculated columns)
    summary_service.py - Refresh summary_monthly + summary_weekly

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
      UploadPage.jsx   - Full processing pipeline UI (detect, process, history)
      MasterDataPage.jsx - 5-tab master data management
      SettingsPage.jsx - Schema status, DB info, backup/restore, danger zone
      master/
        HierarchyTab.jsx   - Tree view CRUD (Area→Regional→NOP→TO)
        SiteTab.jsx        - Server-side paginated site list (50/pg)
        SlaTargetTab.jsx   - SLA target rules + resolver tester
        ThresholdTab.jsx   - Inline-edit threshold params by category
        DataQualityTab.jsx - Data quality dashboard + orphan management
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
- `master_regional` - Regional hierarchy (FK to area, auto-populated from site master)
- `master_nop` - NOP hierarchy (FK to regional, auto-populated from site master)
- `master_to` - TO hierarchy (FK to nop, auto-populated from site master)
- `master_site` - Site master (78K+ rows, enrichment-derived columns)
- `master_sla_target` (9 rows seed) - Priority-based SLA targets
- `master_threshold` (38 rows seed) - Configurable analytics parameters

### Data Tables
- `noc_tickets` - Raw ticket data (72 raw + 17 calculated columns)
- `summary_monthly` - Monthly aggregated KPIs (by area/regional/nop/to/site)
- `summary_weekly` - Weekly aggregated KPIs
- `risk_score_history` - Site risk scores over time

### System Tables
- `saved_views`, `report_history`, `import_logs`, `orphan_log`

### Views
- `v_hierarchy` - Denormalized TO->NOP->Regional->Area join

## Upload & Processing Pipeline
1. User uploads file (single or chunked)
2. File type auto-detected from filename + headers (5 types: site_master, swfm_event, swfm_incident, swfm_realtime, fault_center)
3. Headers normalized to snake_case
4. Processing:
   - Site master: auto-populate hierarchy (regional/nop/to), enrich sites, UPSERT
   - Tickets: 17 calculated columns, hierarchy resolution, duplicate detection, area auto-mapping
5. Summary tables refreshed for affected periods
6. Import logged to import_logs

### 17 Calculated Columns
- 4 time: response_time, repair_time, restore_time, detection_time (minutes)
- 3 SLA: sla_duration, sla_target, is_sla_met
- 7 classification: hour_of_day, day_of_week, week_of_month, month, year, year_month, year_week
- 4 hierarchy: area_id, regional_id, nop_id, to_id (resolved)
- 1 source: calc_source (file type)

### Duplicate Detection
- Key: ticket_number_inap + calc_source
- Same ticket from different sources = NOT duplicate
- Re-upload same file = duplicates skipped

## API Endpoints
- `GET /api/health` - System health check
- `POST /api/upload/single` - Single file upload (< 10MB)
- `POST /api/upload/chunk` - Upload chunk
- `POST /api/upload/chunk/complete` - Assemble chunks
- `GET /api/upload/chunk/status/{id}` - Chunk status
- `POST /api/upload/detect` - Detect file type from uploaded file
- `POST /api/upload/process` - Start processing pipeline
- `GET /api/upload/process/status/{job_id}` - Processing progress/status
- `GET /api/imports` - List import history
- `GET /api/imports/{id}` - Import detail
- `DELETE /api/imports/{id}` - Delete import + related data
- `GET /api/orphans` - List unresolved orphans
- `PUT /api/orphans/{id}/resolve` - Resolve orphan + remap tickets
- `DELETE /api/data/tickets` - Granular delete (by period/source)
- `DELETE /api/data/tickets/by-import/{id}` - Delete by import
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
- `GET/POST /api/master/area` - List/create areas
- `PUT/DELETE /api/master/area/{id}` - Update/soft-delete area
- `GET/POST /api/master/regional` - List/create regionals (?area_id=)
- `PUT/DELETE /api/master/regional/{id}` - Update/soft-delete regional
- `GET/POST /api/master/nop` - List/create NOPs (?regional_id=)
- `PUT/DELETE /api/master/nop/{id}` - Update/soft-delete NOP
- `GET/POST /api/master/to` - List/create TOs (?nop_id=)
- `PUT/DELETE /api/master/to/{id}` - Update/soft-delete TO
- `GET /api/master/hierarchy/tree` - Full nested hierarchy tree
- `GET /api/master/hierarchy/stats` - Count per level
- `GET /api/master/site` - Paginated site list (?page&per_page&filters&sort)
- `GET /api/master/site/{id}` - Single site detail
- `PUT /api/master/site/{id}` - Update site (auto-enriches on class/flag change)
- `POST /api/master/site/export` - Export filtered CSV
- `GET /api/master/sla-target` - List SLA target rules
- `POST /api/master/sla-target` - Create SLA rule
- `PUT /api/master/sla-target/{id}` - Update SLA rule
- `DELETE /api/master/sla-target/{id}` - Delete SLA rule (protect default)
- `GET /api/master/sla-target/resolve` - Resolve target (?site_class&site_flag&area_id)
- `GET /api/master/sla-target/{id}/impact` - Count affected sites
- `GET /api/data-quality/summary` - Data quality dashboard summary

## Key Constraints
- DuckDB single-writer: only 1 write connection via threading.Lock
- Memory limit: 512MB, Threads: 2
- Chunk size: 5MB, Max single upload: 10MB
- Max backups retained: 3
- Frontend proxies /api/* to backend via Vite config
- Schema auto-initializes on startup if database is empty
- Only 1 upload processing job at a time (single-writer constraint)

## Dependencies
- **Python**: fastapi, uvicorn, duckdb, python-multipart, aiofiles, psutil, pandas, openpyxl
- **Frontend**: react, react-dom, react-router-dom, axios, zustand, recharts, lucide-react, vite, tailwindcss

## Workflows
- **Backend API**: uvicorn on port 8000 with --reload
- **Start application**: Vite dev server on port 5000

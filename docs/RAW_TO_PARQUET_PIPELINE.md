# RAW to Parquet Pipeline

Pipeline import utama sekarang memakai pola:

```text
RAW CSV/Excel/Parquet
  -> raw archive
  -> converted source Parquet
  -> Bronze Parquet
  -> Silver Parquet
  -> Gold monthly summary Parquet
  -> DuckDB summary cache untuk UI
```

## Jalur UI

Menu `Upload Data` menerima:

- `.csv`
- `.xlsx`
- `.parquet`

Untuk file tiket NOC, mode default lokal adalah:

```env
NOCIS_UPLOAD_PIPELINE_MODE=parquet
```

Artinya file RAW tidak lagi wajib dikonversi manual sebelum upload. Aplikasi akan mengarsipkan file asli, mengonversi ke Parquet, membuat Bronze/Silver/Gold, lalu memperbarui summary cache.

Site master tetap memakai jalur master-data karena perlu update tabel hierarchy/site.

## Jalur Folder RAW

Untuk memproses folder RAW langsung:

```powershell
.\.venv\Scripts\python.exe -m backend.jobs import-raw-folder C:\NOC-IS-Data\raw\tickets
```

Untuk satu file:

```powershell
.\.venv\Scripts\python.exe -m backend.jobs import-raw-file C:\NOC-IS-Data\raw\tickets\tickets_2026-01.csv --source manual
```

Jika nama file tidak memuat periode `YYYY-MM`, pipeline akan mencoba membaca kolom `yearmonth`, `year_month`, atau `occured_time`.

## Folder Output Lokal

Dengan konfigurasi lokal default:

```text
.data\raw\                         arsip file asli
.parquet_lake\raw_converted\        file source Parquet hasil konversi RAW
.parquet_lake\tickets\bronze\       Bronze Parquet
.parquet_lake\tickets\silver\       Silver Parquet
.parquet_lake\summaries\monthly\    Gold summary Parquet
.data\noc_analytics.duckdb          catalog, jobs, summary cache UI
```

Untuk data besar, arahkan folder ini ke lokasi non-OneDrive, misalnya:

```env
NOCIS_DATA_DIR=C:\NOC-IS-Data\catalog
NOCIS_DB_PATH=C:\NOC-IS-Data\catalog\noc_analytics.duckdb
NOCIS_RAW_DIR=C:\NOC-IS-Data\raw
NOCIS_LAKE_ROOT=C:\NOC-IS-Data\lake
NOCIS_DUCKDB_MEMORY_LIMIT=4GB
NOCIS_DUCKDB_THREADS=4
```

## Prediktif SARIMAX

SARIMAX dijalankan sebagai batch job dari summary cache, bukan saat halaman dibuka:

```powershell
.\.venv\Scripts\python.exe -m backend.jobs execute-sarimax-forecast --entity-level site --window-start 2025-01 --window-end 2025-12 --horizon 3 --limit 100
```

Hasilnya disimpan di `model_run_catalog`. Endpoint forecast akan memakai hasil SARIMAX tersimpan jika tersedia untuk entity/window yang sama. Jika belum ada hasil batch, endpoint masih bisa fallback ke forecast statistik lama.

Dependency SARIMAX memakai `statsmodels`; jalankan ulang `.\scripts\setup-local.ps1` setelah update dependency.

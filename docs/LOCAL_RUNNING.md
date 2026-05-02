# Local Running Guide

This guide is for running NOC-IS Platform on Windows PowerShell before deploying
to Google Cloud.

## 1. Bootstrap Local Environment

From the repository root:

```powershell
.\scripts\setup-local.ps1
```

The script creates:

- `.venv` for backend Python dependencies.
- `.env.local` for backend local configuration.
- `frontend\.env.local` pointing the frontend to `http://127.0.0.1:8000`.
- local admin credentials for development only.

Default local credentials:

- Email: `admin@example.com`
- Password: `Admin@12345`

To choose another password:

```powershell
.\scripts\setup-local.ps1 -AdminEmail "you@example.com" -AdminPassword "your-local-password"
```

## 2. Start Backend

```powershell
.\scripts\start-backend.ps1
```

Backend URLs:

- `http://127.0.0.1:8000/healthz`
- `http://127.0.0.1:8000/api/health`

## 3. Start Frontend

Open a second PowerShell window:

```powershell
.\scripts\start-frontend.ps1
```

Frontend URL:

```text
http://127.0.0.1:5173
```

## 4. Start Both

Alternatively, open both servers in separate PowerShell windows:

```powershell
.\scripts\start-local.ps1
```

## 5. Local Login

In development mode, email delivery is not required. After entering the local
password, the 2FA code is shown directly on the login page.

This behavior is enabled only by:

```text
APP_ENV=development
LOCAL_AUTH_SHOW_2FA_CODE=1
```

Do not enable it in production.

## 6. Useful Checks

Run backend tests:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s backend/tests
```

Run frontend build:

```powershell
cd frontend
npm run build
```

Run local benchmark:

```powershell
.\.venv\Scripts\python.exe -m backend.jobs benchmark-local --rows 5000 --months 2 --site-count 200
```

Generated local data goes to ignored folders such as `.data`, `.uploads`,
`.parquet_lake`, `temp_chunks`, `exports`, and `.test_tmp`.

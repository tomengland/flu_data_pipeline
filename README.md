# Flu Data Pipeline - Washington State

## Quick Start

1. Make sure Docker Desktop is running
2. Open PowerShell in this folder
3. Run: `docker-compose up -d --build`
4. Access services:
   - JupyterLab: http://localhost:8888
   - Flask API: http://localhost:5000
   - PostgreSQL: localhost:5432

## Stop Services
```bash
docker-compose down

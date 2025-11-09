# Washington State Flu Data Pipeline

A comprehensive data pipeline for collecting, analyzing, and visualizing flu surveillance data from Washington State Department of Health (DOH) and CDC sources.

## Quick Start

### 1. Start Docker Services
```bash
docker-compose up -d
```

This starts:
- PostgreSQL database (port 5432)
- Jupyter Notebook (port 8888)
- Flask API Dashboard (port 5001)

### 2. Run Data Collection

#### Access Jupyter Notebook
```bash
# Get the Jupyter access token
docker-compose logs jupyter | grep "http://127.0.0.1:8888/lab?token="

# Open Jupyter in browser (copy the URL from above)
"$BROWSER" http://localhost:8888/lab?token=<your-token>
```

#### Run the Pipeline
1. Navigate to `notebooks/collect_data.ipynb`
2. Click **Run → Run All Cells** (or press Shift+Enter through each cell)
3. Wait for all cells to complete (~2-3 minutes)

**That's it!** The notebook will:
- ✅ Download WA DOH RHINO data
- ✅ Download Census population data
- ✅ Download CDC FluView data
- ✅ Clean and transform the data
- ✅ Create PostgreSQL database tables
- ✅ Load all data into the database

### 3. Initialize and Access Airflow for Automation

A. Initialize Airflow DB
   ```bash
   docker exec -it <flu_jupyter container id> airflow db init
   ```

B. Start Webserver
   
   ```bash
   docker exec -it <flu_jupyter container id> airflow webserver
   ```
C. Start Scheduler
   
   ```bash
    docker exec -it <flu_jupyter container id> airflow scheduler
   ```
D. Log-In to Airflow
   
   Go to: http://localhost:8080
   
   username: admin / password: admin

   The desired Airflow DAG is flu_data_airflow v2.py

E. Airflow will automate the entire data pipeline from start to finish and is designed to execute another pull Daily or as otherwise specified in the .py file.  

### 4. View the Dashboard

```bash
"$BROWSER" http://localhost:5001/viewer
```

The dashboard provides:
- 📈 **Weekly Trends**: Real-time flu activity by week
- 🏨 **Healthcare Impact**: Hospital utilization metrics
- 📊 **Historical Summary**: Long-term flu patterns

### 3. Initialize and Access Airflow for Automation

1. Initialize Airflow DB
   ```bash
   docker exec -it <flu_jupyter container id> airflow db init
   ```

2. Start Webserver
   
   ```bash
   docker exec -it <flu_jupyter container id> airflow webserver
   ```
3. Start Scheduler
   
   ```bash
    docker exec -it <flu_jupyter container id> airflow scheduler
   ```
4. Log-In to Airflow
   
   Go to: http://localhost:8080
   
   username: admin / password: admin
   

## Project Architecture

```
┌─────────────────┐
│  Data Sources   │
│  - WA DOH RHINO │
│  - Census Data  │
│  - CDC FluView  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Jupyter Notebook│  ← Run collect_data.ipynb
│  Data Pipeline  │     (All cells in order)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │  ← Stores 5 normalized tables
│    Database     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Flask API     │  ← Interactive dashboard
│   + Dashboard   │     + CSV exports
└─────────────────┘
```

## Airflow DAG Architecture

<img width="1180" height="586" alt="DAG Screenshot" src="https://github.com/user-attachments/assets/0d8fb830-5742-4272-9444-496c87ae86a1" />

## Database Schema

### Tables Created
1. **county_region**: County names, ACH regions, population density
2. **temporal**: Week dates, epiweeks, flu seasons
3. **illness**: County-level flu metrics by week
4. **healthcare**: Hospital and ER utilization rates
5. **historics**: Historical CDC flu season data

## Common Tasks

### View API Health
```bash
curl http://localhost:5001/health
```

### Export Data to CSV
From the dashboard, click the green export buttons, or:
```bash
curl "http://localhost:5001/api/export/csv?table=illness" -o illness.csv
curl "http://localhost:5001/api/export/csv?table=county_region" -o counties.csv
curl "http://localhost:5001/api/export/csv?table=temporal" -o weeks.csv
curl "http://localhost:5001/api/export/csv?table=healthcare" -o healthcare.csv
curl "http://localhost:5001/api/export/csv?table=historics" -o historical.csv
```

### Restart Services
```bash
# Restart all
docker-compose restart

# Restart specific service
docker-compose restart flask-api
```

### Stop Everything
```bash
# Stop services (keeps data)
docker-compose stop

# Stop and remove containers (keeps data in volumes)
docker-compose down

# Remove everything including data
docker-compose down -v
```

## Updating Data

To refresh the data with latest information:

1. **Open Jupyter Notebook** (see Quick Start step 2)
2. **Run All Cells** in `collect_data.ipynb`
3. The notebook will automatically:
   - Download latest data from sources
   - Drop existing tables
   - Recreate tables with new data
   - Commit changes to database
4. **Refresh the dashboard** - new data appears immediately

## Troubleshooting

### ❌ "No data available" in Dashboard

**Solution**: Run the Jupyter notebook
```bash
"$BROWSER" http://localhost:8888
# Then run all cells in collect_data.ipynb
```

### ❌ "relation does not exist" Error

**Cause**: Database tables not created yet

**Solution**: Run all cells in the Jupyter notebook, especially the cells that:
1. Create tables (with `CREATE TABLE` statements)
2. Load data (with `COPY` statements)
3. Commit changes (with `conn.commit()`)

### ❌ Jupyter Token Not Working

```bash
# Get fresh token
docker-compose logs jupyter | grep token

# Or restart Jupyter
docker-compose restart jupyter
```

### ❌ Port Already in Use

```bash
# Check what's using port 5001
netstat -an | grep 5001

# Change port in docker-compose.yml if needed:
# ports:
#   - "5002:5000"  # Use 5002 instead
```

### ❌ Airflow Log-In Error

```bash
# Remove any other admin User
docker exec -it <flu_jupyter container id> airflow users delete -u admin

# Recreate Admin User
docker exec -it <flu_jupyter container id> airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
```

## Data Sources

- **WA DOH RHINO**: Washington State respiratory illness surveillance
  - URL: https://doh.wa.gov/data-statistical-reports/diseases-and-chronic-conditions/communicable-disease-surveillance-data/respiratory-illness-data-dashboard

- **Census Data**: WA county population density
  - URL: https://data.wa.gov/Demographics/Population-Density-By-County-2000-2020/e6ip-wkqq

- **CDC FluView**: National flu surveillance data
  - API: https://api.delphi.cmu.edu/epidata/fluview/

## API Endpoints

| Endpoint                              | Description               |
| ------------------------------------- | ------------------------- |
| `GET /`                               | API information           |
| `GET /health`                         | Database health check     |
| `GET /viewer`                         | Interactive dashboard     |
| `GET /api/reports/weekly-trends`      | Weekly flu data (JSON)    |
| `GET /api/reports/healthcare-impact`  | Healthcare metrics (JSON) |
| `GET /api/reports/historical-summary` | Historical data (JSON)    |
| `GET /api/export/csv?table=<name>`    | Export table as CSV       |

## System Requirements

- **Docker**: 20.10+
- **Docker Compose**: 2.0+
- **RAM**: 4GB minimum (8GB recommended)
- **Disk Space**: 2GB for images and data

## Database Credentials

Default credentials (configurable in `docker-compose.yml`):
```
Host: localhost (postgres in container network)
Port: 5432
Database: flu_database
Username: fluuser
Password: flupass
```


## License

This project is for educational and public health surveillance purposes.

---

**Getting Started**: Run `docker-compose up -d`, then run all cells in the Jupyter notebook!

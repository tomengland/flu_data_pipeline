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

The data gathering, data cleaning, and SQL ingesting are all done through airflow process.  The two important folders in the project folder are below.

```bash
/api
  |--- app.py  [Flask Server]
/dags
  |--- flu_data_airflow_v2.py   [Airflow DAG, but also houses data gathering, ingesting, and cleaning code along with SQL injection]

```

### 2. Process data through airflow

- ✅ Download WA DOH RHINO data - flu_data_airflow_v2.py
- ✅ Download Census population data - flu_data_airflow_v2.py
- ✅ Download CDC FluView data - flu_data_airflow_v2.py
- ✅ Clean and transform the data - flu_data_airflow_v2.py
- ✅ Create PostgreSQL database tables - flu_data_airflow_v2.py
- ✅ Load all data into the database - flu_data_airflow_v2.py

D. Log-In to Airflow (If website doesn't show up please give it 30 seconds to 1 min after container is created to start up)

   Go to: http://localhost:8080

   username: admin / password: admin

   The desired Airflow DAG is flu_data_airflow_v2.py

E. Airflow will automate the entire data pipeline from start to finish and is designed to execute another pull Daily or as otherwise specified in the .py file.

### 3. View the Dashboard

http://localhost:5001/viewer

The dashboard provides:
- 📈 **Weekly Trends**: Real-time flu activity by week
- 🏨 **Healthcare Impact**: Hospital utilization metrics
- 📊 **Historical Summary**: Long-term flu patterns

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
│     Airflow     │  ← Run airflow dag
│  Data Pipeline  │
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

## Troubleshooting

### ❌ "No data available" in Dashboard

**Solution**: Run the airflow process once to load db with data.
```bash
"$BROWSER" http://localhost:8080

```

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

**Getting Started**: Run `docker-compose up -d`, then run Airflow DAG.

# Flu Data Pipeline - Washington State

## Quick Start

1. Make sure Docker Desktop is running
2. Open PowerShell in this folder
3. Run: `docker-compose up -d --build`
4. Access services:
   - JupyterLab: http://localhost:8888
   - Flask API: http://localhost:5000
   - PostgreSQL: localhost:5432

## Docker Modifcations/Additions (Andrew Fuerst)
Modified JupyterLab access token to interact w/ VS Code. URL would be unchanged. Added Token: flutoken
http://localhost:8888/?token=flutoken should work if the original does not
Also disabled token check which was causing errors connecting to environment in VS Code
Change local port for Flask to 5001:5000 to solve conflict on my host machine. If this causes issues with your machines, we can change back or change to 0 to select any available. 


## Stop Services
```bash
docker-compose down

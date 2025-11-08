from flask import Flask, jsonify, render_template_string, request, Response
from sqlalchemy import create_engine, text
import csv
from io import StringIO
import os

app = Flask(__name__)

DB_USER = os.getenv('POSTGRES_USER', 'fluuser')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'flupass')
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
DB_NAME = os.getenv('POSTGRES_DB', 'flu_database')

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')

@app.route('/')
def home():
    return jsonify({
        'message': 'Flu Data Pipeline API',
        'status': 'running',
        'endpoints': {
            '/health': 'Check API and database health',
            '/viewer': 'Interactive data viewer',
            '/api/reports/weekly-trends': 'Weekly flu activity trends',
            '/api/reports/county-summary': 'Summary by county',
            '/api/reports/seasonal-comparison': 'Compare flu seasons',
            '/api/reports/healthcare-impact': 'Healthcare system impact',
            '/api/reports/top-affected-counties': 'Most affected counties',
            '/api/reports/historical-summary': 'Historical flu season summary',
            '/api/export/csv?table=<table_name>': 'Export table data as CSV'
        }
    })

@app.route('/health')
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({'status': 'healthy', 'database': 'connected'}), 200
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

@app.route('/viewer')
def viewer():
    html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Flu Data Analytics Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
            .container { max-width: 1400px; margin: 0 auto; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                     color: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; }
            .report-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                          gap: 20px; margin: 20px 0; }
            .report-card { background: white; padding: 20px; border-radius: 8px;
                          box-shadow: 0 2px 4px rgba(0,0,0,0.1); cursor: pointer;
                          transition: transform 0.2s; }
            .report-card:hover { transform: translateY(-5px); box-shadow: 0 4px 8px rgba(0,0,0,0.2); }
            .report-card h3 { margin-top: 0; color: #667eea; }
            button { padding: 12px 24px; font-size: 16px; cursor: pointer;
                    background: #667eea; color: white; border: none;
                    border-radius: 5px; margin: 5px; transition: background 0.3s; }
            button:hover { background: #5568d3; }
            .export-section { background: white; padding: 20px; border-radius: 8px;
                            margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .export-buttons { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 15px; }
            .export-btn { background: #27ae60; padding: 10px 20px; }
            .export-btn:hover { background: #229954; }
            .results { background: white; padding: 20px; border-radius: 8px;
                      margin-top: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            table { border-collapse: collapse; width: 100%; margin: 10px 0; }
            th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
            th { background-color: #667eea; color: white; font-weight: bold; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            tr:hover { background-color: #f0f0f0; }
            h2 { color: #333; margin-top: 0; }
            h3 { color: #667eea; margin-bottom: 10px; }
            .error { color: #e74c3c; padding: 20px; background: #fadbd8;
                    border-radius: 5px; }
            .loading { text-align: center; padding: 40px; color: #666; }
            .metric { display: inline-block; margin: 10px 20px 10px 0; }
            .metric-value { font-size: 32px; font-weight: bold; color: #667eea; }
            .metric-label { font-size: 14px; color: #666; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏥 Flu Data Analytics Dashboard</h1>
                <p>Comprehensive flu surveillance and healthcare impact analysis</p>
            </div>

            <div class="export-section">
                <h3>📥 Export Data Tables (CSV)</h3>
                <p>Download raw data tables for further analysis</p>
                <div class="export-buttons">
                    <button class="export-btn" onclick="exportCSV('county_region')">📍 County Region</button>
                    <button class="export-btn" onclick="exportCSV('temporal')">📅 Temporal</button>
                    <button class="export-btn" onclick="exportCSV('illness')">🦠 Illness Data</button>
                    <button class="export-btn" onclick="exportCSV('healthcare')">🏥 Healthcare</button>
                    <button class="export-btn" onclick="exportCSV('historics')">📊 Historical</button>
                </div>
            </div>

            <div class="report-grid">
                <div class="report-card" onclick="loadReport('weekly-trends')">
                    <h3>📈 Weekly Trends</h3>
                    <p>Track flu activity patterns over time</p>
                </div>
                <div class="report-card" onclick="loadReport('county-summary')">
                    <h3>🗺️ County Summary</h3>
                    <p>Flu statistics by geographic region</p>
                </div>
                <div class="report-card" onclick="loadReport('healthcare-impact')">
                    <h3>🏨 Healthcare Impact</h3>
                    <p>Hospital capacity and utilization</p>
                </div>
                <div class="report-card" onclick="loadReport('top-affected-counties')">
                    <h3>⚠️ Top Affected Counties</h3>
                    <p>Counties with highest flu activity</p>
                </div>
                <div class="report-card" onclick="loadReport('historical-summary')">
                    <h3>📊 Historical Summary</h3>
                    <p>Historical flu season trends</p>
                </div>
            </div>

            <div id="results"></div>
        </div>

        <script>
            function exportCSV(tableName) {
                window.location.href = `/api/export/csv?table=${tableName}`;
            }

            async function loadReport(reportType) {
                const resultsDiv = document.getElementById('results');
                resultsDiv.innerHTML = '<div class="loading">📊 Loading report...</div>';

                try {
                    const response = await fetch(`/api/reports/${reportType}`);
                    const data = await response.json();

                    if (data.error) {
                        resultsDiv.innerHTML = `<div class="error">❌ Error: ${data.error}</div>`;
                        return;
                    }

                    displayReport(reportType, data);
                } catch (error) {
                    resultsDiv.innerHTML = `<div class="error">❌ Error: ${error.message}</div>`;
                }
            }

            function displayReport(reportType, data) {
                const resultsDiv = document.getElementById('results');
                let html = '<div class="results">';

                html += `<h2>${getReportTitle(reportType)}</h2>`;

                if (data.summary) {
                    html += '<div style="margin: 20px 0;">';
                    for (const [key, value] of Object.entries(data.summary)) {
                        html += `
                            <div class="metric">
                                <div class="metric-value">${value}</div>
                                <div class="metric-label">${key}</div>
                            </div>
                        `;
                    }
                    html += '</div>';
                }

                if (data.data && data.data.length > 0) {
                    html += createTable(data.data);
                } else {
                    html += '<p>No data available</p>';
                }

                html += '</div>';
                resultsDiv.innerHTML = html;
            }

            function getReportTitle(reportType) {
                const titles = {
                    'weekly-trends': '📈 Weekly Flu Activity Trends',
                    'county-summary': '🗺️ County-Level Summary Statistics',
                    'seasonal-comparison': '📅 Seasonal Comparison Analysis',
                    'healthcare-impact': '🏨 Healthcare System Impact',
                    'top-affected-counties': '⚠️ Top Affected Counties',
                    'historical-summary': '📊 Historical Flu Season Summary'
                };
                return titles[reportType] || reportType;
            }

            function createTable(records) {
                if (records.length === 0) return '<p>No records</p>';

                const headers = Object.keys(records[0]);
                let html = '<table><thead><tr>';
                headers.forEach(header => {
                    html += `<th>${header.replace(/_/g, ' ').toUpperCase()}</th>`;
                });
                html += '</tr></thead><tbody>';

                records.forEach(record => {
                    html += '<tr>';
                    headers.forEach(header => {
                        let value = record[header];
                        if (value !== null && typeof value === 'number') {
                            value = value.toLocaleString();
                        }
                        html += `<td>${value !== null ? value : 'N/A'}</td>`;
                    });
                    html += '</tr>';
                });
                html += '</tbody></table>';
                return html;
            }
        </script>
    </body>
    </html>
    '''
    return render_template_string(html)


@app.route('/api/reports/weekly-trends')
def weekly_trends():
    """Get weekly flu activity trends"""
    try:
        query = text("""
            SELECT
                t.week_end,
                t.epiweek_id,
                t.season,
                i.respiratory_illness_type,
                AVG(i.county_ili_percent) as avg_percent_positive,
                COUNT(DISTINCT i.county_id) as counties_reporting
            FROM temporal t
            LEFT JOIN illness i ON t.epiweek_id = i.epiweek_id
            WHERE i.respiratory_illness_type IS NOT NULL
            GROUP BY t.week_end, t.epiweek_id, t.season, i.respiratory_illness_type
            HAVING AVG(i.county_ili_percent) IS NOT NULL
            ORDER BY t.week_end DESC, i.respiratory_illness_type
            LIMIT 20
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            rows = result.fetchall()

            # Format the data with proper percentages
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                # Format percentages
                if row_dict.get('avg_percent_positive') is not None:
                    row_dict['avg_percent_positive'] = f"{row_dict['avg_percent_positive']:.2f}%"
                data.append(row_dict)

        summary = {}
        if data:
            summary['Latest Week'] = str(data[0]['week_end']) if data[0]['week_end'] else 'N/A'
            summary['Avg County %'] = data[0]['avg_percent_positive'] if data[0]['avg_percent_positive'] else 'N/A'
            summary['Illness Type'] = data[0]['respiratory_illness_type']

        return jsonify({'data': data, 'summary': summary}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/reports/county-summary')
def county_summary():
    """Get summary statistics by county"""
    try:
        query = text("""
            SELECT
                cr.county_name,
                cr.ach_region,
                COUNT(DISTINCT t.epiweek_id) as weeks_reporting,
                AVG(i.county_ili_percent) as avg_percent_positive,
                MAX(i.county_ili_percent) as max_percent_positive,
                AVG(i.deviation_from_state_average) as avg_deviation
            FROM county_region cr
            LEFT JOIN illness i ON cr.county_id = i.county_id
            LEFT JOIN temporal t ON i.epiweek_id = t.epiweek_id
            WHERE i.respiratory_illness_type = 'Flu'
            GROUP BY cr.county_name, cr.ach_region
            ORDER BY avg_percent_positive DESC NULLS LAST
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            rows = result.fetchall()

            # Format the data with proper percentages
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                if row_dict.get('avg_percent_positive') is not None:
                    row_dict['avg_percent_positive'] = f"{row_dict['avg_percent_positive']:.2f}%"
                if row_dict.get('max_percent_positive') is not None:
                    row_dict['max_percent_positive'] = f"{row_dict['max_percent_positive']:.2f}%"
                if row_dict.get('avg_deviation') is not None:
                    row_dict['avg_deviation'] = f"{row_dict['avg_deviation']:.2f}%"
                data.append(row_dict)

        summary = {
            'Total Counties': len(data),
            'Reporting Counties': sum(1 for d in data if d['weeks_reporting'] > 0)
        }

        return jsonify({'data': data, 'summary': summary}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/reports/seasonal-comparison')
def seasonal_comparison():
    """Compare flu seasons"""
    try:
        query = text("""
            SELECT
                t.season,
                COUNT(DISTINCT t.epiweek_id) as weeks_in_season,
                AVG(i.county_ili_percent) as avg_percent_positive,
                MAX(i.county_ili_percent) as peak_percent_positive,
                AVG(i.state_ili_percent) as state_avg_percent
            FROM temporal t
            LEFT JOIN illness i ON t.epiweek_id = i.epiweek_id
            WHERE t.season IS NOT NULL AND i.respiratory_illness_type = 'Flu'
            GROUP BY t.season
            ORDER BY t.season DESC
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            rows = result.fetchall()

            # Format the data with proper percentages
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                if row_dict.get('avg_percent_positive') is not None:
                    row_dict['avg_percent_positive'] = f"{row_dict['avg_percent_positive']:.2f}%"
                if row_dict.get('peak_percent_positive') is not None:
                    row_dict['peak_percent_positive'] = f"{row_dict['peak_percent_positive']:.2f}%"
                if row_dict.get('state_avg_percent') is not None:
                    row_dict['state_avg_percent'] = f"{row_dict['state_avg_percent']:.2f}%"
                data.append(row_dict)

        return jsonify({'data': data}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/reports/healthcare-impact')
def healthcare_impact():
    """Healthcare system impact metrics"""
    try:
        query = text("""
            SELECT
                cr.county_name,
                cr.ach_region,
                h.population_density_2020,
                h.hospitalization_percent,
                h.er_visit_percent,
                h.hospital_to_er_ratio
            FROM healthcare h
            JOIN county_region cr ON h.county_id = cr.county_id
            WHERE h.hospitalization_percent > 0 OR h.er_visit_percent > 0
            ORDER BY h.hospitalization_percent DESC NULLS LAST
            LIMIT 30
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            rows = result.fetchall()

            # Format the data with proper percentages
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                if row_dict.get('hospitalization_percent') is not None:
                    row_dict['hospitalization_percent'] = f"{row_dict['hospitalization_percent']:.2f}%"
                if row_dict.get('er_visit_percent') is not None:
                    row_dict['er_visit_percent'] = f"{row_dict['er_visit_percent']:.2f}%"
                if row_dict.get('hospital_to_er_ratio') is not None:
                    row_dict['hospital_to_er_ratio'] = f"{row_dict['hospital_to_er_ratio']:.3f}"
                data.append(row_dict)

        summary = {
            'Counties Analyzed': len(data)
        }

        return jsonify({'data': data, 'summary': summary}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/reports/top-affected-counties')
def top_affected_counties():
    """Get top affected counties by flu activity"""
    try:
        query = text("""
            SELECT
                cr.county_name,
                cr.ach_region,
                AVG(i.county_ili_percent) as avg_percent_positive,
                MAX(i.county_ili_percent) as peak_percent_positive,
                AVG(i.deviation_from_state_average) as avg_deviation,
                COUNT(DISTINCT t.epiweek_id) as weeks_reporting
            FROM county_region cr
            JOIN illness i ON cr.county_id = i.county_id
            JOIN temporal t ON i.epiweek_id = t.epiweek_id
            WHERE i.respiratory_illness_type = 'Flu' AND i.care_type = 'Hospitalizations'
            GROUP BY cr.county_name, cr.ach_region
            HAVING AVG(i.county_ili_percent) IS NOT NULL
            ORDER BY avg_percent_positive DESC
            LIMIT 15
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            rows = result.fetchall()

            # Format the data with proper percentages
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                if row_dict.get('avg_percent_positive') is not None:
                    row_dict['avg_percent_positive'] = f"{row_dict['avg_percent_positive']:.2f}%"
                if row_dict.get('peak_percent_positive') is not None:
                    row_dict['peak_percent_positive'] = f"{row_dict['peak_percent_positive']:.2f}%"
                if row_dict.get('avg_deviation') is not None:
                    row_dict['avg_deviation'] = f"{row_dict['avg_deviation']:.2f}%"
                data.append(row_dict)

        if data:
            summary = {
                'Highest Avg': data[0]['avg_percent_positive'],
                'Top County': data[0]['county_name']
            }
        else:
            summary = {}

        return jsonify({'data': data, 'summary': summary}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/reports/historical-summary')
def historical_summary():
    """Historical flu season summary from historics table"""
    try:
        query = text("""
            SELECT
                year,
                decade_year,
                peak_week_id,
                peak_ili_percent,
                average_wili_percent,
                peak_vs_avg_diff
            FROM historics
            ORDER BY year DESC
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            rows = result.fetchall()

            # Format the data with proper percentages
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                if row_dict.get('peak_ili_percent') is not None:
                    row_dict['peak_ili_percent'] = f"{row_dict['peak_ili_percent']:.2f}%"
                if row_dict.get('average_wili_percent') is not None:
                    row_dict['average_wili_percent'] = f"{row_dict['average_wili_percent']:.2f}%"
                if row_dict.get('peak_vs_avg_diff') is not None:
                    row_dict['peak_vs_avg_diff'] = f"{row_dict['peak_vs_avg_diff']:.2f}%"
                data.append(row_dict)

        if data:
            # Find max peak
            max_peak = max((float(d['peak_ili_percent'].rstrip('%')) for d in data if d.get('peak_ili_percent')), default=0)
            summary = {
                'Years Tracked': len(data),
                'Highest Peak': f"{max_peak:.2f}%"
            }
        else:
            summary = {}

        return jsonify({'data': data, 'summary': summary}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/csv')
def export_csv():
    """Export table data as CSV"""
    table = request.args.get('table', '')
    valid_tables = ['county_region', 'healthcare', 'historics', 'illness', 'temporal']

    if table not in valid_tables:
        return jsonify({'error': f'Invalid table. Choose from: {", ".join(valid_tables)}'}), 400

    try:
        query = text(f"SELECT * FROM {table} LIMIT 1000")

        with engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            rows = result.fetchall()

        # Create CSV in memory
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)

        csv_data = output.getvalue()

        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={table}.csv'}
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

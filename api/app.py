from flask import Flask, jsonify
from sqlalchemy import create_engine, text
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
        'status': 'running'
    })

@app.route('/health')
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({'status': 'healthy', 'database': 'connected'}), 200
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

@task
def create_sql_tables():
    # Connect to PostgreSQL databases
    conn = psycopg2.connect(
        dbname='flu_database',
        user='fluuser',
        password='flupass',
        host='postgres',
        port='5432'
    )

    print("Connected to PostgreSQL database")

    # Create a cursor object
    cur = conn.cursor()

    # Create Schema
    cur.execute("CREATE SCHEMA IF NOT EXISTS flu_schema;")
    print("Schema created/verified")

    # Create Table 1 (County/Region Reference) - NO FOREIGN KEYS
    cur.execute("DROP TABLE IF EXISTS flu_schema.county_region CASCADE;")
    cur.execute("""CREATE TABLE flu_schema.county_region (
                    county_id INT PRIMARY KEY,
                    county_name TEXT,
                    ach_region TEXT,
                    population_density_2020 FLOAT
                    );"""
                )
    print("Table 1 (county_region) created")

    # Create Table 2 (Temporal Reference) - NO FOREIGN KEYS
    cur.execute("DROP TABLE IF EXISTS flu_schema.temporal CASCADE;")
    cur.execute("""CREATE TABLE flu_schema.temporal (
                    epiweek_id INT PRIMARY KEY,
                    week_start DATE,
                    week_end DATE,
                    season TEXT
                    );"""
                )
    print("Table 2 (temporal) created")

    # Create Table 3 (County/Weekly Illness Comparison) - HAS FOREIGN KEYS
    cur.execute("DROP TABLE IF EXISTS flu_schema.illness CASCADE;")
    cur.execute("""CREATE TABLE flu_schema.illness (
                    epiweek_id INT,
                    county_id INT,
                    respiratory_illness_type TEXT,
                    care_type TEXT,
                    county_ili_percent FLOAT,
                    state_ili_percent FLOAT,
                    deviation_from_state_average FLOAT,
                    PRIMARY KEY (epiweek_id, county_id, respiratory_illness_type, care_type),
                    FOREIGN KEY (epiweek_id) REFERENCES flu_schema.temporal(epiweek_id),
                    FOREIGN KEY (county_id) REFERENCES flu_schema.county_region(county_id)
                    );"""
                )
    print("Table 3 (illness) created")

    # Create Table 4 (Healthcare Utilization) - HAS FOREIGN KEYS
    cur.execute("DROP TABLE IF EXISTS flu_schema.healthcare CASCADE;")
    cur.execute("""CREATE TABLE flu_schema.healthcare (
                    county_id INT PRIMARY KEY,
                    population_density_2020 FLOAT,
                    hospitalization_percent FLOAT,
                    er_visit_percent FLOAT,
                    hospital_to_er_ratio FLOAT,
                    FOREIGN KEY (county_id) REFERENCES flu_schema.county_region(county_id)
                    );"""
                )
    print("Table 4 (healthcare) created")

    # Create Table 5 (Historical Flu Season Summary) - NO FOREIGN KEYS
    cur.execute("DROP TABLE IF EXISTS flu_schema.historics CASCADE;")
    cur.execute("""CREATE TABLE flu_schema.historics (
                    year INT PRIMARY KEY,
                    decade_year INT,
                    peak_week_id INT,
                    peak_ili_percent FLOAT,
                    average_wili_percent FLOAT,
                    peak_vs_avg_diff FLOAT
                    );"""
                )
    print("Table 5 (historics) created")

    print("All Tables created in PostgreSQL database")

    conn.commit()
    cur.close()
    conn.close()
    print("PostgreSQL connection closed")


@task
def ingest_sql_data(temporal_path, illness_path, healthcare_path, historic_path, county_region_path):

    # Connect to PostgreSQL databases
    engine = create_engine('postgresql://fluuser:flupass@postgres/flu_database')
    conn = psycopg2.connect(
        dbname='flu_database',
        user='fluuser',
        password='flupass',
        host='postgres',
        port='5432'
    )

    # Create a cursor object
    cur = conn.cursor()

    print("Connected to PostgreSQL database")

    # County/Region Data Ingest

    # Staging Table
    cur.execute("DROP TABLE IF EXISTS temp;")
    cur.execute("""CREATE TABLE temp (
                    county_id INT PRIMARY KEY,
                    county_name TEXT,
                    ach_region TEXT,
                    population_density_2020 FLOAT
                    );"""
                )

    # Ingest region_county data
    with open(county_region_path, 'r') as f:
        sql = """
            COPY temp
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

        # Copy to Primary Table
        cur.execute("""INSERT INTO flu_schema.county_region (county_id, county_name, ach_region, population_density_2020)
                    SELECT *
                    FROM temp
                    ON CONFLICT (county_id) DO NOTHING;"""
                    )

    # Print Confirmation
    print("Table 1 data ingested")

    # Temporal Data Ingest

    # Staging Table 2
    cur.execute("DROP TABLE IF EXISTS temp;")
    cur.execute("""CREATE TABLE temp (
                    epiweek_id INT PRIMARY KEY,
                    week_start DATE,
                    week_end DATE,
                    season TEXT
                    );"""
                )

    # Temporal Ingest
    with open(temporal_path, 'r') as f:
        sql = """
            COPY temp
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

        # Copy to Primary Table
        cur.execute("""INSERT INTO flu_schema.temporal (epiweek_id, week_start, week_end, season)
                    SELECT *
                    FROM temp
                    ON CONFLICT (epiweek_id) DO NOTHING;"""
                    )

    # Print Confirmation
    print("Table 2 data ingested")

    # Illness Data Ingest

    # Staging Table 3
    cur.execute("DROP TABLE IF EXISTS temp;")
    cur.execute("""CREATE TABLE temp (
                    epiweek_id INT,
                    county_id INT,
                    respiratory_illness_type TEXT,
                    care_type TEXT,
                    county_ili_percent FLOAT,
                    state_ili_percent FLOAT,
                    deviation_from_state_average FLOAT,
                    PRIMARY KEY (epiweek_id, county_id, respiratory_illness_type, care_type)
                    );"""
                )

    # Illness Ingest
    with open(illness_path, 'r') as f:
        sql = """
            COPY temp
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

        # Copy to Primary Table
        cur.execute("""INSERT INTO flu_schema.illness (epiweek_id, county_id, respiratory_illness_type, care_type, county_ili_percent, state_ili_percent, deviation_from_state_average)
                    SELECT *
                    FROM temp
                    ON CONFLICT (epiweek_id, county_id, respiratory_illness_type, care_type) DO NOTHING;"""
                    )

    # Print Confirmation
    print("Table 3 data ingested")

    # Healthcare Data Ingest

    # Staging Table 4
    cur.execute("DROP TABLE IF EXISTS temp;")
    cur.execute("""CREATE TABLE temp (
                    county_id INT PRIMARY KEY,
                    population_density_2020 FLOAT,
                    hospitalization_percent FLOAT,
                    er_visit_percent FLOAT,
                    hospital_to_er_ratio FLOAT
                    );"""
                )

    # Ingest to Staging
    with open(healthcare_path, 'r') as f:
        sql = """
            COPY temp
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

        # Add to Primary Table
        cur.execute("""INSERT INTO flu_schema.healthcare (county_id, population_density_2020, hospitalization_percent, er_visit_percent, hospital_to_er_ratio)
                    SELECT *
                    FROM temp
                    ON CONFLICT (county_id) DO NOTHING;"""
                    )

    # Print Confirmation
    print("Table 4 data ingested")

    # Historic Data Ingest

    # Staging Table
    cur.execute("DROP TABLE IF EXISTS temp;")
    cur.execute("""CREATE TABLE temp (
                    year INT PRIMARY KEY,
                    decade_year INT,
                    peak_week_id INT,
                    peak_ili_percent FLOAT,
                    average_wili_percent FLOAT,
                    peak_vs_avg_diff FLOAT
                    );"""
                )

    # Ingest To Staging
    with open(historic_path, 'r') as f:
        sql = """
            COPY temp
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

        # Add to Primary Table
        cur.execute("""INSERT INTO flu_schema.historics (year, decade_year, peak_week_id, peak_ili_percent, average_wili_percent, peak_vs_avg_diff)
                    SELECT *
                    FROM temp
                    ON CONFLICT (year) DO NOTHING;"""
                    )

    # Print Confirmation
    print("Table 5 data ingested")

    # Close Connections and Commit
    conn.commit()
    cur.close()
    conn.close()

    print("PostgreSQL connection closed")

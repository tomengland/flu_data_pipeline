# %%
# Imports
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import psycopg2
from datetime import datetime, timedelta
import os
import json
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
import traceback


# Folder creation
os.makedirs('/app/data/raw', exist_ok=True)
os.makedirs('/app/processed_files', exist_ok=True)

# Open database connections
engine = create_engine('postgresql://fluuser:flupass@postgres/flu_database')


# Default arguments for Airflow DAG
default_args = {
    'owner': 'healt_data_team',
    'start_date': datetime(2025, 11, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# %%
# Define Functions for DAG Tasks

@task
def collect_rhino_data():

    # WA DOH RHINO Data
    print("\n" + "=" * 60)
    print("COLLECTING WA DOH RHINO DATA")
    print("=" * 60)

    # WA DOH RHINO downloadable data
    doh_rhino_url = "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/Respiratory_Disease_RHINO_Downloadable_Data.csv"

    # ACH to Counties mapping - MUST match official WA county names exactly
    ach_to_counties = {
        "Better Health Together": ["Spokane", "Stevens", "Pend Oreille", "Ferry"],
        "Cascade Pacific Action Alliance": ["Thurston", "Mason", "Grays Harbor", "Pacific", "Lewis"],
        "Elevate Health": ["Yakima", "Kittitas"],
        "Greater Health Now": ["Spokane"],  # Urban Spokane focus - duplicate with Better Health Together
        "Healthier Here": ["King"],
        "North Sound": ["Whatcom", "Skagit", "Snohomish", "San Juan", "Island"],
        "Olympic Community of Health": ["Clallam", "Jefferson", "Kitsap"],
        "Southwest Washington": ["Clark", "Skamania", "Klickitat", "Cowlitz", "Wahkiakum"],
        "Thriving Together NCW": ["Chelan", "Douglas", "Grant", "Okanogan"]
    }

    # Official WA State counties for validation
    wa_counties = [
        "Adams", "Asotin", "Benton", "Chelan", "Clallam", "Clark", "Columbia", "Cowlitz",
        "Douglas", "Ferry", "Franklin", "Garfield", "Grant", "Grays Harbor", "Island",
        "Jefferson", "King", "Kitsap", "Kittitas", "Klickitat", "Lewis", "Lincoln",
        "Mason", "Okanogan", "Pacific", "Pend Oreille", "Pierce", "San Juan", "Skagit",
        "Skamania", "Snohomish", "Spokane", "Stevens", "Thurston", "Wahkiakum",
        "Walla Walla", "Whatcom", "Whitman", "Yakima"
    ]

    try:
        df_doh_rhino = pd.read_csv(doh_rhino_url)

        # Add source column
        df_doh_rhino['source'] = 'WA_DOH_RHINO'

        print(f"\n✓ DOH RHINO data loaded: {len(df_doh_rhino)} records")
        print(f"  Original columns: {df_doh_rhino.columns.tolist()}")

        # Validate counties in mapping
        print(f"\n Validating County Mapping:")
        all_mapped_counties = set()
        for ach, counties in ach_to_counties.items():
            all_mapped_counties.update(counties)

        # Check for invalid county names
        invalid_counties = all_mapped_counties - set(wa_counties)
        if invalid_counties:
            print(f"     WARNING: Invalid county names found: {invalid_counties}")

        # Check for unmapped counties
        unmapped_counties = set(wa_counties) - all_mapped_counties
        if unmapped_counties:
            print(f"     WARNING: Counties not in any ACH: {sorted(unmapped_counties)}")
            print(f"      ({len(unmapped_counties)} counties: likely Pierce, Adams, Asotin, Benton, Columbia, Franklin, Garfield, Lincoln, Walla Walla, Whitman)")

        print(f"   ✓ {len(all_mapped_counties)} counties mapped across {len(ach_to_counties)} ACH regions")

        # Remove Statewide and Unassigned records before exploding
        original_count = len(df_doh_rhino)
        df_doh_rhino = df_doh_rhino[
            ~df_doh_rhino['Location'].isin(['Statewide', 'Unassigned ACH Region'])
        ].copy()
        removed_count = original_count - len(df_doh_rhino)
        print(f"\n🗑️  Removed {removed_count} Statewide/Unassigned records")
        print(f"   Remaining: {len(df_doh_rhino)} ACH region records")

        # Map ACH to counties and explode
        df_doh_rhino['county_list'] = df_doh_rhino['Location'].map(ach_to_counties)

        # Explode: create one row per county
        df_doh_rhino_exploded = df_doh_rhino.explode('county_list').reset_index(drop=True)

        # Rename county_list to county for clarity
        df_doh_rhino_exploded.rename(columns={'county_list': 'county'}, inplace=True)

        print(f"\n After County Explosion:")
        print(f"   - Original ACH records: {len(df_doh_rhino)}")
        print(f"   - Exploded county records: {len(df_doh_rhino_exploded)}")
        print(f"   - Expansion factor: {len(df_doh_rhino_exploded) / len(df_doh_rhino):.2f}x")

        # Verify unique counties
        unique_counties = df_doh_rhino_exploded['county'].unique()
        print(f"\n   Unique counties in data: {len(unique_counties)}")
        print(f"   Counties: {sorted(unique_counties)}")

        # Show county record counts
        print(f"\n Records per County:")
        county_counts = df_doh_rhino_exploded['county'].value_counts().sort_index()
        for county, count in county_counts.items():
            # Show which ACH regions include this county
            achs = [ach for ach, counties in ach_to_counties.items() if county in counties]
            ach_str = ", ".join(achs)
            print(f"   - {county}: {count:,} records (ACH: {ach_str})")

        # Date range
        print(f"\n Date Range:")
        print(f"   - From: {df_doh_rhino_exploded['Week Start'].min()}")
        print(f"   - To: {df_doh_rhino_exploded['Week End'].max()}")

        # Clean up the percentage data
        def clean_percentage(value):
            """Convert empty strings to NaN, keep numeric values"""
            if pd.isna(value):
                return None
            if isinstance(value, str):
                if value.strip() == '':
                    return None
            try:
                return float(value)
            except:
                return None

        df_doh_rhino_exploded['1-Week Percent_cleaned'] = df_doh_rhino_exploded['1-Week Percent '].apply(clean_percentage)

        # Show data dimensions
        print(f"\n Data Dimensions:")
        print(f"   - Seasons: {df_doh_rhino_exploded['Season'].nunique()}")
        print(f"   - Counties: {df_doh_rhino_exploded['county'].nunique()}")
        print(f"   - Respiratory Illnesses: {', '.join(df_doh_rhino_exploded['Respiratory Illness Category'].unique())}")
        print(f"   - Care Types: {', '.join(df_doh_rhino_exploded['Care Type'].unique())}")
        print(f"   - Demographic Categories: {', '.join(df_doh_rhino_exploded['Demographic Category'].unique())}")

        # Example: Latest flu data by county
        latest_week = df_doh_rhino_exploded['Week End'].max()
        latest_flu_hosp = df_doh_rhino_exploded[
            (df_doh_rhino_exploded['Week End'] == latest_week) &
            (df_doh_rhino_exploded['Respiratory Illness Category'] == 'Flu') &
            (df_doh_rhino_exploded['Care Type'] == 'Hospitalizations') &
            (df_doh_rhino_exploded['Demographic Category'] == 'Overall')
        ].copy()

        if len(latest_flu_hosp) > 0:
            print(f"\n Latest Flu Hospitalizations by County ({latest_week}):")
            latest_flu_hosp_sorted = latest_flu_hosp.sort_values('1-Week Percent_cleaned', ascending=False)
            for _, row in latest_flu_hosp_sorted.head(10).iterrows():
                pct = row['1-Week Percent_cleaned']
                if pd.notna(pct):
                    print(f"   - {row['county']}: {pct}% (from {row['Location']})")

        # Data quality
        total_rows = len(df_doh_rhino_exploded)
        data_rows = df_doh_rhino_exploded['1-Week Percent_cleaned'].notna().sum()
        empty_rows = total_rows - data_rows

        print(f"\n Data Quality:")
        print(f"   - Total records: {total_rows:,}")
        print(f"   - Records with data: {data_rows:,} ({data_rows/total_rows*100:.1f}%)")
        print(f"   - Empty/suppressed: {empty_rows:,} ({empty_rows/total_rows*100:.1f}%)")

        # Save
        rhino_path = '/app/data/raw/wa_doh_rhino.csv'
        df_doh_rhino_exploded.to_csv(rhino_path, index=False)
        print(f"\n Saved to: {rhino_path}")

        #rhino_path = "/app/data/raw/rhino.parquet"
        #df_doh_rhino_exploded.to_parquet(rhino_path)
        return rhino_path

        print("\n📋 Sample records (showing county-level data):")
        sample_cols = ['county', 'Location', 'Week Start', 'Week End', 'Respiratory Illness Category', 'Care Type', '1-Week Percent_cleaned']
        print(df_doh_rhino_exploded[sample_cols].head(20).to_string(index=False))

    except Exception as e:
        print(f" Error: {e}")
        import traceback
        traceback.print_exc()

@task
def collect_census_data():
    ## Collect census
    print("=" * 60)
    print("COLLECTING CENSUS DATA")
    print("=" * 60)

    # Download census data
    census_url = "https://data.wa.gov/api/views/e6ip-wkqq/rows.csv?accessType=DOWNLOAD"

    try:
        df_census = pd.read_csv(census_url)

        print(f"\n Census data loaded: {len(df_census)} counties")
        print(f" Columns: {df_census.columns.tolist()}")

        # Check for missing values in 2020 data
        missing_2020 = df_census['Population Density 2020'].isna().sum()
        print(f"\n Missing 2020 density values: {missing_2020}")

        # Show summary statistics
        print("\n 2020 Population Density Statistics:")
        print(df_census['Population Density 2020'].describe())

        # Show top 5 most dense counties
        print("\n Top 5 Most Dense Counties (2020):")
        top_counties = df_census.nlargest(5, 'Population Density 2020')[['County Name', 'Population Density 2020']]
        print(top_counties.to_string(index=False))

        # Save to raw data
        census_path = '/app/data/raw/wa_population_density.csv'
        df_census.to_csv(census_path, index=False)
        print(f"\n Saved to: {census_path}")

        print("\nFirst 5 rows:")
        print(df_census.head())

        #census_path = "/app/data/raw/census.parquet"
        #df_census.to_parquet(census_path)
        return census_path

    except Exception as e:
        print(f" Error: {e}")
        import traceback
        traceback.print_exc()

@task
def collect_fluview_data():

    # Collect FluView data
    print("\n" + "=" * 60)
    print("COLLECTING CDC FLUVIEW DATA")
    print("=" * 60)

    # API endpoint
    api_url = "https://api.delphi.cmu.edu/epidata/fluview/"

    # Parameters - Get data from 2020 onwards
    params = {
        'regions': 'wa',
        'epiweeks': '202001-202452'  # 2020 through 2024
    }

    try:
        # Make API request
        response = requests.get(api_url, params=params)
        data = response.json()

        # Check if successful
        if data['result'] == 1:
            df_fluview = pd.DataFrame(data['epidata'])

            print(f"\n FluView data loaded: {len(df_fluview)} weeks")
            print(f" Date range: {df_fluview['epiweek'].min()} to {df_fluview['epiweek'].max()}")

            # Show key columns
            print(f"\n Key columns:")
            key_cols = ['region', 'epiweek', 'num_ili', 'num_patients', 'wili']
            print(f"   {key_cols}")

            # Summary statistics
            print("\n ILI Statistics:")
            print(f"   - Average ILI cases per week: {df_fluview['num_ili'].mean():.0f}")
            print(f"   - Max ILI cases in a week: {df_fluview['num_ili'].max()}")
            print(f"   - Average % ILI: {df_fluview['wili'].mean():.2f}%")
            print(f"   - Max % ILI: {df_fluview['wili'].max():.2f}%")

            # Show weeks with highest ILI
            print("\n Top 5 Weeks by ILI Percentage:")
            top_ili = df_fluview.nlargest(5, 'wili')[['epiweek', 'num_ili', 'num_patients', 'wili']]
            print(top_ili.to_string(index=False))

            # Save to raw data
            fluview_path = '/app/data/raw/wa_fluview_data.csv'
            df_fluview.to_csv(fluview_path, index=False)
            print(f"\n Saved to: {fluview_path}")

            print("\nFirst 5 rows:")
            print(df_fluview.head())

            
            #fluview_path = "/app/data/raw/fluview.parquet"
            #df_fluview.to_parquet(fluview_path)
            return fluview_path

        else:
            print(f" API Error: {data.get('message', 'Unknown error')}")

    except Exception as e:
        print(f" Error: {e}")
        traceback.print_exc()

@task
def create_dataframe_tables(census_path, rhino_path, fluview_path):
    
    df_census = pd.read_csv(census_path)
    df_doh_rhino_exploded = pd.read_csv(rhino_path)
    df_fluview = pd.read_csv(fluview_path)

    # Create Table 1 DF -----------------------------------------------------------------------------------

    # Set up unique county names
    df_county_region = df_census[['County Name', 'Population Density 2020']].drop_duplicates().sort_values(by='County Name', ascending=True).reset_index(drop=True)

    # Add ACH region based on county
    df_county_region = pd.merge(df_county_region, df_doh_rhino_exploded[['county', 'Location']].drop_duplicates(), left_on='County Name', right_on='county', how='left')

    # Combine Spokane ACH regions
    df_county_region = (df_county_region.groupby(["County Name", "Population Density 2020"], dropna=False)["Location"].apply(lambda x: ", ".join(sorted(x.dropna().unique()))).reset_index())
    df_county_region['Location'].replace(r'^\s*$', 'Unassigned', regex = True, inplace=True)

    # Add in county_id
    df_county_region['county_id'] = df_county_region.index + 1

    # Rename columns
    df_county_region.rename(columns={'County Name': 'county_name', 'Location': 'ach_region', 'Population Density 2020': 'population_density_2020'}, inplace=True)

    # Reorder columns
    df_county_region = df_county_region[['county_id', 'county_name', 'ach_region', 'population_density_2020']]

    # Create Table 2 DF -------------------------------------------------------------------------------------

    # Create rhino epiweek_id column
    df_doh_rhino_exploded['epiweek_id'] = df_doh_rhino_exploded['Week End'].str[:4] + df_doh_rhino_exploded['Week'].astype(str).str.zfill(2)

    df_temporal = df_doh_rhino_exploded[['epiweek_id', 'Week Start', 'Week End', 'Season']].drop_duplicates().reset_index(drop=True)
    df_temporal = df_temporal.sort_values(by=['epiweek_id'], ascending=True).reset_index(drop=True)

    # Convert Datatypes (Later...)
    df_temporal['epiweek_id'] = df_temporal['epiweek_id'].astype(int)
    df_temporal['Week Start'] = pd.to_datetime(df_temporal['Week Start'])
    df_temporal['Week End'] = pd.to_datetime(df_temporal['Week End'])

    # Rename Columns
    df_temporal.rename(columns={'Week Start': 'week_start', 'Week End': 'week_end', 'Season': 'season'}, inplace=True)

    # Create Table 3 DF ---------------------------------------------------------------------------------------

    df_illness = df_doh_rhino_exploded[['epiweek_id', 'county', 'Respiratory Illness Category', 'Care Type', '1-Week Percent_cleaned']].copy()

    # Add in county_id by merging with county_region DF
    df_illness = pd.merge(df_illness, df_county_region[['county_id', 'county_name']], left_on='county', right_on='county_name', how='left')
    df_illness.drop(columns=['county', 'county_name'], inplace=True)

    # Add in state_ili_percent from fluview
    df_illness['epiweek_id'] = df_illness['epiweek_id'].astype(int)
    df_illness = pd.merge(df_illness, df_fluview[['epiweek', 'wili']], left_on='epiweek_id', right_on='epiweek', how='left')
    df_illness.rename(columns={'wili': 'state_ili_percent'}, inplace=True)
    df_illness.drop(columns=['epiweek'], inplace=True)
    df_illness.drop_duplicates(subset=['epiweek_id', 'county_id', 'Respiratory Illness Category', 'Care Type'], inplace=True)

    # Create Difference Column
    df_illness['deviation_from_state_average'] = df_illness['1-Week Percent_cleaned'] - df_illness['state_ili_percent']

    # Rename Columns
    df_illness.rename(columns={'Respiratory Illness Category': 'respiratory_illness_type',
                                'Care Type': 'care_type',
                                '1-Week Percent_cleaned': 'county_ili_percent'}, inplace=True)

    # Reorder Columns
    df_illness = df_illness[['epiweek_id', 'county_id', 'respiratory_illness_type', 'care_type', 'county_ili_percent', 'state_ili_percent', 'deviation_from_state_average']]


    # Create Table 4 DF-------------------------------------------------------------------------------------

    df_healthcare = df_county_region[['county_id', 'county_name', 'population_density_2020']].copy()
    df_healthcare = pd.merge(df_healthcare, df_doh_rhino_exploded[['county', 'Respiratory Illness Category', 'Care Type', '1-Week Percent_cleaned']].drop_duplicates(), left_on='county_name', right_on='county', how='left')

    # Add in and calculated generics rates
    df_healthcare['rates'] = df_healthcare.groupby(['county_id', 'Care Type'])['1-Week Percent_cleaned'].transform('mean')

    # Drop Extra Columns
    df_healthcare.drop(columns=['county', '1-Week Percent_cleaned', 'county_name', 'Respiratory Illness Category'], inplace=True)

    # Combine by groups
    df_healthcare = df_healthcare.drop_duplicates().reset_index(drop=True)

    # Separate by Care Type
    df_healthcare['hospitalization_percent'] = df_healthcare.apply(lambda row: row['rates'] if row['Care Type'] == 'Hospitalizations' else None, axis=1)
    df_healthcare['er_visit_percent'] = df_healthcare.apply(lambda row: row['rates'] if row['Care Type'] == 'Emergency Visits' else None, axis=1)

    # Consolidate and drop extras
    df_healthcare.drop(columns=['Care Type', 'rates'], inplace=True)
    df_healthcare = df_healthcare.groupby(['county_id', 'population_density_2020'], as_index=False).agg('first')

    # Calculate Ratio
    df_healthcare['hospital_to_er_ratio'] = df_healthcare['hospitalization_percent'] / df_healthcare['er_visit_percent']

    # Fill NaN
    df_healthcare.fillna(0, inplace=True)


    # Create Table 5 DF-------------------------------------------------------------------------------------

    # Starting Point
    df_historics = df_fluview[['epiweek','wili']].copy()

    # Create year and decade_year
    df_historics['year'] = df_historics['epiweek'].astype(str).str[:4].astype(int)
    df_historics['decade_year'] = (df_historics['year'] // 10) * 10

    # Find Peak wili and epiweek id
    df_historics['peak_ili_percent'] = df_historics.groupby('year')['wili'].transform('max')
    df_historics['peak_week_id'] = df_historics.groupby('year')['wili'].transform(lambda x: df_historics.loc[x.idxmax(), 'epiweek'])

    # Find yearly average
    df_historics['average_wili_percent'] = df_historics.groupby('year')['wili'].transform('mean')

    # Find peak vs average difference
    df_historics['peak_vs_avg_diff'] = df_historics['peak_ili_percent'] - df_historics['average_wili_percent']

    # Skip population density

    # Reorder columns
    df_historics = df_historics[['year', 'decade_year', 'peak_week_id', 'peak_ili_percent', 'average_wili_percent', 'peak_vs_avg_diff']].drop_duplicates().reset_index(drop=True)

    # Create .csv files for export
    temporal_path = '/app/processed_files/temporal.csv'
    illness_path = '/app/processed_files/illness.csv'
    healthcare_path = '/app/processed_files/healthcare.csv'
    historic_path = '/app/processed_files/historic_flu.csv'
    county_region_path = '/app/processed_files/county_region.csv'

    # Export all DFs to CSV for SQL Ingest
    df_temporal.to_csv('/app/processed_files/temporal.csv', index=False)
    df_illness.to_csv('/app/processed_files/illness.csv', index=False)
    df_healthcare.to_csv('/app/processed_files/healthcare.csv', index=False)
    df_historics.to_csv('/app/processed_files/historic_flu.csv', index=False)
    df_county_region.to_csv('/app/processed_files/county_region.csv', index=False)

    return df_temporal, df_illness, df_healthcare, df_historics, df_county_region

@task
def open_sql_connection():
    # Connect to PostgreSQL databases (2 methods shown)
    engine = create_engine('postgresql://fluuser:flupass@postgres/flu_database')
    conn = psycopg2.connect(
        dbname='flu_database',
        user='fluuser',
        password='flupass',
        host='postgres',
        port='5432'
    )

    print(" Connected to PostgreSQL database")

    # Create a cursor object
    cur = conn.cursor()

    return conn, engine, cur

@task
def create_sql_tables():

    # Create Schema
    cur.execute("CREATE SCHEMA IF NOT EXISTS flu_schema;")

    # Create Table 1 (County/Region Reference)
    cur.execute("DROP TABLE IF EXISTS flu_schema.county_region;")
    cur.execute("""CREATE TABLE county_region (
                    county_id INT PRIMARY KEY,
                    county_name TEXT,
                    ach_region TEXT,
                    population_density_202 FLOAT
                    );"""
                )
    print(" Table 1 created")

    # Create Table 2 (Temporal Reference)
    cur.execute("DROP TABLE IF EXISTS flu_schema.temporal;")
    cur.execute("""CREATE TABLE temporal (
                    epiweek_id INT PRIMARY KEY,
                    week_start DATE,
                    week_end DATE,
                    season TEXT
                    );"""
                )
    print(" Table 2 created")


    # Create Table 3 (County/Weekly Illness Comparison)
    cur.execute("DROP TABLE IF EXISTS flu_schema.illness;")
    cur.execute("""CREATE TABLE illness (
                    epiweek_id INT,
                    FOREIGN KEY (epiweek_id) REFERENCES temporal(epiweek_id),
                    county_id INT,
                    FOREIGN KEY (county_id) REFERENCES county_region(county_id),
                    respiratory_illness_type TEXT,
                    care_type TEXT,
                    PRIMARY KEY (epiweek_id, county_id, respiratory_illness_type, care_type),
                    county_ili_percent FLOAT,
                    state_ili_percent FLOAT,
                    deviation_from_state_average FLOAT
                    );"""
                )
    print(" Table 3 created")


    # Create Table 4 (Healthcare Utilization)
    cur.execute("DROP TABLE IF EXISTS flu_schema.healthcare;")
    cur.execute("""CREATE TABLE healthcare (
                    county_id INT PRIMARY KEY,
                    population_density_2020 FLOAT,
                    hospitalization_percent FLOAT,
                    er_visit_percent FLOAT,
                    hospital_to_er_ratio FLOAT,
                    FOREIGN KEY (county_id) REFERENCES county_region(county_id)
                    );"""
                )
    print(" Table 4 created")


    # Create Table 5 (Historical Flue Season Summary)
    cur.execute("DROP TABLE IF EXISTS flu_schema.historics;")
    cur.execute("""CREATE TABLE historics (
                    year INT PRIMARY KEY,
                    decade_year INT,
                    peak_week_id INT,
                    peak_ili_percent FLOAT,
                    average_wili_percent FLOAT,
                    peak_vs_avg_diff FLOAT
                    );"""
                )
    print(" Table 5 created")

    print(" All Tables created in PostgreSQL database")

@task
def ingest_static_sql_data():

    # Ingest region_county data (only done once since it should not change)
    with open(county_region_path, 'r') as f:
        sql = """
            COPY county_region
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

    print(" Table 1 data ingested")

@task
def ingest_dynamic_sql_data():


    # Temporal Ingest
    with open(temporal_path, 'r') as f:
        sql = """
            COPY temporal
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

    print(" Table 2 data ingested")

    # Illness Ingest
    with open(illness_path, 'r') as f:
        sql = """
            COPY illness
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

    print(" Table 3 data ingested")

    # Healthcare Ingest
    with open(healthcare_path, 'r') as f:
        sql = """
            COPY healthcare
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

    print(" Table 4 data ingested")

    # Historics Ingest
    with open(historic_path, 'r') as f:
        sql = """
            COPY historics
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
        """
        cur.copy_expert(sql, f)

    print(" Table 5 data ingested")

@task
def close_sql_connection():
    # Commit changes and close connection
    conn.commit()
    cur.close()
    conn.close()
    print(" PostgreSQL connection closed")


# %% [markdown]
# # Define DAG

# %%
with (DAG('setup_flu_data', default_args=default_args, schedule_interval='@daily') as setup_dag):
    # Define DAG tasks and dependencies
    rhino = collect_rhino_data()
    census = collect_census_data()
    fluview = collect_fluview_data()

    create_df = create_dataframe_tables(census, rhino, fluview)
    open_conn = open_sql_connection()
    create_tables = create_sql_tables()
    ingest_static = ingest_static_sql_data()
    ingest_dynamic = ingest_dynamic_sql_data()
    close_conn = close_sql_connection()


    end_task = DummyOperator(
        task_id='end',
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    [rhino, census, fluview] >> create_df >> open_conn >> create_tables >> ingest_static >> ingest_dynamic >> close_conn >> end_task


#with (DAG('add_flu_data', default_args=default_args, schedule_interval='@monthly') as recurring_dag):



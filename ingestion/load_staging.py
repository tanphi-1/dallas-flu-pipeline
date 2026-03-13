import os, pandas as pd, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ['SUPABASE_DB_URL']

DCHHS_COLS = [
    'report_week_end_date','mmwr_week','flu_season',
    'total_tests_performed','total_positive_tests','pct_positive',
    'flu_a_count','flu_b_count','flu_hospitalizations',
    'icu_admissions','pediatric_deaths',
    'school_absenteeism_pct','school_ili_pct','source_pdf_filename'
]

DSHS_COLS = [
    'report_week_end_date','mmwr_week','flu_season',
    'ili_pct','ili_baseline_pct','above_baseline','providers_reporting',
    'age_0_4_ili','age_5_24_ili','age_25_49_ili','age_50_64_ili','age_65_plus_ili',
    'total_ili_cases','total_patient_visits',
    'flu_a_count','flu_b_count','h1n1_count','h3n2_count','source_pdf_filename'
]

def upsert(conn, table, cols, df):
    df = df[[c for c in cols if c in df.columns]].where(pd.notna(df), None)
    rows = [tuple(row[c] if c in row else None for c in cols) for _, row in df.iterrows()]
    sql  = f'''
        INSERT INTO {table} ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (report_week_end_date) DO UPDATE SET
        {', '.join(f"{c} = EXCLUDED.{c}" for c in cols if c != 'report_week_end_date')}
    '''
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    print(f'  Loaded {len(rows)} rows into {table}')

def main():
    conn = psycopg2.connect(DB_URL)
    try:
        dchhs = pd.read_csv('data/processed/dchhs_weekly.csv')
        dchhs['report_week_end_date'] = pd.to_datetime(dchhs['report_week_end_date']).dt.date
        upsert(conn, 'staging.stg_dchhs_weekly', DCHHS_COLS, dchhs)

        dshs  = pd.read_csv('data/processed/dshs_weekly.csv')
        dshs['report_week_end_date'] = pd.to_datetime(dshs['report_week_end_date']).dt.date
        upsert(conn, 'staging.stg_dshs_weekly',  DSHS_COLS,  dshs)
    finally:
        conn.close()

if __name__ == '__main__': main()

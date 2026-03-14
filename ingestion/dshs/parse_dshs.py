import os, io, re, sys, json
import pdfplumber, pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
BUCKET   = 'flu-pdfs-dshs'
OUTPUT   = 'data/processed/dshs_weekly.csv'
LOG      = 'data/processed/dshs_parse_errors.json'

def safe_float(val):
    try: return float(str(val).replace('%','').replace(',','').strip())
    except: return None

def safe_int(val):
    try: return int(str(val).replace(',','').strip())
    except: return None

def derive_season(date):
    return f'{date.year}-{date.year+1}' if date.month >= 10 else f'{date.year-1}-{date.year}'

def mmwr_week_to_date(year, week):
    '''Compute the Saturday (week-ending date) for a given MMWR week.
    MMWR Week 1 starts on the first Sunday on or before January 4.'''
    from datetime import date, timedelta
    jan4 = date(year, 1, 4)
    # Sunday = 6 in isoweekday(), but we need day-of-week where Sunday=0
    dow = jan4.isoweekday() % 7  # 0=Sun, 1=Mon, ..., 6=Sat
    week1_start = jan4 - timedelta(days=dow)  # Sunday of MMWR week 1
    week_start = week1_start + timedelta(weeks=week - 1)
    return week_start + timedelta(days=6)  # Saturday = week ending date

def debug_pdf(filename):
    pdf_bytes = supabase.storage.from_(BUCKET).download(filename)
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        flat_idx = 0
        for i, page in enumerate(pdf.pages):
            print(f'\n=== PAGE {i+1} ===')
            tables = page.extract_tables()
            if not tables:
                print('  (no tables)')
            for j, tbl in enumerate(tables):
                num_cols = max(len(r) for r in tbl) if tbl else 0
                print(f'\n  -- tables[{flat_idx}]  (page {i+1}, table {j+1})  '
                      f'rows={len(tbl)}, cols={num_cols} --')
                for r_idx, row in enumerate(tbl):
                    cells = [f'[{c_idx}]={repr(cell)}' for c_idx, cell in enumerate(row)]
                    print(f'    row[{r_idx}]: {", ".join(cells)}')
                flat_idx += 1
        print(f'\n  Total tables (flat): {flat_idx}')

def extract_record(filename, pdf_bytes):
    errors = []
    rec    = {'source_pdf_filename': filename}
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text   = '\n'.join(p.extract_text() or '' for p in pdf.pages)
        tables = []
        for page in pdf.pages: tables.extend(page.extract_tables())

        # Week ending date from filename (reliable fallback)
        # filename format: 2025_week01_dshs.pdf
        fn_match = re.search(r'(\d{4})_week(\d{2})', filename)
        if fn_match:
            # Use MMWR week + year to derive approximate date
            rec['mmwr_week'] = int(fn_match.group(2))

        # Table 4 — ILINet summary (ILI% + baseline printed explicitly)
        # Confirmed: Table 4 contains 'Texas ILINet baseline' row
        try:
            t4 = tables[3]   # adjust index
            rec['providers_reporting'] = safe_int(t4[1][1])
            rec['ili_pct']             = safe_float(t4[4][1])   # % ILI
            rec['ili_baseline_pct']    = safe_float(t4[5][1])   # baseline
            if rec['ili_pct'] and rec['ili_baseline_pct']:
                rec['above_baseline'] = rec['ili_pct'] > rec['ili_baseline_pct']
        except (IndexError, TypeError) as e:
            errors.append(f'Table4 ILINet error: {e}')

        # Table 5 — Age group breakdown (CONFIRMED in DSHS PDFs)
        # Columns: Week | 0-4 | 5-24 | 25-49 | 50-64 | 65+ | Total | Patients | %ILI
        # Each row = one MMWR week. Last row = current week.
        try:
            t5 = tables[4]   # adjust index
            # Last data row is the current week
            last_row = t5[-1]
            rec['age_0_4_ili']         = safe_int(last_row[1])
            rec['age_5_24_ili']        = safe_int(last_row[2])
            rec['age_25_49_ili']       = safe_int(last_row[3])
            rec['age_50_64_ili']       = safe_int(last_row[4])
            rec['age_65_plus_ili']     = safe_int(last_row[5])
            rec['total_ili_cases']     = safe_int(last_row[6])
            rec['total_patient_visits']= safe_int(last_row[7])
        except (IndexError, TypeError) as e:
            errors.append(f'Table5 age group error: {e}')

        # Table 2 — Lab data (Flu A, Flu B, H1N1, H3N2)
        try:
            t2 = tables[1]   # adjust index
            rec['flu_a_count']  = safe_int(t2[1][1])
            rec['flu_b_count']  = safe_int(t2[5][1])
            rec['h1n1_count']   = safe_int(t2[3][1])
            rec['h3n2_count']   = safe_int(t2[4][1])
        except (IndexError, TypeError) as e:
            errors.append(f'Table2 lab error: {e}')

        # Try to extract report_week_end_date from text
        m = re.search(r'[Ww]eek [Ee]nding[:\s]+([\d/]+)', text)
        if m:
            try:
                rec['report_week_end_date'] = datetime.strptime(
                    m.group(1).strip(), '%m/%d/%Y').date()
            except:
                errors.append(f'Date parse failed: {m.group(1)}')

        # Fallback: compute date from MMWR week + year in filename
        if not rec.get('report_week_end_date') and fn_match:
            year = int(fn_match.group(1))
            week = int(fn_match.group(2))
            rec['report_week_end_date'] = mmwr_week_to_date(year, week)
            errors.append(f'Used MMWR fallback date: {rec["report_week_end_date"]}')

    if rec.get('report_week_end_date'):
        rec['flu_season'] = derive_season(rec['report_week_end_date'])
    else:
        errors.append('SKIPPED: no report_week_end_date could be determined')
    return rec, errors

def main(debug_file=None):
    if debug_file:
        debug_pdf(debug_file); return
    pdfs    = [f['name'] for f in supabase.storage.from_(BUCKET).list()]
    records, all_errors = [], {}
    for filename in sorted(pdfs):
        print(f'Parsing: {filename}')
        pdf_bytes = supabase.storage.from_(BUCKET).download(filename)
        rec, errors = extract_record(filename, pdf_bytes)
        if errors: all_errors[filename] = errors
        if not rec.get('report_week_end_date'):
            print(f'  SKIPPED (no date): {filename}')
            continue
        records.append(rec)
    os.makedirs('data/processed', exist_ok=True)
    pd.DataFrame(records).to_csv(OUTPUT, index=False)
    json.dump(all_errors, open(LOG,'w'), indent=2, default=str)
    print(f'Saved {len(records)} rows.')

if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--debug':
        main(debug_file=sys.argv[2])
    else: main()

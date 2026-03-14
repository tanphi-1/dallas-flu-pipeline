import os, io, re, sys, json
import pdfplumber, pandas as pd
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
BUCKET   = 'flu-pdfs-dshs'
OUTPUT   = 'data/processed/dshs_weekly.csv'
LOG      = 'data/processed/dshs_parse_errors.json'

def safe_float(val):
    if val is None: return None
    try: return float(str(val).replace('%','').replace(',','').strip())
    except: return None

def safe_int(val):
    if val is None: return None
    try: return int(float(str(val).replace(',','').strip()))
    except: return None

def extract_number(s):
    '''Extract leading integer from "1607 (69.18%)" → 1607.'''
    if s is None: return None
    m = re.match(r'([\d,]+)', str(s).strip())
    return safe_int(m.group(1)) if m else None

def derive_season(d):
    return f'{d.year}-{d.year+1}' if d.month >= 10 else f'{d.year-1}-{d.year}'

def mmwr_week_to_date(year, week):
    '''Compute the Saturday (week-ending date) for a given MMWR week.'''
    jan4 = date(year, 1, 4)
    dow = jan4.isoweekday() % 7  # 0=Sun, ..., 6=Sat
    week1_start = jan4 - timedelta(days=dow)
    week_start = week1_start + timedelta(weeks=week - 1)
    return week_start + timedelta(days=6)

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

def find_table(tables, marker):
    '''Find table containing marker text in any cell.'''
    for tbl in tables:
        for row in tbl:
            for cell in row:
                if cell and marker.lower() in str(cell).lower():
                    return tbl
    return None

def extract_record(filename, pdf_bytes):
    errors = []
    rec    = {'source_pdf_filename': filename}
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text   = '\n'.join(p.extract_text() or '' for p in pdf.pages)
        tables = []
        for page in pdf.pages: tables.extend(page.extract_tables())

        # MMWR week + year from filename (e.g. 2024_week03_dshs.pdf)
        fn_match = re.search(r'(\d{4})_week(\d{2})', filename)
        if fn_match:
            rec['mmwr_week'] = int(fn_match.group(2))

        # --- Flu type table: find by "Influenza A" in first cell ---
        flu_tbl = None
        for tbl in tables:
            if tbl and tbl[0] and tbl[0][0] and 'influenza a' == str(tbl[0][0]).strip().lower():
                flu_tbl = tbl
                break
        if flu_tbl:
            try:
                for row in flu_tbl:
                    label = str(row[0] or '').strip().lower()
                    if label == 'influenza a':
                        rec['flu_a_count'] = extract_number(row[1])
                    elif label == 'influenza b':
                        rec['flu_b_count'] = extract_number(row[1])
                    elif 'h1n1' in label:
                        rec['h1n1_count'] = extract_number(row[1])
                    elif 'h3n2' in label:
                        rec['h3n2_count'] = extract_number(row[1])
            except Exception as e:
                errors.append(f'Flu type table error: {e}')
        else:
            errors.append('Flu type table not found')

        # --- ILINet baseline table: find by "ILINet baseline" ---
        ili_tbl = find_table(tables, 'ILINet baseline')
        if ili_tbl:
            try:
                for row in ili_tbl:
                    label = str(row[0] or '').lower()
                    if 'baseline' in label:
                        # Baseline value could be in any non-None cell after label
                        for cell in row[1:]:
                            v = safe_float(cell)
                            if v is not None:
                                rec['ili_baseline_pct'] = v
                                break
                    elif 'percentage' in label and 'ili' in label:
                        for cell in row[1:]:
                            v = safe_float(cell)
                            if v is not None:
                                rec['ili_pct'] = v
                                break
                    elif 'providers reporting' == label.strip().rstrip('.') or \
                         'number of providers reporting' == label.strip():
                        for cell in row[1:]:
                            v = safe_int(cell)
                            if v is not None:
                                rec['providers_reporting'] = v
                                break
            except Exception as e:
                errors.append(f'ILINet table error: {e}')
        else:
            errors.append('ILINet baseline table not found')

        # --- Age group table: find by "Number of ILI Cases" or "Total ILI" ---
        age_tbl = find_table(tables, 'Total ILI')
        if not age_tbl:
            age_tbl = find_table(tables, 'Number of ILI Cases')
        if age_tbl:
            try:
                # Last data row = current week
                last_row = age_tbl[-1]
                # Find column indices from header rows
                # Standard layout: Week|Providers|0-4|5-24|25-49|50-64|65+|TotalILI|TotalPatients|%ILI
                # But merged cells create gaps. Use the known column pattern.
                # Collect all non-None values from last row
                vals = [cell for cell in last_row]
                # Try known column indices first (from debug: 1,4,7,9,12,15,18,21,24)
                if len(vals) >= 25:
                    rec['providers_reporting'] = rec.get('providers_reporting') or safe_int(vals[1])
                    rec['age_0_4_ili']          = safe_int(vals[4])
                    rec['age_5_24_ili']         = safe_int(vals[7])
                    rec['age_25_49_ili']        = safe_int(vals[9])
                    rec['age_50_64_ili']        = safe_int(vals[12])
                    rec['age_65_plus_ili']      = safe_int(vals[15])
                    rec['total_ili_cases']      = safe_int(vals[18])
                    rec['total_patient_visits'] = safe_int(vals[21])
                    ili_from_table = safe_float(vals[24])
                    if ili_from_table is not None:
                        rec['ili_pct'] = rec.get('ili_pct') or ili_from_table
                else:
                    # Fallback: collect non-None numeric values
                    nums = [cell for cell in vals if cell is not None and str(cell).strip()]
                    errors.append(f'Age table has {len(vals)} cols, expected 25+. Got {len(nums)} values.')
            except Exception as e:
                errors.append(f'Age group table error: {e}')
        else:
            errors.append('Age group table not found')

        # Compute above_baseline
        if rec.get('ili_pct') is not None and rec.get('ili_baseline_pct') is not None:
            rec['above_baseline'] = rec['ili_pct'] > rec['ili_baseline_pct']

        # --- Date extraction ---
        # Primary: compute from MMWR week + year in filename (most reliable)
        if fn_match:
            year = int(fn_match.group(1))
            week = int(fn_match.group(2))
            rec['report_week_end_date'] = mmwr_week_to_date(year, week)
        else:
            # Fallback: regex on text
            m = re.search(r'[Ww]eek [Ee]nding[:\s]+([\d/]+)', text)
            if m:
                try:
                    rec['report_week_end_date'] = datetime.strptime(
                        m.group(1).strip(), '%m/%d/%Y').date()
                except:
                    pass
            if not rec.get('report_week_end_date'):
                m = re.search(r'[Ww]eek [Ee]nding:?\s*\n?\s*(\w+ \d{1,2},?\s*\d{4})', text)
                if m:
                    ds = m.group(1).replace(',', '')
                    try: rec['report_week_end_date'] = datetime.strptime(ds.strip(), '%B %d %Y').date()
                    except: pass

    if rec.get('report_week_end_date'):
        rec['flu_season'] = derive_season(rec['report_week_end_date'])
    else:
        errors.append('SKIPPED: no report_week_end_date could be determined')
    return rec, errors

TARGET_SEASONS = {'2023-2024', '2024-2025'}

def main(debug_file=None):
    if debug_file:
        debug_pdf(debug_file); return
    pdfs    = [f['name'] for f in supabase.storage.from_(BUCKET).list()]
    records, all_errors, skipped_season = [], {}, []
    for filename in sorted(pdfs):
        print(f'Parsing: {filename}')
        pdf_bytes = supabase.storage.from_(BUCKET).download(filename)
        rec, errors = extract_record(filename, pdf_bytes)
        if errors: all_errors[filename] = errors
        if not rec.get('report_week_end_date'):
            print(f'  SKIPPED (no date): {filename}')
            continue
        if rec.get('flu_season') not in TARGET_SEASONS:
            print(f'  SKIPPED (season {rec.get("flu_season")}): {filename}')
            skipped_season.append({'filename': filename, 'flu_season': rec.get('flu_season'),
                                   'date': str(rec.get('report_week_end_date'))})
            continue
        records.append(rec)
    os.makedirs('data/processed', exist_ok=True)
    pd.DataFrame(records).to_csv(OUTPUT, index=False)
    all_errors['_skipped_by_season'] = skipped_season
    json.dump(all_errors, open(LOG,'w'), indent=2, default=str)
    print(f'Saved {len(records)} rows. Skipped {len(skipped_season)} (out-of-season). Errors logged to {LOG}')

if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--debug':
        main(debug_file=sys.argv[2])
    else: main()

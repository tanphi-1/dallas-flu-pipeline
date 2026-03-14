import os, io, re, sys, json
import pdfplumber, pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
BUCKET   = 'flu-pdfs-dchhs'
OUTPUT   = 'data/processed/dchhs_weekly.csv'
LOG      = 'data/processed/dchhs_parse_errors.json'

def safe_float(val):
    if val is None: return None
    try: return float(str(val).replace('%','').replace(',','').strip())
    except: return None

def safe_int(val):
    if val is None: return None
    try: return int(float(str(val).replace(',','').strip()))
    except: return None

def derive_season(date):
    return f'{date.year}-{date.year+1}' if date.month >= 10 else f'{date.year-1}-{date.year}'

def debug_pdf(filename):
    '''Print all tables with flat indices, row indices, and column indices.'''
    pdf_bytes = supabase.storage.from_(BUCKET).download(filename)
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        flat_idx = 0
        for i, page in enumerate(pdf.pages):
            print(f'\n=== PAGE {i+1} ===')
            print(f'\n  -- TEXT --')
            print(page.extract_text()[:500] if page.extract_text() else '(no text)')
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

def row_label(row):
    '''Get the label text from a row, checking col[0] then col[1].'''
    for cell in row[:3]:  # check first 3 cells
        if cell and str(cell).strip():
            return str(cell).strip().lower()
    return ''

def get_current_week_val(row, is_pct=False):
    '''Extract current week value from a row with merged-cell gaps.
    For count rows (is_pct=False): season total exists; current week is second-to-last numeric value.
    For percent rows (is_pct=True): no season total; current week is the last numeric value.'''
    # Collect only numeric-looking values (skip labels, empty strings)
    nums = []
    for i, cell in enumerate(row):
        if cell is None:
            continue
        s = str(cell).strip().replace('*', '')
        if not s:
            continue
        clean = s.replace(',', '').replace('%', '')
        if re.match(r'^-?\d+\.?\d*$', clean):
            nums.append(s)
    if not nums:
        return None
    if is_pct or len(nums) == 1:
        return nums[-1]       # last numeric = current week
    return nums[-2] if len(nums) >= 2 else nums[-1]  # second-to-last = current week

def parse_date(text):
    '''Extract report week ending date from PDF text.'''
    # Try: "Week N ending January 13, 2024" or "Week Ending 1/13/2024"
    # Pattern 1: Month DD, YYYY
    m = re.search(r'ending[:\s]+(\w+ \d{1,2},?\s*\d{4})', text, re.IGNORECASE)
    if m:
        ds = m.group(1).replace(',', '')
        for fmt in ('%B %d %Y', '%b %d %Y'):
            try: return datetime.strptime(ds.strip(), fmt).date()
            except: pass
    # Pattern 2: MM/DD/YYYY
    m = re.search(r'ending[:\s]+([\d]{1,2}/[\d]{1,2}/[\d]{4})', text, re.IGNORECASE)
    if m:
        try: return datetime.strptime(m.group(1).strip(), '%m/%d/%Y').date()
        except: pass
    # Pattern 3: from filename — "Week-N-Ending-M.D.YYYY" or "Week Ending M-D-YYYY"
    m = re.search(r'Ending[_-]?(\d{1,2})[.\-](\d{1,2})[.\-](\d{4})', text, re.IGNORECASE)
    if m:
        try: return datetime.strptime(f'{m.group(1)}/{m.group(2)}/{m.group(3)}', '%m/%d/%Y').date()
        except: pass
    return None

def extract_record(filename, pdf_bytes):
    errors = []
    rec    = {'source_pdf_filename': filename}
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text   = '\n'.join(p.extract_text() or '' for p in pdf.pages)
        tables = []
        for page in pdf.pages: tables.extend(page.extract_tables())

        # Week ending date — try PDF text first, then filename
        rec['report_week_end_date'] = parse_date(text)
        if not rec['report_week_end_date']:
            rec['report_week_end_date'] = parse_date(filename)
        if not rec['report_week_end_date']:
            errors.append('Week ending date not found')

        # MMWR week — prefer "Week N ending" (current week) over "CDC Week" (table header)
        m2 = re.search(r'[Ww]eek\s+(\d+)\s+ending', text, re.IGNORECASE)
        if not m2:
            m2 = re.search(r'[Ww]eek[- ](\d+)[- ]Ending', filename, re.IGNORECASE)
        rec['mmwr_week'] = int(m2.group(1)) if m2 else None

        # Lab data — find by "Total Influenza Tests Performed"
        lab = find_table(tables, 'Total Influenza Tests Performed')
        if lab:
            try:
                for row in lab:
                    label = row_label(row)
                    if 'total influenza tests performed' in label:
                        rec['total_tests_performed'] = safe_int(get_current_week_val(row))
                    elif 'total positive influenza' in label:
                        rec['total_positive_tests'] = safe_int(get_current_week_val(row))
                    elif 'percent positive influenza' in label:
                        rec['pct_positive'] = safe_float(get_current_week_val(row, is_pct=True))
                    elif 'positive influenza a' in label:
                        rec['flu_a_count'] = safe_int(get_current_week_val(row))
                    elif 'positive influenza b' in label:
                        rec['flu_b_count'] = safe_int(get_current_week_val(row))
            except Exception as e:
                errors.append(f'Lab table parse error: {e}')
        else:
            errors.append('Lab data table not found')

        # Hospitalization data — find by "Influenza hospitalizations"
        hosp = find_table(tables, 'Influenza hospitalizations')
        if hosp:
            try:
                for row in hosp:
                    label = row_label(row)
                    if 'influenza hospitalizations' in label:
                        rec['flu_hospitalizations'] = safe_int(get_current_week_val(row))
                    elif 'icu admissions' in label:
                        rec['icu_admissions'] = safe_int(get_current_week_val(row))
                    elif 'pediatric deaths' in label:
                        rec['pediatric_deaths'] = safe_int(get_current_week_val(row))
            except Exception as e:
                errors.append(f'Hospitalization table parse error: {e}')
        else:
            errors.append('Hospitalization table not found')

        # Narrative text — School absenteeism
        absent = re.search(
            r'school absenteeism rate was ([\d.]+)%.*?([\d.]+)%.*?influenza',
            text, re.IGNORECASE | re.DOTALL)
        if absent:
            rec['school_absenteeism_pct'] = safe_float(absent.group(1))
            rec['school_ili_pct']         = safe_float(absent.group(2))

    if rec.get('report_week_end_date'):
        rec['flu_season'] = derive_season(rec['report_week_end_date'])
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

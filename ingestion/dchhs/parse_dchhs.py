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
    try: return float(str(val).replace('%','').replace(',','').strip())
    except: return None

def safe_int(val):
    try: return int(str(val).replace(',','').strip())
    except: return None

def derive_season(date):
    return f'{date.year}-{date.year+1}' if date.month >= 10 else f'{date.year-1}-{date.year}'

def debug_pdf(filename):
    '''Print all table contents — run this first on several PDFs.'''
    pdf_bytes = supabase.storage.from_(BUCKET).download(filename)
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            print(f'\n=== PAGE {i+1} ===')
            for j, tbl in enumerate(page.extract_tables()):
                print(f'  -- Table {j+1} --')
                for row in tbl: print('   ', row)

def extract_record(filename, pdf_bytes):
    errors = []
    rec    = {'source_pdf_filename': filename}
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text   = '\n'.join(p.extract_text() or '' for p in pdf.pages)
        tables = []
        for page in pdf.pages: tables.extend(page.extract_tables())

        # Week ending date
        m = re.search(r'[Ww]eek [Ee]nding[:\s]+([\d/]+)', text)
        if m:
            try:
                rec['report_week_end_date'] = datetime.strptime(
                    m.group(1).strip(), '%m/%d/%Y').date()
            except: errors.append(f'Date parse failed: {m.group(1)}')
        else: errors.append('Week ending date not found')

        # MMWR week
        m2 = re.search(r'CDC [Ww]eek\s+(\d+)', text)
        rec['mmwr_week'] = int(m2.group(1)) if m2 else None

        # Table 1 — Lab data
        # IMPORTANT: adjust table index and row numbers after running debug_pdf()
        try:
            t1 = tables[0]
            rec['total_tests_performed'] = safe_int(t1[8][6])   # adjust indices
            rec['total_positive_tests']  = safe_int(t1[9][6])
            rec['pct_positive']          = safe_float(t1[10][6])
            rec['flu_a_count']           = safe_int(t1[11][6])
            rec['flu_b_count']           = safe_int(t1[12][6])
        except (IndexError, TypeError) as e:
            errors.append(f'Table1 parse error: {e}')

        # Table 2 — Hospitalizations
        try:
            t2 = tables[1]
            rec['flu_hospitalizations'] = safe_int(t2[1][6])   # adjust indices
            rec['icu_admissions']       = safe_int(t2[2][6])
            rec['pediatric_deaths']     = safe_int(t2[3][6])
        except (IndexError, TypeError) as e:
            errors.append(f'Table2 parse error: {e}')

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
        records.append(rec)
    os.makedirs('data/processed', exist_ok=True)
    pd.DataFrame(records).to_csv(OUTPUT, index=False)
    json.dump(all_errors, open(LOG,'w'), indent=2, default=str)
    print(f'Saved {len(records)} rows. Errors logged to {LOG}')

if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--debug':
        main(debug_file=sys.argv[2])
    else: main()

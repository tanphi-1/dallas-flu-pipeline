import os, requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
BUCKET   = 'flu-pdfs-dshs'
BASE     = 'https://www.dshs.texas.gov/sites/default/files/IDCU/disease/respiratory_virus_surveillance'

# DSHS URL pattern: /YEAR/YEAR-weekNN-trvsreport.pdf
# Season 2023-2024: weeks 40-52 in 2023, weeks 1-20 in 2024
# Season 2024-2025: weeks 40-52 in 2024, weeks 1-20 in 2025
WEEKS_TO_FETCH = [
    # 2023-2024 season
    *[(2023, w) for w in range(40, 53)],   # Oct-Dec 2023
    *[(2024, w) for w in range(1,  21)],   # Jan-May 2024
    # 2024-2025 season
    *[(2024, w) for w in range(40, 53)],   # Oct-Dec 2024
    *[(2025, w) for w in range(1,  21)],   # Jan-May 2025
]

def list_existing():
    try: return {f['name'] for f in supabase.storage.from_(BUCKET).list()}
    except: return set()

def build_url(year, week):
    return f'{BASE}/{year}/{year}-week{week:02d}-trvsreport.pdf'

def main():
    existing = list_existing()
    session  = requests.Session()
    uploaded, missing = 0, []
    for year, week in WEEKS_TO_FETCH:
        filename = f'{year}_week{week:02d}_dshs.pdf'
        if filename in existing:
            print(f'  Skip: {filename}'); continue
        url  = build_url(year, week)
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            missing.append(filename); continue
        supabase.storage.from_(BUCKET).upload(
            path=filename, file=resp.content,
            file_options={'content-type':'application/pdf','upsert':'true'})
        print(f'  Uploaded: {filename}')
        uploaded += 1
    print(f'Done. Uploaded: {uploaded}. Missing/404: {len(missing)}')
    if missing: print('Missing:', missing)

if __name__ == '__main__': main()

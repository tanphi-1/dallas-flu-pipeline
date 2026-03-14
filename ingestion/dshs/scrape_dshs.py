import os, re, subprocess
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
BUCKET   = 'flu-pdfs-dshs'
BASE     = 'https://www.dshs.texas.gov'
INDEX    = BASE + '/texas-respiratory-virus-surveillance-report'

# We only want PDFs from the 2024 and 2025 folders (covers both seasons)
YEAR_FOLDERS = ['2024', '2025']

def curl_get(url):
    r = subprocess.run(['curl', '-sL', url], capture_output=True, timeout=60)
    r.check_returncode()
    return r.stdout

def list_existing():
    try: return {f['name'] for f in supabase.storage.from_(BUCKET).list()}
    except: return set()

def extract_week(path):
    '''Extract MMWR week number from varied DSHS filename patterns.'''
    basename = path.split('/')[-1].lower()
    # Pattern: 2024-week03-trvsreport.pdf or 2025-week-30-trvsreport.pdf
    m = re.search(r'(\d{4})-?week-?(\d{1,2})', basename)
    if m: return int(m.group(1)), int(m.group(2))
    # Pattern: 2024Week02TRVS-Final-pubJan19.pdf
    m = re.search(r'(\d{4})week(\d{1,2})trvs', basename)
    if m: return int(m.group(1)), int(m.group(2))
    # Pattern: 2025.week38.trvsreport...pdf
    m = re.search(r'(\d{4})\.week(\d{1,2})\.', basename)
    if m: return int(m.group(1)), int(m.group(2))
    # Pattern: 2025-wee43-trvsreport.pdf (typo on DSHS site)
    m = re.search(r'(\d{4})-wee(\d{1,2})-', basename)
    if m: return int(m.group(1)), int(m.group(2))
    return None, None

def get_pdf_links():
    html = curl_get(INDEX).decode('utf-8', errors='replace')
    pattern = re.compile(
        r'/sites/default/files/IDCU/disease/respiratory_virus_surveillance/'
        r'(\d{4})/[^"]+\.pdf', re.IGNORECASE)
    seen, results = set(), []
    for m in re.finditer(pattern, html):
        path = m.group(0)
        year_folder = m.group(1)
        if year_folder not in YEAR_FOLDERS or path in seen:
            continue
        # Skip FAQ documents
        if 'faq' in path.lower():
            continue
        seen.add(path)
        year, week = extract_week(path)
        if year and week:
            results.append((year, week, BASE + path))
        else:
            print(f'  WARN: could not parse week from: {path}')
    return results

def main():
    existing = list_existing()
    links = get_pdf_links()
    print(f'Found {len(links)} DSHS PDF links')
    uploaded = 0
    for year, week, url in sorted(links):
        filename = f'{year}_week{week:02d}_dshs.pdf'
        if filename in existing:
            print(f'  Skip: {filename}'); continue
        try:
            content = curl_get(url)
            supabase.storage.from_(BUCKET).upload(
                path=filename, file=content,
                file_options={'content-type':'application/pdf','upsert':'true'})
            print(f'  Uploaded: {filename}')
            uploaded += 1
        except Exception as e:
            print(f'  ERROR {filename}: {e}')
    print(f'Done. Uploaded: {uploaded}')

if __name__ == '__main__': main()

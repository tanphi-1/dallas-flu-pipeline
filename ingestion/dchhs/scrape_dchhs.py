import os, re, subprocess
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase  = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
BUCKET    = 'flu-pdfs-dchhs'
BASE_URL  = 'https://www.dallascounty.org'
INDEX_URL = BASE_URL + '/departments/dchhs/data-reports/influenza-surveillance.php'
# Season years to collect — scraper filters PDFs by year in URL
SEASONS   = ['2023', '2024', '2025']

def curl_get(url):
    r = subprocess.run(['curl', '-sL', url], capture_output=True, timeout=60)
    r.check_returncode()
    return r.stdout

def list_existing():
    try:
        return {f['name'] for f in supabase.storage.from_(BUCKET).list()}
    except:
        return set()

def get_pdf_links():
    html    = curl_get(INDEX_URL).decode('utf-8', errors='replace')
    pattern = re.compile(
        r'/Assets/uploads/docs/hhs/influenza-surveillance/(\d{4})/[^"]+\.pdf',
        re.IGNORECASE)
    seen, results = set(), []
    for m in re.finditer(pattern, html):
        path = m.group(0)
        # Skip paths containing CMS metadata junk
        if '&quot;' in path or 'columnData' in path:
            continue
        year = m.group(1)
        if year in SEASONS and path not in seen:
            seen.add(path)
            results.append((year, BASE_URL + path))
    return results

def main():
    existing = list_existing()
    links    = get_pdf_links()
    print(f'Found {len(links)} DCHHS PDF links')
    uploaded = 0
    for year, url in links:
        filename = f"{year}_{url.split('/')[-1]}"
        if filename in existing:
            print(f'  Skip (exists): {filename}')
            continue
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

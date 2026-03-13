import os, re, requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase  = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
BUCKET    = 'flu-pdfs-dchhs'
BASE_URL  = 'https://www.dallascounty.org'
INDEX_URL = BASE_URL + '/departments/dchhs/data-reports/influenza-surveillance.php'
# Season years to collect — scraper filters PDFs by year in URL
SEASONS   = ['2023', '2024', '2025']

def list_existing():
    try:
        return {f['name'] for f in supabase.storage.from_(BUCKET).list()}
    except:
        return set()

def get_pdf_links(session):
    resp    = session.get(INDEX_URL, timeout=30)
    pattern = re.compile(
        r'/Assets/uploads/docs/hhs/influenza-surveillance/(\d{4})/[^"]+\.pdf',
        re.IGNORECASE)
    seen, results = set(), []
    for m in re.finditer(pattern, resp.text):
        year = m.group(1)
        if year in SEASONS and m.group(0) not in seen:
            seen.add(m.group(0))
            results.append((year, BASE_URL + m.group(0)))
    return results

def main():
    existing = list_existing()
    session  = requests.Session()
    links    = get_pdf_links(session)
    print(f'Found {len(links)} DCHHS PDF links')
    uploaded = 0
    for year, url in links:
        filename = f"{year}_{url.split('/')[-1]}"
        if filename in existing:
            print(f'  Skip (exists): {filename}')
            continue
        try:
            content = session.get(url, timeout=30).content
            supabase.storage.from_(BUCKET).upload(
                path=filename, file=content,
                file_options={'content-type':'application/pdf','upsert':'true'})
            print(f'  Uploaded: {filename}')
            uploaded += 1
        except Exception as e:
            print(f'  ERROR {filename}: {e}')
    print(f'Done. Uploaded: {uploaded}')

if __name__ == '__main__': main()

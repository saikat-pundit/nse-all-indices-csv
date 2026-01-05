import pandas as pd, requests, re, zipfile, os, json
from io import StringIO
url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTBuDewVgTDoc_zaWYQyaWKpBt0RwtFPhnBrpqr1v6Y5wfAmPpEYvTsaWd64bsHhH68iYNtLMSRpOQ0/pub?gid=1630572077&single=true&output=csv"
df = pd.read_csv(StringIO(requests.get(url).text))
primary_links = str(df.iloc[69, 9]) if pd.notna(df.iloc[69, 9]) else ""
secondary_links = str(df.iloc[70, 9]) if pd.notna(df.iloc[70, 9]) else ""

print(f"📦 PRIMARY links: {len(primary_links.split(';')) if primary_links else 0}")
print(f"📦 SECONDARY links: {len(secondary_links.split(';')) if secondary_links else 0}")

def get_filename(file_id):
    """Extract original filename from Google Drive"""
    try:
        meta_url = f"https://drive.google.com/file/d/{file_id}/view"
        resp = requests.get(meta_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        
        # Try JSON-LD
        json_ld = re.search(r'<script type="application/ld\+json">(.*?)</script>', resp.text, re.DOTALL)
        if json_ld:
            data = json.loads(json_ld.group(1))
            return data.get('name', f"file_{file_id}.jpg")
        
        # Try HTML title
        title = re.search(r'<title>(.*?) - Google Drive</title>', resp.text)
        if title:
            return title.group(1).strip()
            
    except:
        pass
    return f"file_{file_id}.jpg"

def create_zip(links_str, zip_name):
    """Create zip archive from Google Drive links"""
    if not links_str or links_str.lower() == 'nan':
        return False
    
    success = 0
    links = [l.strip() for l in links_str.split(';') if l.strip()]
    
    with zipfile.ZipFile(f"{zip_name}.zip", 'w') as zipf:
        for link in links:
            match = re.search(r'/d/([a-zA-Z0-9_-]+)', link)
            if not match:
                continue
                
            file_id = match.group(1)
            filename = get_filename(file_id)
            
            # Clean filename
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            if not filename.lower().endswith(('.jpg', '.jpeg', '.png')):
                filename += '.jpg'
            
            # Download and add to zip
            try:
                dl_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                session = requests.Session()
                response = session.get(dl_url, stream=True, timeout=30)
                
                # Handle large files
                if "confirm=" in response.url:
                    token = re.search(r'confirm=([0-9A-Za-z_]+)', response.url).group(1)
                    response = session.get(f"{dl_url}&confirm={token}", stream=True, timeout=30)
                
                zipf.writestr(filename, response.content)
                success += 1
                print(f"✓ {filename[:50]}..." if len(filename) > 50 else f"✓ {filename}")
                
            except Exception as e:
                print(f"✗ {filename[:30]}...")
                continue
    
    if success > 0:
        print(f"✅ {zip_name}.zip created ({success}/{len(links)} files)")
        return True
    return False

# Create zip files
if primary_links:
    print("\n🟦 Creating PRIMARY.zip...")
    create_zip(primary_links, "PRIMARY")

if secondary_links:
    print("\n🟨 Creating Secondary_Higher_Secondary.zip...")
    create_zip(secondary_links, "Secondary_Higher_Secondary")

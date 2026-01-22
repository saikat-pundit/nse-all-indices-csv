import pandas as pd
import requests
import re
import zipfile
import json
from io import BytesIO

# Read the CSV
df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTBuDewVgTDoc_zaWYQyaWKpBt0RwtFPhnBrpqr1v6Y5wfAmPpEYvTsaWd64bsHhH68iYNtLMSRpOQ0/pub?gid=979866094&single=true&output=csv")

# Filter to get only rows from Y2:Z12 (note: pandas is 0-indexed, so rows 1-11)
data = df.iloc[1:12][[df.columns[24], df.columns[25]]]  # Y is 25th column (index 24), Z is 26th column (index 25)
data.columns = ['ZIP_FILE_NAME', 'DRIVE_LINKS']

def get_filename(file_id):
    """Get original filename from Google Drive"""
    try:
        meta_url = f"https://drive.google.com/file/d/{file_id}/view"
        resp = requests.get(meta_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        
        # Try JSON-LD
        json_ld = re.search(r'<script type="application/ld\+json">(.*?)</script>', resp.text, re.DOTALL)
        if json_ld:
            data = json.loads(json_ld.group(1))
            name = data.get('name', f"file_{file_id}")
            return name
            
        # Try HTML title
        title = re.search(r'<title>(.*?) - Google Drive</title>', resp.text)
        if title:
            name = title.group(1).strip()
            return name
            
    except Exception as e:
        print(f"Warning: Could not get filename for {file_id}: {e}")
    
    return f"file_{file_id}"

def create_zip(zip_name, links_str):
    """Create zip file with files from Google Drive links"""
    if not links_str or str(links_str).lower() == 'nan':
        return False
    
    # Clean zip name for filename
    clean_name = re.sub(r'[<>:"/\\|?*]', '_', zip_name)
    zip_filename = f"{clean_name}.zip"
    
    links = [l.strip() for l in str(links_str).split(';') if l.strip()]
    print(f"\n📦 Processing: {zip_name}")
    print(f"   Found {len(links)} document(s)")
    
    success = 0
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for i, link in enumerate(links):
            # Extract file ID
            match = re.search(r'id=([a-zA-Z0-9_-]+)', link)
            if not match:
                match = re.search(r'/d/([a-zA-Z0-9_-]+)', link)
            
            if not match:
                print(f"   ✗ Could not extract file ID from: {link[:50]}...")
                continue
                
            file_id = match.group(1)
            original_filename = get_filename(file_id)
            
            # Keep original extension if present, otherwise guess from content
            if '.' not in original_filename:
                original_filename = f"{original_filename}.pdf"  # Default to pdf
            
            # Download file
            try:
                dl_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                session = requests.Session()
                response = session.get(dl_url, stream=True, timeout=30)
                
                # Handle large file confirmation
                if "confirm=" in response.url:
                    token = re.search(r'confirm=([0-9A-Za-z_]+)', response.url).group(1)
                    response = session.get(f"{dl_url}&confirm={token}", stream=True, timeout=30)
                
                # Read content and add to zip
                content = response.content
                zipf.writestr(original_filename, content)
                success += 1
                print(f"   ✓ {original_filename}")
                
            except Exception as e:
                print(f"   ✗ Error downloading {file_id}: {str(e)[:50]}")
                continue
    
    if success > 0:
        print(f"   ✅ Created: {zip_filename} ({success}/{len(links)} files)")
        return True
    return False

# Process each row
print(f"📊 Processing {len(data)} zip files...")
for idx, row in data.iterrows():
    zip_name = row['ZIP_FILE_NAME']
    drive_links = row['DRIVE_LINKS']
    create_zip(zip_name, drive_links)

print("\n🎉 All zip files processed!")

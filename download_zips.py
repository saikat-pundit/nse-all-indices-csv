import pandas as pd, requests, re, zipfile, os
from io import StringIO

# Download CSV
csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTBuDewVgTDoc_zaWYQyaWKpBt0RwtFPhnBrpqr1v6Y5wfAmPpEYvTsaWd64bsHhH68iYNtLMSRpOQ0/pub?gid=1630572077&single=true&output=csv"
df = pd.read_csv(StringIO(requests.get(csv_url).text))

# Get links from J71 and J72
primary = str(df.iloc[70, 9]) if pd.notna(df.iloc[70, 9]) else ""
secondary = str(df.iloc[71, 9]) if pd.notna(df.iloc[71, 9]) else ""

def get_info(url):
    """Get file ID and name from Google Drive URL"""
    match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
    if not match: return None, None
    fid = match.group(1)
    
    # Get filename
    try:
        html = requests.get(f"https://drive.google.com/file/d/{fid}/view").text
        name = re.search(r'"title":"([^"]+)"', html).group(1)
    except:
        name = f"file_{fid}.jpg"
    return fid, name

def save_zip(links_str, zip_name):
    """Create zip file"""
    if not links_str: return False
    
    with zipfile.ZipFile(f"{zip_name}.zip", 'w') as z:
        for link in links_str.split(';'):
            if not link.strip(): continue
            
            fid, fname = get_info(link.strip())
            if not fid: continue
            
            try:
                # Download
                dl_url = f"https://drive.google.com/uc?export=download&id={fid}"
                r = requests.get(dl_url, stream=True)
                
                # Handle large files
                if "confirm=" in r.url:
                    token = re.search(r'confirm=([0-9A-Za-z_]+)', r.url).group(1)
                    dl_url = f"{dl_url}&confirm={token}"
                    r = requests.get(dl_url, stream=True)
                
                z.writestr(fname, r.content)
                print(f"✓ {fname}")
            except:
                print(f"✗ {fname}")
                continue
    
    return os.path.exists(f"{zip_name}.zip")

# Create zips
if primary and save_zip(primary, "PRIMARY"):
    print("✓ PRIMARY.zip")

if secondary and save_zip(secondary, "Secondary_Higher_Secondary"):
    print("✓ Secondary_Higher_Secondary.zip")

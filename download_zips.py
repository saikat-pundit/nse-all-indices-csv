import pandas as pd, requests, re, zipfile
from io import StringIO

# Get CSV
df = pd.read_csv(StringIO(requests.get("https://docs.google.com/spreadsheets/d/e/2PACX-1vTBuDewVgTDoc_zaWYQyaWKpBt0RwtFPhnBrpqr1v6Y5wfAmPpEYvTsaWd64bsHhH68iYNtLMSRpOQ0/pub?gid=1630572077&single=true&output=csv").text))

# Extract exactly J71 (70,9) and J72 (71,9)
primary = str(df.iloc[70, 9]) if pd.notna(df.iloc[70, 9]) else ""
secondary = str(df.iloc[71, 9]) if pd.notna(df.iloc[71, 9]) else ""

# Create zip function
def make_zip(links, zip_name):
    if not links: return False
    
    with zipfile.ZipFile(f"{zip_name}.zip", 'w') as z:
        for link in links.split(';'):
            link = link.strip()
            if not link: continue
            
            # Extract file ID
            fid_match = re.search(r'/d/([a-zA-Z0-9_-]+)', link)
            if not fid_match: continue
            fid = fid_match.group(1)
            
            # Get original filename
            try:
                resp = requests.get(f"https://drive.google.com/file/d/{fid}/view")
                fname = re.search(r'"title":"([^"]+)"', resp.text).group(1)
            except:
                fname = f"file_{fid}.jpg"
            
            # Download and add to zip
            try:
                dl = f"https://drive.google.com/uc?export=download&id={fid}"
                r = requests.get(dl)
                
                # Handle large files
                if "confirm=" in r.url:
                    token = re.search(r'confirm=([0-9A-Za-z_]+)', r.url).group(1)
                    r = requests.get(f"{dl}&confirm={token}")
                
                z.writestr(fname, r.content)
                print(f"✓ {fname}")
            except:
                print(f"✗ {fname}")
    
    return True

# Create zips with exact cell references
if primary: 
    make_zip(primary, "PRIMARY")
    print("PRIMARY.zip created from J71")

if secondary: 
    make_zip(secondary, "Secondary_Higher_Secondary")
    print("Secondary_Higher_Secondary.zip created from J72")

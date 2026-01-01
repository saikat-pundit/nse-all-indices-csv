import requests
import csv
import re
from pathlib import Path

def fetch_fii_data():
    """Simplified FII data fetcher from NSDL"""
    
    url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_Dec152025.html"
    
    try:
        print("Fetching FII data from NSDL...")
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Find and extract table
        html = response.text
        table_start = html.find('<table')
        if table_start == -1:
            print("No table found")
            return False
        
        table_end = html.find('</table>', table_start)
        table_html = html[table_start:table_end+8]
        
        # Parse table rows
        rows = []
        for row_match in re.finditer(r'<tr.*?>(.*?)</tr>', table_html, re.DOTALL):
            row_html = row_match.group(1)
            
            # Extract cells
            cells = []
            for cell_match in re.finditer(r'<t[dh].*?>(.*?)</t[dh]>', row_html, re.DOTALL):
                cell_content = cell_match.group(1)
                
                # Remove HTML tags
                cell_content = re.sub(r'<.*?>', ' ', cell_content)
                
                # Clean up: remove commas and extra spaces
                cell_content = cell_content.replace(',', '').strip()
                cell_content = re.sub(r'\s+', ' ', cell_content)
                
                cells.append(cell_content)
            
            if cells:
                rows.append(cells)
        
        # Create directory if needed
        data_dir = Path(__file__).parent.parent / 'Data'
        data_dir.mkdir(exist_ok=True)
        
        # Save to CSV
        csv_path = data_dir / 'FII.csv'
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        
        print(f"✅ Saved {len(rows)} rows to {csv_path}")
        
        # Show sample
        if rows:
            print("\nFirst 3 rows:")
            for i, row in enumerate(rows[:3]):
                print(f"{i+1}: {row}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    fetch_fii_data()

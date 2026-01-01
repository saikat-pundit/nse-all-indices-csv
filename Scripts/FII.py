import requests
import csv
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_with_retry(url, max_retries=3):
    headers = {'User-Agent': 'Mozilla/5.0'}
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            logger.info(f"Fetched {len(response.text)} chars")
            return response.text
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                continue
            logger.error("All retries failed")
            raise

def extract_table_data(html):
    """Extract all table rows from HTML."""
    rows = []
    start = 0
    
    while True:
        tr_start = html.find('<tr', start)
        if tr_start == -1: break
        
        tr_end = html.find('</tr>', tr_start)
        if tr_end == -1: break
        
        row_html = html[tr_start:tr_end]
        cells = []
        cell_pos = 0
        
        # Extract cells from row
        while True:
            td_start = row_html.find('<td', cell_pos)
            th_start = row_html.find('<th', cell_pos)
            
            start_tag = td_start if td_start != -1 else th_start
            if start_tag == -1: break
            
            end_tag = '</td>' if td_start != -1 else '</th>'
            content_start = row_html.find('>', start_tag)
            cell_end = row_html.find(end_tag, content_start)
            
            if content_start == -1 or cell_end == -1: break
            
            # Clean cell content
            cell = row_html[content_start+1:cell_end]
            cell = ' '.join(cell.replace('&nbsp;', ' ').split())  # Basic cleaning
            cells.append(cell.strip())
            
            cell_pos = cell_end + len(end_tag)
        
        if cells:
            rows.append(cells)
        start = tr_end
    
    return rows

def save_csv(data, filepath):
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerows(data)
        logger.info(f"Saved {len(data)} rows to {filepath}")
        return True
    except Exception as e:
        logger.error(f"CSV save failed: {e}")
        return False

def main():
    url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_Dec152025.html"
    
    csv_path = Path(__file__).parent.parent / 'Data' / 'FII.csv'
    csv_path.parent.mkdir(exist_ok=True)
    
    try:
        html = fetch_with_retry(url)
        table_data = extract_table_data(html)
        
        if not table_data:
            logger.error("No data extracted")
            return False
        
        logger.info(f"Sample: {table_data[:2]}")
        
        if save_csv(table_data, csv_path):
            logger.info(f"✅ Success! {len(table_data)} rows at {datetime.now().strftime('%H:%M:%S')}")
            return True
            
    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        return False

if __name__ == "__main__":
    exit(0 if main() else 1)

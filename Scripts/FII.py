import requests
import csv
import time
from datetime import datetime
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_data():
    """Fetch HTML from NSDL with retry."""
    url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_Dec152025.html"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    for retry in range(3):
        try:
            logger.info(f"Fetching data (attempt {retry+1}/3)")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.warning(f"Attempt {retry+1} failed: {e}")
            if retry < 2:
                time.sleep(5)
    
    logger.error("All fetch attempts failed")
    return None

def extract_table(html):
    """Extract and parse table data."""
    start = html.find('<table')
    if start == -1:
        return []
    
    end = html.find('</table>', start)
    table_html = html[start:end+8]
    
    rows = []
    pos = 0
    
    while True:
        tr_start = table_html.find('<tr', pos)
        if tr_start == -1:
            break
        
        tr_end = table_html.find('</tr>', tr_start)
        if tr_end == -1:
            break
        
        row_html = table_html[tr_start:tr_end]
        cells = []
        cell_pos = 0
        
        while True:
            td_start = row_html.find('<td', cell_pos)
            if td_start == -1:
                td_start = row_html.find('<th', cell_pos)
                if td_start == -1:
                    break
            
            td_end = row_html.find('>', td_start)
            if td_end == -1:
                break
            
            td_close = row_html.find('</td>', td_end)
            if td_close == -1:
                td_close = row_html.find('</th>', td_end)
                if td_close == -1:
                    break
            
            content = row_html[td_end+1:td_close]
            
            # Remove HTML tags
            while '<' in content and '>' in content:
                tag_start = content.find('<')
                tag_end = content.find('>', tag_start)
                if tag_end != -1:
                    content = content[:tag_start] + content[tag_end+1:]
            
            # Remove commas from data
            content = content.replace(',', '').strip()
            cells.append(content)
            cell_pos = td_close + 5
        
        if cells:
            rows.append(cells)
        pos = tr_end
    
    return rows

def save_csv(data, filename):
    """Save data to CSV file."""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)
        logger.info(f"Saved {len(data)} rows to {filename}")
        return True
    except Exception as e:
        logger.error(f"Save failed: {e}")
        return False

def main():
    """Main function."""
    logger.info("=" * 50)
    logger.info("FII Data Fetcher")
    logger.info("=" * 50)
    
    # Setup paths
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'Data'
    data_dir.mkdir(exist_ok=True)
    csv_file = data_dir / 'FII.csv'
    
    # Fetch data
    html = fetch_data()
    if not html:
        return False
    
    # Extract data
    data = extract_table(html)
    if not data:
        logger.error("No data extracted")
        return False
    
    # Display sample
    logger.info(f"Extracted {len(data)} rows")
    logger.info("Sample (first 3 rows):")
    for i in range(min(3, len(data))):
        logger.info(f"  {data[i]}")
    
    # Save to CSV
    if save_csv(data, csv_file):
        logger.info("=" * 50)
        logger.info(f"✅ Success! Saved to {csv_file}")
        logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 50)
        return True
    
    return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

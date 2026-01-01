import requests
import csv
import time
from datetime import datetime
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_with_retry(url, max_retries=3, delay=5):
    """Fetch URL with retry logic."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30, verify=True)
            response.raise_for_status()
            logger.info(f"Fetched {len(response.text)} characters")
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                logger.error(f"All {max_retries} attempts failed")
                raise
    
    return None

def extract_table_data(table_html):
    """Extract data from HTML table."""
    rows = []
    row_start = 0
    
    while True:
        tr_start = table_html.find('<tr', row_start)
        if tr_start == -1:
            break
            
        tr_end = table_html.find('</tr>', tr_start)
        if tr_end == -1:
            break
            
        row_html = table_html[tr_start:tr_end]
        cells = extract_cells(row_html)
        
        if cells:  # Only add rows with data
            rows.append(cells)
        
        row_start = tr_end
    
    return rows

def extract_cells(row_html):
    """Extract cells from a table row."""
    cells = []
    cell_start = 0
    
    while True:
        # Look for table cells (td or th tags)
        td_start = row_html.find('<td', cell_start)
        th_start = row_html.find('<th', cell_start)
        
        if td_start == -1 and th_start == -1:
            break
        
        # Get the starting position of the first cell tag found
        if td_start == -1:
            cell_tag_start = th_start
            closing_tag = '</th>'
        elif th_start == -1:
            cell_tag_start = td_start
            closing_tag = '</td>'
        else:
            cell_tag_start = min(td_start, th_start)
            closing_tag = '</td>' if cell_tag_start == td_start else '</th>'
        
        # Find the content start (> after the opening tag)
        content_start = row_html.find('>', cell_tag_start)
        if content_start == -1:
            break
        
        # Find the closing tag
        cell_end = row_html.find(closing_tag, content_start)
        if cell_end == -1:
            break
        
        # Extract and clean cell content
        cell_content = row_html[content_start + 1:cell_end]
        cell_content = clean_html_content(cell_content)
        
        cells.append(cell_content)
        cell_start = cell_end + len(closing_tag)
    
    return cells

def clean_html_content(text):
    """Clean HTML content by removing tags and entities."""
    # Remove HTML tags
    while '<' in text and '>' in text:
        start = text.find('<')
        end = text.find('>', start)
        if end != -1:
            text = text[:start] + ' ' + text[end + 1:]
    
    # Replace common HTML entities
    replacements = {
        '&nbsp;': ' ',
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
        '&#39;': "'",
    }
    
    for entity, replacement in replacements.items():
        text = text.replace(entity, replacement)
    
    # Clean whitespace and REPLACE COMMAS
    text = ' '.join(text.split())
    text = text.replace(',', '')  # Remove all commas
    
    return text.strip()

def save_to_csv(data, filepath):
    """Save data to CSV file."""
    try:
        # Extract only columns at index 1 and 86 (0-indexed)
        filtered_data = []
        for row in data:
            if len(row) > 86:  # Ensure row has at least 87 columns
                # Keep only columns 1 and 86
                filtered_row = [row[1], row[86]]
                filtered_data.append(filtered_row)
            elif len(row) > 1:  # For header row or rows with fewer columns
                # For header row (index 0), show 5th column data (index 4)
                if len(filtered_data) == 0 and len(row) > 5:
                    filtered_row = [row[5], row[5]]  # Use 5th column for both positions
                else:
                    # For other rows, just take what's available
                    filtered_row = [row[1] if len(row) > 1 else "", 
                                   row[-1] if len(row) > 0 else ""]
                filtered_data.append(filtered_row)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(filtered_data)
        
        logger.info(f"Data saved to {filepath} ({len(filtered_data)} rows)")
        return True
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")
        return False

def main():
    """Main function to fetch and process FII data."""
    logger.info("FII Data Fetcher - NSDL Fortnightly Sector-wise Investment")
    
    url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_Dec152025.html"
    
    # Create Data directory if it doesn't exist
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'Data'
    data_dir.mkdir(exist_ok=True)
    csv_path = data_dir / 'FII.csv'
    
    # If old FII.csv exists, it will be replaced automatically when we write to it
    if csv_path.exists():
        logger.info(f"Replacing existing file: {csv_path}")
    
    try:
        # Fetch data with retry logic
        html_content = fetch_with_retry(url)
        
        if not html_content:
            logger.error("Failed to fetch HTML content")
            return False
        
        # Find the first table in HTML (simplified version)
        table_start = html_content.find('<table')
        if table_start == -1:
            logger.error("No table found in HTML")
            return False
        
        table_end = html_content.find('</table>', table_start)
        if table_end == -1:
            logger.error("Incomplete table in HTML")
            return False
        
        table_html = html_content[table_start:table_end + 8]
        
        # Extract data from the table
        table_data = extract_table_data(table_html)
        
        if not table_data:
            logger.error("No data extracted from table")
            return False
        
        logger.info(f"Extracted {len(table_data)} rows, showing columns 1 and 86 only")
        
        # Save to CSV (replaces if exists)
        success = save_to_csv(table_data, csv_path)
        
        if success:
            logger.info(f"✅ Success! Data saved to {csv_path}")
            return True
        else:
            return False
            
    except Exception as e:
        logger.error(f"❌ Error in execution: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

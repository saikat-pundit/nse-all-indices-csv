import requests
import csv
import os
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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries} to fetch data from NSDL...")
            response = requests.get(url, headers=headers, timeout=30, verify=True)
            response.raise_for_status()
            logger.info(f"Successfully fetched data ({len(response.text)} characters)")
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Waiting {delay} seconds before retry...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logger.error(f"All {max_retries} attempts failed")
                raise
    
    return None

def parse_table_html(html_content):
    """Parse HTML table and extract data."""
    tables = []
    start = 0
    
    while True:
        # Find the next table
        table_start = html_content.find('<table', start)
        if table_start == -1:
            break
            
        table_end = html_content.find('</table>', table_start)
        if table_end == -1:
            break
            
        table_html = html_content[table_start:table_end + 8]
        tables.append(table_html)
        start = table_end + 8
    
    return tables

def extract_table_data(table_html):
    """Extract data from HTML table."""
    rows = []
    row_start = 0
    
    while True:
        # Find table rows (tr tags)
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
    
    # Clean whitespace
    text = ' '.join(text.split())
    
    return text.strip()

def save_to_csv(data, filepath):
    """Save data to CSV file."""
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)
        
        logger.info(f"Data saved to {filepath} ({len(data)} rows)")
        return True
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")
        return False

def main():
    """Main function to fetch and process FII data."""
    logger.info("=" * 60)
    logger.info("FII Data Fetcher - NSDL Fortnightly Sector-wise Investment")
    logger.info("=" * 60)
    
    # URL for NSDL FII data (update the date in the URL as needed)
    url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_Dec152025.html"
    
    # Create Data directory if it doesn't exist
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'Data'
    data_dir.mkdir(exist_ok=True)
    csv_path = data_dir / 'FII.csv'
    
    try:
        # Fetch data with retry logic
        html_content = fetch_with_retry(url)
        
        if not html_content:
            logger.error("Failed to fetch HTML content")
            return False
        
        # Parse tables from HTML
        tables = parse_table_html(html_content)
        
        if not tables:
            logger.error("No tables found in HTML")
            return False
        
        logger.info(f"Found {len(tables)} table(s) in HTML")
        
        # Extract data from the first table (usually the main data table)
        table_data = extract_table_data(tables[0])
        
        if not table_data:
            logger.error("No data extracted from table")
            return False
        
        # Display sample data
        logger.info(f"\nExtracted {len(table_data)} rows of data")
        logger.info("\nSample of extracted data:")
        logger.info("-" * 60)
        for i, row in enumerate(table_data[:5]):
            logger.info(f"Row {i + 1}: {row}")
        logger.info("-" * 60)
        
        # Save to CSV
        success = save_to_csv(table_data, csv_path)
        
        if success:
            # Log success message
            logger.info("\n" + "=" * 60)
            logger.info("✅ SUCCESS: FII data fetched and saved successfully!")
            logger.info(f"📁 File: {csv_path}")
            logger.info(f"📊 Rows: {len(table_data)}")
            logger.info(f"🕐 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
            return True
        else:
            return False
            
    except Exception as e:
        logger.error(f"❌ Error in main execution: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

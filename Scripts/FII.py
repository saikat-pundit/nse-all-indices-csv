import requests
import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging
import calendar

# Set up logging - Reduce verbosity
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_primary_and_fallback_dates():
    """Get primary and fallback dates for fetching data."""
    today = datetime.now()
    
    # Get 15th of current month
    current_month_15th = today.replace(day=15)
    
    if today <= current_month_15th:
        # On or before 15th of current month
        # Primary: previous month's end
        if today.month == 1:
            primary_month = 12
            primary_year = today.year - 1
        else:
            primary_month = today.month - 1
            primary_year = today.year
        
        _, primary_day = calendar.monthrange(primary_year, primary_month)
        
        # Fallback: 15th of previous month
        fallback_month = primary_month
        fallback_year = primary_year
        fallback_day = 15
        
    else:
        # After 15th of current month
        # Primary: 15th of current month
        primary_month = today.month
        primary_year = today.year
        primary_day = 15
        
        # Fallback: previous month's end
        if today.month == 1:
            fallback_month = 12
            fallback_year = today.year - 1
        else:
            fallback_month = today.month - 1
            fallback_year = today.year
        
        _, fallback_day = calendar.monthrange(fallback_year, fallback_month)
    
    # Get month abbreviations
    primary_month_abbr = calendar.month_abbr[primary_month]
    fallback_month_abbr = calendar.month_abbr[fallback_month]
    
    primary_date_str = f"{primary_day}-{primary_month_abbr}-{primary_year}"
    fallback_date_str = f"{fallback_day}-{fallback_month_abbr}-{fallback_year}"
    
    primary_url = f"https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_{primary_month_abbr}{primary_day}{primary_year}.html"
    fallback_url = f"https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_{fallback_month_abbr}{fallback_day}{fallback_year}.html"
    
    return primary_url, primary_date_str, fallback_url, fallback_date_str

def fetch_url_with_retries(url, description, max_retries=3, delay=3):
    """Fetch URL with retry logic."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries}: {description}")
            response = requests.get(url, headers=headers, timeout=30, verify=True)
            
            # Check for 400/404 errors
            if response.status_code in [400, 404]:
                logger.info(f"URL not available (HTTP {response.status_code})")
                time.sleep(delay)
                continue
            
            response.raise_for_status()
            
            # Check if page contains valid data
            if len(response.text) < 5000 or "No Data" in response.text:
                logger.info("Page exists but no valid data found")
                time.sleep(delay)
                continue
            
            logger.info(f"Successfully fetched {len(response.text):,} characters")
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.info(f"Attempt failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    
    return None

def try_fetch_data():
    """Try to fetch data from primary and fallback URLs."""
    # Get URLs
    primary_url, primary_date, fallback_url, fallback_date = get_primary_and_fallback_dates()
    
    logger.info(f"Primary URL: {primary_date}")
    logger.info(f"Fallback URL: {fallback_date}")
    logger.info("-" * 50)
    
    # Try primary URL with retries
    html_content = fetch_url_with_retries(primary_url, f"Trying primary URL", 3, 3)
    
    if html_content:
        return html_content, primary_url, primary_date
    
    logger.info("Primary URL failed, trying fallback...")
    
    # Try fallback URL with retries
    html_content = fetch_url_with_retries(fallback_url, f"Trying fallback URL", 3, 3)
    
    if html_content:
        return html_content, fallback_url, fallback_date
    
    return None, None, None

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

def save_to_csv(data, filepath, url, date_str):
    """Save data to CSV file."""
    try:
        # Extract only columns at index 1 and 86 (0-indexed)
        filtered_data = []
        
        # Store currency unit for later use
        currency_unit = ""
        
        # Process header row
        if data and len(data) > 0:
            # First row - use 5th column data (index 4), but only in first column
            if len(data[0]) > 5:
                filtered_data.append([data[0][5], ""])
            else:
                filtered_data.append(["", ""])
        
        # Process remaining rows
        row_count = 0
        for i, row in enumerate(data[1:], 1):
            # Skip empty rows
            if not any(cell.strip() for cell in row):
                continue
                
            # Get currency unit from second row (index 1 in original data)
            if i == 1 and len(row) > 0:
                # Extract USD Mn or other currency info
                if any(unit in cell for unit in ["USD", "INR", "Rs"] for cell in row):
                    for cell in row:
                        if any(unit in cell for unit in ["USD", "INR", "Rs"]):
                            currency_unit = cell.strip()
                            break
                # Skip the currency unit row
                continue
                
            # Skip the third row (empty or useless row)
            if i == 2:
                continue
                
            # Process data rows
            if len(row) > 86:  # Ensure row has at least 87 columns
                # Keep only columns 1 and 86
                filtered_row = [row[1], row[86]]
                filtered_data.append(filtered_row)
            elif len(row) > 1:
                # For header row (Sectors, Equity)
                if "Sectors" in row[0] or "Equity" in row[0]:
                    # Add currency unit to Equity column if available
                    if "Equity" in row[-1] or row_count == 0:
                        if currency_unit:
                            filtered_row = [row[0], f"Equity({currency_unit})"]
                        else:
                            filtered_row = [row[0], "Equity"]
                    else:
                        filtered_row = [row[0], row[-1]]
                else:
                    # For other rows
                    filtered_row = [row[0] if len(row) > 0 else "", 
                                   row[-1] if len(row) > 0 else ""]
                filtered_data.append(filtered_row)
            
            row_count += 1
        
        # Check if we have meaningful data
        if len(filtered_data) <= 2:
            logger.info("No meaningful data found")
            return False, 0
        
        # Add data date info row
        filtered_data.append(["Data Date:", date_str])
        
        # Add timestamp row at the end in IST
        ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
        ist_time_str = ist_time.strftime("%d-%b %H:%M")
        filtered_data.append(["Update Time:", f"{ist_time_str} IST"])
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(filtered_data)
        
        logger.info(f"Saved {len(filtered_data)} rows to CSV")
        return True, len(filtered_data)
    except Exception as e:
        logger.info(f"Failed to save CSV: {e}")
        return False, 0

def main():
    """Main function to fetch and process FII data."""
    logger.info("Starting FII Data Fetcher")
    logger.info("=" * 50)
    
    # Create Data directory
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'Data'
    data_dir.mkdir(exist_ok=True)
    csv_path = data_dir / 'FII.csv'
    
    # Check existing file
    existing_data = None
    existing_rows = 0
    if csv_path.exists():
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                existing_data = f.read()
                existing_rows = len(f.readlines())
            logger.info(f"Existing file has {existing_rows} rows")
        except:
            pass
    
    try:
        # Try to fetch data
        html_content, successful_url, successful_date = try_fetch_data()
        
        if not html_content:
            logger.info("Could not fetch data from any URL")
            if existing_data:
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info("Restored existing data")
            return False
        
        # Extract table
        table_start = html_content.find('<table')
        if table_start == -1:
            logger.info("No table found in HTML")
            if existing_data:
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info("Restored existing data")
            return False
        
        table_end = html_content.find('</table>', table_start)
        if table_end == -1:
            logger.info("Incomplete table")
            if existing_data:
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info("Restored existing data")
            return False
        
        table_html = html_content[table_start:table_end + 8]
        
        # Extract data
        table_data = extract_table_data(table_html)
        
        if not table_data:
            logger.info("No data extracted")
            if existing_data:
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info("Restored existing data")
            return False
        
        logger.info(f"Extracted {len(table_data)} rows from HTML")
        
        # Save to CSV
        success, new_row_count = save_to_csv(table_data, csv_path, successful_url, successful_date)
        
        if success:
            logger.info("=" * 50)
            logger.info(f"SUCCESS: Data fetched for {successful_date}")
            logger.info(f"Rows: {new_row_count}")
            logger.info(f"Updated at: {(datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%d-%b %H:%M IST')}")
            logger.info("=" * 50)
            return True
        else:
            if existing_data:
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info("Restored existing data")
            return False
            
    except Exception as e:
        logger.info(f"Error: {e}")
        if existing_data:
            with open(csv_path, 'w', encoding='utf-8') as f:
                f.write(existing_data)
            logger.info("Restored existing data")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

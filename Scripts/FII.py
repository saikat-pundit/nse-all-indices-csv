import requests
import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging
import calendar

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_target_date(attempt=0):
    """Get target date for fetching data with fallback logic."""
    today = datetime.now()
    
    # Get 15th of current month
    current_month_15th = today.replace(day=15)
    
    if attempt == 0:
        # First attempt: Try the primary date
        if today <= current_month_15th:
            # On or before 15th of current month - fetch previous month's end
            if today.month == 1:
                target_month = 12
                target_year = today.year - 1
            else:
                target_month = today.month - 1
                target_year = today.year
            
            # Get last day of previous month
            _, prev_last_day = calendar.monthrange(target_year, target_month)
            selected_day = prev_last_day
            logger.info(f"📅 Primary attempt: Today ({today.strftime('%d-%b')}) ≤ 15th → Trying PREVIOUS month's END: {selected_day}-{calendar.month_abbr[target_month]}-{target_year}")
        else:
            # After 15th of current month - fetch current month's 15th
            target_month = today.month
            target_year = today.year
            selected_day = 15
            logger.info(f"📅 Primary attempt: Today ({today.strftime('%d-%b')}) > 15th → Trying CURRENT month's 15th: {selected_day}-{calendar.month_abbr[target_month]}-{target_year}")
    
    elif attempt == 1:
        # Second attempt: Try the other fortnight date
        if today <= current_month_15th:
            # If primary was month end, try 15th of previous month
            if today.month == 1:
                target_month = 12
                target_year = today.year - 1
            else:
                target_month = today.month - 1
                target_year = today.year
            selected_day = 15
            logger.info(f"📅 Fallback attempt {attempt}: Trying PREVIOUS month's 15th: {selected_day}-{calendar.month_abbr[target_month]}-{target_year}")
        else:
            # If primary was 15th of current month, try previous month's end
            if today.month == 1:
                target_month = 12
                target_year = today.year - 1
            else:
                target_month = today.month - 1
                target_year = today.year
            _, prev_last_day = calendar.monthrange(target_year, target_month)
            selected_day = prev_last_day
            logger.info(f"📅 Fallback attempt {attempt}: Trying PREVIOUS month's END: {selected_day}-{calendar.month_abbr[target_month]}-{target_year}")
    
    else:
        # Third attempt: Try one month earlier from primary date
        if today <= current_month_15th:
            # Try 15th of month before previous
            if today.month == 1:
                target_month = 11
                target_year = today.year - 1
            elif today.month == 2:
                target_month = 12
                target_year = today.year - 1
            else:
                target_month = today.month - 2
                target_year = today.year
            selected_day = 15
            logger.info(f"📅 Fallback attempt {attempt}: Trying 15th of month before previous: {selected_day}-{calendar.month_abbr[target_month]}-{target_year}")
        else:
            # Try end of month before previous
            if today.month == 1:
                target_month = 11
                target_year = today.year - 1
            elif today.month == 2:
                target_month = 12
                target_year = today.year - 1
            else:
                target_month = today.month - 2
                target_year = today.year
            _, month_end = calendar.monthrange(target_year, target_month)
            selected_day = month_end
            logger.info(f"📅 Fallback attempt {attempt}: Trying END of month before previous: {selected_day}-{calendar.month_abbr[target_month]}-{target_year}")
    
    # Get month abbreviation (Jan, Feb, etc.)
    month_abbr = calendar.month_abbr[target_month]
    
    return month_abbr, selected_day, target_year

def generate_url(attempt=0):
    """Generate dynamic URL with appropriate date and fallback logic."""
    month_abbr, day, year = get_target_date(attempt)
    url = f"https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_{month_abbr}{day}{year}.html"
    
    if attempt == 0:
        logger.info(f"🌐 Primary URL: {url}")
    else:
        logger.info(f"🌐 Fallback URL attempt {attempt}: {url}")
    
    return url, f"{day}-{month_abbr}-{year}"

def fetch_with_retry(url, max_retries=3, delay=5):
    """Fetch URL with retry logic."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            logger.info(f"🔍 Attempt {attempt + 1}/{max_retries} to fetch data...")
            response = requests.get(url, headers=headers, timeout=30, verify=True)
            
            # Check for 400/404 errors
            if response.status_code in [400, 404]:
                logger.warning(f"⚠️  URL not found (HTTP {response.status_code}): {url}")
                return None
            
            response.raise_for_status()
            
            # Check if page contains data or error message
            if "404" in response.text or "Page Not Found" in response.text or len(response.text) < 1000:
                logger.warning(f"⚠️  Page exists but may not contain valid data (length: {len(response.text)} chars)")
                return None
            
            logger.info(f"✅ Successfully fetched {len(response.text)} characters")
            return response.text
            
        except requests.exceptions.HTTPError as e:
            if response.status_code in [400, 404]:
                logger.warning(f"⚠️  URL not found (HTTP {response.status_code}): {url}")
                return None
            else:
                logger.warning(f"⚠️  Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"⏳ Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.error(f"❌ All {max_retries} attempts failed for this URL")
                    return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️  Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Waiting {delay} seconds before retry...")
                time.sleep(delay)
                delay *= 2
            else:
                logger.error(f"❌ All {max_retries} attempts failed for this URL")
                return None
    
    return None

def try_multiple_dates():
    """Try multiple date combinations to find available data."""
    max_date_attempts = 3
    html_content = None
    successful_url = None
    successful_date = None
    
    for attempt in range(max_date_attempts):
        url, date_str = generate_url(attempt)
        logger.info(f"🔄 Trying date combination {attempt + 1}/{max_date_attempts}: {date_str}")
        
        html_content = fetch_with_retry(url, max_retries=2, delay=3)
        
        if html_content is not None:
            successful_url = url
            successful_date = date_str
            logger.info(f"🎯 Found valid data for date: {date_str}")
            break
        else:
            logger.warning(f"📭 No data found for date: {date_str}")
            if attempt < max_date_attempts - 1:
                logger.info(f"⏳ Trying next date combination in 2 seconds...")
                time.sleep(2)
    
    return html_content, successful_url, successful_date

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
        
        # Check if we have meaningful data (more than just header and timestamp rows)
        if len(filtered_data) <= 2:  # Only header and timestamp rows
            logger.warning("⚠️  No meaningful data found in HTML table")
            return False, 0
        
        # Add data date info row
        filtered_data.append(["Data Date:", date_str])
        
        # Add timestamp row at the end in IST
        ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
        ist_time_str = ist_time.strftime("%d-%b %H:%M")  # Format: 01-Jan 19:30
        filtered_data.append(["Update Time:", f"{ist_time_str} IST"])
        
        # Add URL info row
        filtered_data.append(["Data Source:", url])
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(filtered_data)
        
        logger.info(f"💾 Data saved to {filepath} ({len(filtered_data)} rows)")
        return True, len(filtered_data)
    except Exception as e:
        logger.error(f"❌ Failed to save CSV: {e}")
        return False, 0

def main():
    """Main function to fetch and process FII data."""
    logger.info("=" * 60)
    logger.info("📊 FII Data Fetcher - NSDL Fortnightly Sector-wise Investment")
    logger.info("=" * 60)
    
    # Create Data directory if it doesn't exist
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'Data'
    data_dir.mkdir(exist_ok=True)
    csv_path = data_dir / 'FII.csv'
    
    # Check if old FII.csv exists
    existing_data = None
    existing_rows = 0
    if csv_path.exists():
        logger.info(f"📂 Existing file found: {csv_path}")
        try:
            # Read existing data to preserve it if new fetch fails
            with open(csv_path, 'r', encoding='utf-8') as f:
                existing_data = f.read()
                existing_rows = len(f.readlines())
            logger.info(f"📊 Existing data has {existing_rows} rows")
        except Exception as e:
            logger.warning(f"⚠️  Could not read existing CSV: {e}")
    
    try:
        # Try multiple date combinations to find available data
        logger.info("🔄 Trying multiple date combinations to find available data...")
        html_content, successful_url, successful_date = try_multiple_dates()
        
        if not html_content:
            logger.error("❌ Could not find data for any date combination")
            if existing_data and csv_path.exists():
                logger.info("🔄 Keeping existing data (no new data found)")
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info(f"✅ Kept existing data ({existing_rows} rows)")
            else:
                logger.warning("📭 No data available and no existing data to restore")
            return False
        
        logger.info(f"✅ Found data for date: {successful_date}")
        
        # Find the first table in HTML
        table_start = html_content.find('<table')
        if table_start == -1:
            logger.error("❌ No table found in HTML")
            if existing_data and csv_path.exists():
                logger.info("🔄 Restoring original data (no table found)")
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info(f"✅ Original data restored ({existing_rows} rows)")
            return False
        
        table_end = html_content.find('</table>', table_start)
        if table_end == -1:
            logger.error("❌ Incomplete table in HTML")
            if existing_data and csv_path.exists():
                logger.info("🔄 Restoring original data (incomplete table)")
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info(f"✅ Original data restored ({existing_rows} rows)")
            return False
        
        table_html = html_content[table_start:table_end + 8]
        
        # Extract data from the table
        logger.info("🔍 Extracting data from HTML table...")
        table_data = extract_table_data(table_html)
        
        if not table_data:
            logger.error("❌ No data extracted from table")
            if existing_data and csv_path.exists():
                logger.info("🔄 Restoring original data (no data extracted)")
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info(f"✅ Original data restored ({existing_rows} rows)")
            return False
        
        logger.info(f"📈 Extracted {len(table_data)} raw rows from HTML")
        
        # Save to CSV
        logger.info("💾 Saving data to CSV...")
        success, new_row_count = save_to_csv(table_data, csv_path, successful_url, successful_date)
        
        if success:
            logger.info("=" * 60)
            logger.info(f"✅ DATA FETCHED SUCCESSFULLY!")
            logger.info(f"📅 Data Date: {successful_date}")
            logger.info(f"📁 File: {csv_path}")
            logger.info(f"📊 Total rows: {new_row_count}")
            logger.info(f"🔄 Replaced: {existing_rows} old rows")
            logger.info(f"🌐 Source: {successful_url}")
            logger.info(f"🕐 Updated at: {(datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%d-%b %H:%M IST')}")
            logger.info("=" * 60)
            return True
        else:
            # If save fails, restore original data
            if existing_data and csv_path.exists():
                logger.info("🔄 Restoring original data due to save failure")
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write(existing_data)
                logger.info(f"✅ Original data restored ({existing_rows} rows)")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error in execution: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # Restore original data on any exception
        if existing_data and csv_path.exists():
            logger.info("🔄 Restoring original data due to exception")
            with open(csv_path, 'w', encoding='utf-8') as f:
                f.write(existing_data)
            logger.info(f"✅ Original data restored ({existing_rows} rows)")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting FII Data Fetcher...")
    success = main()
    if success:
        logger.info("✨ Script completed successfully!")
    else:
        logger.info("⚠️  Script completed with warnings/errors")
    exit(0 if success else 1)

#!/usr/bin/env python3
"""
BSE India Market Data Fetcher
Automatically fetches market capitalization data from BSE India API
"""

import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BSEIndiaFetcher:
    def __init__(self, base_dir="data"):
        self.base_url = "https://api.bseindia.com/BseIndiaAPI/api"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.bseindia.com/",
            "Origin": "https://www.bseindia.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        
    def fetch_market_cap_data(self, category=1, type=2, retries=3):
        """
        Fetch market capitalization data from BSE API
        
        Args:
            category: Category filter (default: 1)
            type: Type filter (default: 2)
            retries: Number of retry attempts
            
        Returns:
            JSON data or None if failed
        """
        url = f"{self.base_url}/MktCapBoard_indstream/w?cat={category}&type={type}"
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching data from BSE API (attempt {attempt + 1}/{retries})...")
                
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    timeout=30,
                    verify=True
                )
                
                response.raise_for_status()
                
                # Try to parse JSON
                data = response.json()
                logger.info(f"Successfully fetched data. Type: {type(data)}")
                
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All {retries} attempts failed")
                    return None
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                logger.debug(f"Response text: {response.text[:500]}")
                return None
        
        return None
    
    def process_to_dataframe(self, data):
        """
        Convert API response to pandas DataFrame
        
        Args:
            data: JSON data from API
            
        Returns:
            pandas DataFrame or None
        """
        if not data:
            logger.error("No data to process")
            return None
        
        try:
            # Handle different response formats
            if isinstance(data, list):
                df = pd.DataFrame(data)
                logger.info(f"Processed list data with {len(df)} rows and {len(df.columns)} columns")
            elif isinstance(data, dict):
                # Try to find list of records in dictionary
                df = None
                for key, value in data.items():
                    if isinstance(value, list):
                        df = pd.DataFrame(value)
                        logger.info(f"Found list in key '{key}' with {len(df)} rows")
                        break
                
                if df is None:
                    # If no list found, create DataFrame from dict
                    df = pd.DataFrame([data])
                    logger.info("Created DataFrame from dictionary")
            else:
                logger.error(f"Unsupported data type: {type(data)}")
                return None
            
            # Add metadata columns
            df['fetch_timestamp'] = datetime.now().isoformat()
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing data to DataFrame: {e}")
            return None
    
    def save_to_csv(self, df, filename=None):
        """
        Save DataFrame to CSV file
        
        Args:
            df: pandas DataFrame
            filename: Output filename (optional)
            
        Returns:
            Path to saved file or None
        """
        if df is None or df.empty:
            logger.error("No data to save")
            return None
        
        try:
            if filename is None:
                # Generate filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"bse_market_cap_{timestamp}.csv"
            
            filepath = self.base_dir / filename
            
            # Save to CSV
            df.to_csv(filepath, index=False, encoding='utf-8')
            logger.info(f"Data saved to {filepath} ({len(df)} rows)")
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
            return None
    
    def save_to_json(self, data, filename=None):
        """
        Save raw JSON data to file
        
        Args:
            data: Raw JSON data
            filename: Output filename (optional)
            
        Returns:
            Path to saved file or None
        """
        if not data:
            logger.error("No data to save")
            return None
        
        try:
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"bse_raw_data_{timestamp}.json"
            
            filepath = self.base_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Raw JSON saved to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving JSON: {e}")
            return None
    
    def run(self, save_raw_json=False):
        """
        Main execution method
        
        Args:
            save_raw_json: Whether to save raw JSON data
            
        Returns:
            Dictionary with results
        """
        results = {
            'success': False,
            'csv_file': None,
            'json_file': None,
            'timestamp': datetime.now().isoformat(),
            'row_count': 0
        }
        
        # Fetch data
        raw_data = self.fetch_market_cap_data()
        
        if raw_data is None:
            logger.error("Failed to fetch data from BSE API")
            return results
        
        # Save raw JSON if requested
        if save_raw_json:
            json_file = self.save_to_json(raw_data)
            results['json_file'] = str(json_file) if json_file else None
        
        # Process to DataFrame
        df = self.process_to_dataframe(raw_data)
        
        if df is None:
            logger.error("Failed to process data")
            return results
        
        # Save to CSV
        csv_file = self.save_to_csv(df)
        
        if csv_file:
            results.update({
                'success': True,
                'csv_file': str(csv_file),
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': list(df.columns)
            })
        
        return results


def main():
    """Main function for command-line execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch BSE India market data')
    parser.add_argument('--output-dir', '-o', default='data', 
                       help='Output directory for CSV files')
    parser.add_argument('--save-json', '-j', action='store_true',
                       help='Save raw JSON data')
    parser.add_argument('--category', '-c', type=int, default=1,
                       help='Category parameter for API')
    parser.add_argument('--type', '-t', type=int, default=2,
                       help='Type parameter for API')
    
    args = parser.parse_args()
    
    # Initialize fetcher
    fetcher = BSEIndiaFetcher(base_dir=args.output_dir)
    
    logger.info("="*50)
    logger.info("BSE India Market Data Fetcher")
    logger.info(f"Started at: {datetime.now().isoformat()}")
    logger.info("="*50)
    
    # Run fetcher
    results = fetcher.run(save_raw_json=args.save_json)
    
    # Print results
    logger.info("\n" + "="*50)
    logger.info("FETCH RESULTS")
    logger.info("="*50)
    
    if results['success']:
        logger.info(f"✓ Successfully fetched and saved data")
        logger.info(f"  CSV File: {results['csv_file']}")
        logger.info(f"  Rows: {results['row_count']:,}")
        logger.info(f"  Columns: {results['column_count']}")
        
        if results['json_file']:
            logger.info(f"  JSON File: {results['json_file']}")
    else:
        logger.error("✗ Failed to fetch or save data")
    
    logger.info(f"Completed at: {datetime.now().isoformat()}")
    logger.info("="*50)
    
    return 0 if results['success'] else 1


if __name__ == "__main__":
    exit(main())

#!/usr/bin/env python3
"""
ECI Data Extractor
Script to extract data from ECI API and save to CSV format
"""

import requests
import json
import csv
import yaml
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass, asdict
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('eci_extractor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ECIConfig:
    """Configuration for ECI API"""
    base_url: str = "https://gateway-officials.eci.gov.in/api/v1/noticeMapping/getRecords"
    bearer_token: str = ""
    pref_username: str = ""
    state_cd: str = "S25"
    ac_no: int = 260
    default_part_list: List[int] = None
    
    def __post_init__(self):
        if self.default_part_list is None:
            self.default_part_list = [
                210, 10, 166, 212, 9, 161, 7, 122, 164, 88, 89, 208, 8, 165,
                121, 119, 86, 211, 209, 163, 206, 207, 214, 123, 160, 213,
                167, 87, 215, 216, 217, 162, 168, 120
            ]

@dataclass
class ExtractionResult:
    """Result of data extraction"""
    success: bool
    total_records: int = 0
    csv_file: str = ""
    json_file: str = ""
    error_message: str = ""
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

class ECIAPIClient:
    """Client for interacting with ECI API"""
    
    def __init__(self, config: ECIConfig):
        self.config = config
        self.session = requests.Session()
        self._setup_headers()
        
    def _setup_headers(self):
        """Setup HTTP headers for API requests"""
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "applicationName": "ERONET2.0",
            "PLATFORM-TYPE": "ECIWEB",
            "Authorization": f"Bearer {self.config.bearer_token}",
            "currentRole": "aero",
            "state": self.config.state_cd,
            "appName": "ERONET2.0",
            "atkn_bnd": "Be+/vXoD77+9Fe+/vTDvv71sRUAxE++/vRDvv70/K++/vSzvv70HJO+/ve+/vQ7vv73vv73vv70qcQ==",
            "rtkn_bnd": "77+977+9N++/ve+/ve+/ve+/ve+/vXrvv73hm6Xvv73vv712LzYq77+977+977+977+9Fe+/ve+/ve+/vQjvv70g",
            "channelidobo": "ERONET",
            "Origin": "https://officials.eci.gov.in",
            "DNT": "1",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Priority": "u=0"
        }
        self.session.headers.update(self.headers)
    
    def fetch_data(self, part_no: int = 216, page_number: int = 1, 
                   page_limit: int = 100, category_type: str = "ld",
                   part_list: Optional[List[int]] = None) -> Dict[str, Any]:
        """Fetch data from ECI API"""
        
        if part_list is None:
            part_list = self.config.default_part_list
        
        params = {
            "partNo": part_no,
            "stateCd": self.config.state_cd,
            "acNo": self.config.ac_no,
            "pageNumber": page_number,
            "pageLimit": page_limit,
            "isTotalCount": "Y",
            "categoryType": category_type,
            "prefUserName": self.config.pref_username,
            "partList": ",".join(map(str, part_list))
        }
        
        try:
            logger.info(f"Fetching data: Part={part_no}, Page={page_number}")
            
            response = self.session.get(
                self.config.base_url, 
                params=params,
                timeout=30
            )
            
            # Log rate limiting info
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = response.headers['X-RateLimit-Remaining']
                logger.info(f"Rate limit remaining: {remaining}")
            
            if response.status_code == 200:
                return {
                    "success": True,
                    "data": response.json(),
                    "status_code": response.status_code,
                    "headers": dict(response.headers)
                }
            else:
                logger.error(f"HTTP Error {response.status_code}")
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}",
                    "response_text": response.text[:500],
                    "status_code": response.status_code
                }
                
        except requests.exceptions.Timeout:
            logger.error("Request timeout")
            return {"success": False, "error": "Request timeout"}
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return {"success": False, "error": str(e)}
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return {"success": False, "error": "Invalid JSON response"}

class DataProcessor:
    """Process and save ECI data"""
    
    @staticmethod
    def flatten_nested_data(data: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested dictionaries for CSV export"""
        items = {}
        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.update(DataProcessor.flatten_nested_data(v, new_key, sep))
                elif isinstance(v, list):
                    # Convert lists to JSON strings for CSV
                    items[new_key] = json.dumps(v, ensure_ascii=False)
                else:
                    items[new_key] = v
        else:
            items[parent_key] = data
        return items
    
    @staticmethod
    def extract_records_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract all records from API payload"""
        all_records = []
        
        # Try different possible record locations
        record_keys = ['records', 'data', 'list', 'items', 'result']
        
        for key in record_keys:
            if key in payload and isinstance(payload[key], list):
                all_records.extend(payload[key])
        
        # If no standard keys found, look for any lists in the payload
        if not all_records:
            for key, value in payload.items():
                if isinstance(value, list):
                    all_records.extend(value)
        
        return all_records
    
    @staticmethod
    def save_to_csv(records: List[Dict[str, Any]], filename: str) -> bool:
        """Save records to CSV file"""
        if not records:
            logger.warning("No records to save")
            return False
        
        try:
            # Flatten all records
            flattened_records = []
            for record in records:
                flattened_records.append(DataProcessor.flatten_nested_data(record))
            
            # Get all unique keys
            all_keys = set()
            for record in flattened_records:
                all_keys.update(record.keys())
            
            fieldnames = sorted(all_keys)
            
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for record in flattened_records:
                    # Ensure all keys are present in each row
                    row = {key: record.get(key, '') for key in fieldnames}
                    writer.writerow(row)
            
            logger.info(f"Saved {len(records)} records to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False
    
    @staticmethod
    def save_to_json(data: Any, filename: str) -> bool:
        """Save data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False, default=str)
            logger.info(f"Saved data to {filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            return False

class ECIExtractor:
    """Main extractor class"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self.api_client = ECIAPIClient(self.config)
        self.processor = DataProcessor()
        
    def _load_config(self, config_path: Optional[str]) -> ECIConfig:
        """Load configuration from YAML file or environment"""
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config_data = yaml.safe_load(f)
                
                # Convert part_list string to list if needed
                if 'default_part_list' in config_data:
                    if isinstance(config_data['default_part_list'], str):
                        config_data['default_part_list'] = [
                            int(x.strip()) for x in config_data['default_part_list'].split(',')
                        ]
                
                return ECIConfig(**config_data)
            except Exception as e:
                logger.error(f"Error loading config: {e}")
        
        # Fallback to environment variables or defaults
        return ECIConfig(
            bearer_token=os.getenv('ECI_BEARER_TOKEN', ''),
            pref_username=os.getenv('ECI_PREF_USERNAME', '')
        )
    
    def extract_all_data(self, output_dir: str = "output", 
                        save_json: bool = True) -> ExtractionResult:
        """
        Extract all data from ECI API and save to CSV
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Create base filenames
        base_filename = f"eci_data_{timestamp}"
        csv_file = output_path / f"{base_filename}.csv"
        json_file = output_path / f"{base_filename}.json"
        metadata_file = output_path / f"{base_filename}_metadata.yaml"
        
        try:
            # Fetch data from API
            logger.info("Starting data extraction from ECI API")
            result = self.api_client.fetch_data()
            
            if not result["success"]:
                return ExtractionResult(
                    success=False,
                    error_message=result.get("error", "Unknown error")
                )
            
            api_data = result["data"]
            
            # Save raw JSON if requested
            if save_json:
                self.processor.save_to_json(api_data, str(json_file))
            
            # Extract records from payload
            records = []
            if isinstance(api_data, dict) and 'payload' in api_data:
                records = self.processor.extract_records_from_payload(api_data['payload'])
            elif isinstance(api_data, list):
                records = api_data
            
            total_records = len(records)
            
            if total_records == 0:
                logger.warning("No records found in API response")
                # Save the entire response as a single record
                records = [api_data]
                total_records = 1
            
            # Save to CSV
            csv_success = self.processor.save_to_csv(records, str(csv_file))
            
            if not csv_success:
                return ExtractionResult(
                    success=False,
                    error_message="Failed to save CSV file"
                )
            
            # Create metadata
            metadata = {
                "extraction_timestamp": datetime.now().isoformat(),
                "total_records": total_records,
                "api_status": api_data.get('statusCode', 'unknown'),
                "api_message": api_data.get('message', ''),
                "rate_limit": {
                    "remaining": result.get("headers", {}).get("X-RateLimit-Remaining", "unknown"),
                    "capacity": result.get("headers", {}).get("X-RateLimit-Burst-Capacity", "unknown"),
                    "rate": result.get("headers", {}).get("X-RateLimit-Replenish-Rate", "unknown")
                }
            }
            
            # Save metadata
            with open(metadata_file, 'w') as f:
                yaml.dump(metadata, f, default_flow_style=False)
            
            logger.info(f"Extraction complete: {total_records} records saved")
            
            return ExtractionResult(
                success=True,
                total_records=total_records,
                csv_file=str(csv_file),
                json_file=str(json_file) if save_json else "",
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return ExtractionResult(
                success=False,
                error_message=str(e)
            )
    
    def extract_multiple_pages(self, output_dir: str = "output", 
                              max_pages: int = 10) -> ExtractionResult:
        """
        Extract data from multiple pages
        """
        all_records = []
        metadata_list = []
        
        for page in range(1, max_pages + 1):
            logger.info(f"Fetching page {page}")
            result = self.api_client.fetch_data(page_number=page)
            
            if not result["success"]:
                logger.error(f"Failed to fetch page {page}: {result.get('error')}")
                break
            
            api_data = result["data"]
            
            # Extract records
            records = []
            if isinstance(api_data, dict) and 'payload' in api_data:
                records = self.processor.extract_records_from_payload(api_data['payload'])
            elif isinstance(api_data, list):
                records = api_data
            
            all_records.extend(records)
            
            # Check if we should continue (no more records)
            if not records:
                logger.info(f"No more records on page {page}, stopping")
                break
            
            # Respect rate limiting
            time.sleep(0.5)
        
        # Save all records to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        csv_file = output_path / f"eci_data_multipage_{timestamp}.csv"
        
        success = self.processor.save_to_csv(all_records, str(csv_file))
        
        if success:
            return ExtractionResult(
                success=True,
                total_records=len(all_records),
                csv_file=str(csv_file)
            )
        else:
            return ExtractionResult(
                success=False,
                error_message="Failed to save multipage CSV"
            )

def main():
    """Main entry point"""
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Extract ECI data to CSV')
    parser.add_argument('--config', '-c', default='config.yaml',
                       help='Path to config file (default: config.yaml)')
    parser.add_argument('--output', '-o', default='output',
                       help='Output directory (default: output)')
    parser.add_argument('--multipage', '-m', action='store_true',
                       help='Extract multiple pages')
    parser.add_argument('--max-pages', type=int, default=10,
                       help='Maximum pages to extract (default: 10)')
    parser.add_argument('--no-json', action='store_true',
                       help='Do not save JSON file')
    
    args = parser.parse_args()
    
    # Check if config file exists
    if not os.path.exists(args.config):
        logger.warning(f"Config file {args.config} not found")
        logger.info("Please create a config.yaml file or use environment variables")
        logger.info("Required environment variables: ECI_BEARER_TOKEN, ECI_PREF_USERNAME")
        
        # Check for environment variables
        if not os.getenv('ECI_BEARER_TOKEN') or not os.getenv('ECI_PREF_USERNAME'):
            logger.error("No configuration found. Exiting.")
            sys.exit(1)
    
    try:
        # Create extractor
        extractor = ECIExtractor(args.config)
        
        # Extract data
        if args.multipage:
            result = extractor.extract_multiple_pages(
                output_dir=args.output,
                max_pages=args.max_pages
            )
        else:
            result = extractor.extract_all_data(
                output_dir=args.output,
                save_json=not args.no_json
            )
        
        # Print results
        print("\n" + "="*60)
        if result.success:
            print("✅ EXTRACTION SUCCESSFUL")
            print(f"📊 Total Records: {result.total_records}")
            print(f"💾 CSV File: {result.csv_file}")
            if result.json_file:
                print(f"📄 JSON File: {result.json_file}")
            
            # Print sample of CSV
            if os.path.exists(result.csv_file):
                with open(result.csv_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                    print(f"\n📋 CSV Headers ({len(headers)} columns):")
                    for i, header in enumerate(headers[:10], 1):
                        print(f"  {i:2}. {header}")
                    if len(headers) > 10:
                        print(f"  ... and {len(headers) - 10} more columns")
        else:
            print("❌ EXTRACTION FAILED")
            print(f"Error: {result.error_message}")
        print("="*60)
        
        # Exit with appropriate code
        sys.exit(0 if result.success else 1)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

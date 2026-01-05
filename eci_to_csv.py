#!/usr/bin/env python3
"""
ECI Data to CSV Converter with better error handling
"""

import requests
import json
import csv
import os
import time
from datetime import datetime
from typing import Dict, List, Any

def get_eci_data_with_retry(max_retries=3, retry_delay=5):
    """Fetch data from ECI API with retry logic"""
    
    url = "https://gateway-officials.eci.gov.in/api/v1/noticeMapping/getRecords"
    
    params = {
        "partNo": 216,
        "stateCd": "S25",
        "acNo": 260,
        "pageNumber": 1,
        "pageLimit": 100,
        "isTotalCount": "Y",
        "categoryType": "ld",
        "prefUserName": "a71782e5-0c9f-4216-ab8c-6f109acb8965",
        "partList": "210,10,166,212,9,161,7,122,164,88,89,208,8,165,121,119,86,211,209,163,206,207,214,123,160,213,167,87,215,216,217,162,168,120"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.5",
        "applicationName": "ERONET2.0",
        "PLATFORM-TYPE": "ECIWEB",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJDczJZLThBb3c2bEU4NW5xMnJuRE94bVpWTkRxYmpHUE5wLVNGdzQ3RjdzIn0.eyJleHAiOjE3Njc2NjgwNDAsImlhdCI6MTc2NzYyNDg0MCwianRpIjoiMjA0YTk2YWUtMGVhYi00OTFhLTk0NWUtYjA1ZmM3NDM1ZDE0IiwiaXNzIjoiaHR0cDovLzEwLjIxMC4xMTMuMjE6ODA4MC9yZWFsbXMvZWNpLXByb2QtcmVhbG0iLCJhdWQiOlsicmVhbG0tbWFuYWdlbWVudCIsImFjY291bnQiXSwic3ViIjoiNjNlYTk3YTAtOWY2MS00OTEyLTliZmYtZTA3MjAzNjk4MWIwIiwidHlwIdoiQmVhcmVyIiwiYXpwIjoiYXV0aG4tY2xpZW50Iiwic2Vzc2lvbl9zdGF0ZSI6IjcyZTg4ZDg5LTcwN2UtNGIyZC05ZWRkLWE3OWVhODdkNzc3OSIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiYWVybyIsImRlZmF1bHQtcm9sZXMtZWNpLXByb2QtcmVhbG0iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZWFsbS1tYW5hZ2VtZW50Ijp7InJvbGVzIjpbImltcGVyc29uYXRpb24iXX0sImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoicHJvZmlsZSBlbWFpbCIsInNpZCI6IjcyZTg4ZDg5LTcwN2UtNGIyZC05ZWRkLWE3OWVhODdkNzc3OSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiYXV0aG9yaXplZFN0YXRlcyI6WyJTMjUiXSwicm9sZUlkIjo5LCJhdXRob3JpemVkRGlzdHJpY3RzIjpbIlMyNTIwIl0sImVtYWlsSWQiOiJrYXVzaWtpY2hhdHRlcmplZUBnbWFpbC5jb20iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhNzE3ODJlNS0wYzlmLTQyMTYtYWI4Yy02ZjEwOWFjYjg5NjUiLCJhdXRob3JpemVkQWNzIjpbMjYwXSwiZ2l2ZW5fbmFtZSI6IkFybmFiIiwibG9naW5OYW1lIjoiQUVST1MyNUEyNjBOMiIsIm5hbWUiOiJBcm5hYiBDaGF0dGVyamVlIiwicGhvbmVfbnVtYmVyIjoiNzcxOTM3MTgxNCIsImZhbWlseV9uYW1lIjoiQ2hhdHRlcmplZSIsImF1dGhvcml6ZWRQYXJ0cyI6WzIxMCwxMCwxNjYsMjEyLDksMTYxLDcsMTIyLDE2NCw4OCw4OSwyMDgsOCwxNjUsMTIxLDExOSw4NiwyMTEsMjA5LDE2MywyMDYsMjA3LDIxNCwxMjMsMTYwLDIxMywxNjcsODcsMjE1LDIxNiwyMTcsMTYyLDE2OCwxMjBdfQ.KJNfZ7rSmU6tAe9OJDPxu43Q9U7lfbAKe_vVNa9VNDuBaPUILJPMkPHe2qyYwT1yibIwEe6F5iZ54DLf8TMYjPW42tH_UzFN-IhI10BMUoI6U8iHHoFBbiSTetiyjNq-SDJGp-shrdT71nk7mt3npkJFxsEz7qUrZ1VSv6UznT20olTvotVkVGHESik3BfGHXQx0BpY1cEr58YuC5cFQ5WhuRzK8Q5jyHTGOY3id67Q_fJGak13epPsOwyePGG4cKCl7mpnRkIA5Bpa-TeKeWCVZN6p9ichqLi7fJnkrPerj3ctry8nNkNENgAYhBEqhk9OBXM8HF7WXxpYpaHvVRQ",
        "currentRole": "aero",
        "state": "S25",
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
    
    # Try with retries
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} to fetch data...")
            print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Use longer timeout for GitHub Actions
            response = requests.get(
                url, 
                params=params, 
                headers=headers, 
                timeout=(15, 30)  # (connect timeout, read timeout)
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ API call successful on attempt {attempt + 1}")
                
                if 'X-RateLimit-Remaining' in response.headers:
                    remaining = response.headers['X-RateLimit-Remaining']
                    print(f"Rate limit remaining: {remaining}")
                
                return data
            else:
                print(f"❌ API error {response.status_code} on attempt {attempt + 1}")
                print(f"Response: {response.text[:200]}")
                
        except requests.exceptions.Timeout:
            print(f"⚠️ Timeout on attempt {attempt + 1}")
        except requests.exceptions.ConnectionError:
            print(f"⚠️ Connection error on attempt {attempt + 1}")
        except Exception as e:
            print(f"⚠️ Error on attempt {attempt + 1}: {e}")
        
        # Wait before retry (except on last attempt)
        if attempt < max_retries - 1:
            print(f"Waiting {retry_delay} seconds before retry...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
    
    print("❌ All retry attempts failed")
    return None

def extract_elector_data(api_data: Dict) -> List[Dict]:
    """Extract elector details from API response"""
    
    if not api_data or 'payload' not in api_data:
        print("❌ No payload found in API response")
        return []
    
    payload = api_data['payload']
    
    if 'electorDetailDto' not in payload:
        print("❌ No electorDetailDto found in payload")
        return []
    
    electors = payload['electorDetailDto']
    
    if not isinstance(electors, list):
        print("❌ electorDetailDto is not a list")
        return []
    
    print(f"Found {len(electors)} elector records")
    return electors

def save_to_csv(data: List[Dict], filename: str):
    """Save data to CSV file"""
    
    if not data:
        print("❌ No data to save")
        return False
    
    try:
        fieldnames = list(data[0].keys())
        
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"✅ Saved {len(data)} records to {filename}")
        print(f"Columns: {len(fieldnames)}")
        return True
        
    except Exception as e:
        print(f"❌ Error saving CSV: {e}")
        return False

def save_to_json(data: Dict, filename: str):
    """Save raw API response to JSON file"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved raw data to {filename}")
        return True
    except Exception as e:
        print(f"❌ Error saving JSON: {e}")
        return False

def create_sample_data():
    """Create sample data for testing when API fails"""
    print("⚠️ Creating sample data for testing...")
    
    sample_data = {
        "status": None,
        "statusCode": 200,
        "refId": None,
        "message": "Data fetched successfully",
        "payload": {
            "electorDetailDto": [
                {
                    "id": 7419866,
                    "epicId": 33419796,
                    "epicNo": "UWX2212983",
                    "acNo": 260,
                    "partNo": 216,
                    "partSerialNo": 279,
                    "electorName": "Bishnu Biswas",
                    "electorNameVer": "বিষ্ণু বিশ্বাস",
                    "electorMobileNo": "9883960193",
                    "electorEfMobile": "8768528801",
                    "category": "progeny",
                    "userId": "a71782e5-0c9f-4216-ab8c-6f109acb8965",
                    "loginName": "AEROS25A260N2",
                    "address": "District Magistrate and District Collectorate Office",
                    "fullName": "Arnab Chatterjee",
                    "progLimitExceed": False,
                    "parentNameMismatch": True,
                    "extraInfo": "0050",
                    "createdOn": "2026-01-02T21:53:08",
                    "createdBy": "BACKEND"
                }
            ]
        }
    }
    
    print("✅ Sample data created")
    return sample_data

def main():
    """Main function to extract and save data"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "eci_data"
    os.makedirs(output_dir, exist_ok=True)
    
    print("=" * 60)
    print("ECI Data Extraction Started")
    print("=" * 60)
    
    # Step 1: Try to get real data with retries
    api_data = get_eci_data_with_retry(max_retries=3, retry_delay=5)
    
    use_sample_data = False
    if not api_data:
        print("⚠️ Failed to get real data from API")
        
        # Check if we should use sample data
        if os.getenv('USE_SAMPLE_DATA', 'false').lower() == 'true':
            print("Using sample data as configured...")
            api_data = create_sample_data()
            use_sample_data = True
        else:
            print("❌ Extraction failed. No data to save.")
            return
    
    # Step 2: Save raw JSON
    json_file = os.path.join(output_dir, f"eci_raw_{timestamp}.json")
    save_to_json(api_data, json_file)
    
    # Step 3: Extract elector data
    elector_data = extract_elector_data(api_data)
    
    if not elector_data:
        print("❌ No elector data extracted")
        return
    
    # Step 4: Save to CSV
    csv_file = os.path.join(output_dir, f"eci_electors_{timestamp}.csv")
    csv_success = save_to_csv(elector_data, csv_file)
    
    # Step 5: Create a summary file
    summary_file = os.path.join(output_dir, f"summary_{timestamp}.txt")
    try:
        with open(summary_file, 'w') as f:
            f.write(f"ECI Data Extraction Summary\n")
            f.write(f"===========================\n")
            f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source: {'SAMPLE DATA' if use_sample_data else 'REAL API'}\n")
            f.write(f"Total Records: {len(elector_data)}\n")
            f.write(f"CSV File: {csv_file if csv_success else 'Failed'}\n")
            f.write(f"JSON File: {json_file}\n")
        
        print(f"✅ Summary saved to {summary_file}")
    except Exception as e:
        print(f"❌ Error saving summary: {e}")
    
    print("\n" + "=" * 60)
    print("EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"📊 Records extracted: {len(elector_data)}")
    print(f"💾 CSV saved: {csv_file}")
    print(f"📄 JSON saved: {json_file}")
    if use_sample_data:
        print(f"⚠️ Note: Using sample data (API failed)")
    print("=" * 60)

if __name__ == "__main__":
    main()

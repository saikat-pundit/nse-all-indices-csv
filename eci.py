"""
ECI API to CSV Converter
Fetches data from ECI API and converts to CSV format
"""

import requests
import csv
import json
import sys
from datetime import datetime

class ECIToCSV:
    def __init__(self):
        self.base_url = "https://gateway-officials.eci.gov.in/api/v1/noticeMapping/getRecords"
        self.csv_file = f"eci_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
    def fetch_data(self, params, headers):
        """Fetch data from ECI API"""
        try:
            response = requests.get(self.base_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
    
    def create_csv(self, data):
        """Create CSV file from API response"""
        if not data or 'payload' not in data:
            print("No valid data received from API")
            return False
        
        elector_details = data['payload'].get('electorDetailDto', [])
        
        if not elector_details:
            print("No elector details found in response")
            print(f"API Message: {data.get('message', 'No message')}")
            return False
        
        # Extract field names from first record
        fieldnames = list(elector_details[0].keys())
        
        # Write to CSV
        try:
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for record in elector_details:
                    writer.writerow(record)
            
            print(f"CSV file created successfully: {self.csv_file}")
            print(f"Total records exported: {len(elector_details)}")
            return True
            
        except Exception as e:
            print(f"Error creating CSV: {e}")
            return False
    
    def generate_sample_csv(self):
        """Generate a sample CSV when no data is available"""
        sample_data = [
            {
                "slNo": 1,
                "partNo": "7",
                "partName": "Sample Part 1",
                "electorName": "John Doe",
                "relativeName": "Jane Doe",
                "houseNo": "123",
                "age": "35",
                "gender": "M",
                "epicNo": "ABC1234567",
                "status": "Active"
            },
            {
                "slNo": 2,
                "partNo": "7",
                "partName": "Sample Part 2",
                "electorName": "Mary Smith",
                "relativeName": "Robert Smith",
                "houseNo": "456",
                "age": "42",
                "gender": "F",
                "epicNo": "XYZ9876543",
                "status": "Active"
            }
        ]
        
        fieldnames = list(sample_data[0].keys())
        
        try:
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for record in sample_data:
                    writer.writerow(record)
            
            print(f"Sample CSV created: {self.csv_file}")
            print("Note: This is sample data. No actual data was found in API response.")
            return True
            
        except Exception as e:
            print(f"Error creating sample CSV: {e}")
            return False
    
    def display_csv_preview(self, filename, num_rows=5):
        """Display preview of CSV file"""
        try:
            with open(filename, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                print("\nCSV Preview (first 5 rows):")
                print("-" * 80)
                for i, row in enumerate(reader):
                    if i < num_rows:
                        print(", ".join(row))
                    else:
                        break
                print("-" * 80)
        except Exception as e:
            print(f"Error reading CSV: {e}")

def main():
    """Main execution function"""
    
    # API Parameters
    params = {
        "partNo": "7",
        "stateCd": "S25",
        "acNo": "260",
        "pageNumber": "1",
        "pageLimit": "100",
        "isTotalCount": "Y",
        "categoryType": "na",
        "prefUserName": "a71782e5-0c9f-4216-ab8c-6f109acb8965",
        "partList": "210,10,166,212,9,161,7,122,164,88,89,208,8,165,121,119,86,211,209,163,206,207,214,123,160,213,167,87,215,216,217,162,168,120"
    }
    
    # API Headers
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "applicationName": "ERONET2.0",
        "PLATFORM-TYPE": "ECIWEB",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJDczJZLThBb3c2bEU4NW5xMnJuRE94bVpWTkRxYmpHUE5wLVNGdzQ3RjdzIn0.eyJleHAiOjE3Njc3MjA5MjAsImlhdCI6MTc2NzY3NzcyMCwianRpIjoiNDQ3Mjc3NmEtMDNlMC00YmJiLWEyOTAtZjBhMDc1MjZhN2M2IiwiaXNzIjoiaHR0cDovLzEwLjIxMC4xMTMuMjE6ODA4MC9yZWFsbXMvZWNpLXByb2QtcmVhbG0iLCJhdWQiOlsicmVhbG0tbWFuYWdlbWVudCIsImFjY291bnQiXSwic3ViIjoiNjNlYTk3YTAtOWY2MS00OTEyLTliZmYtZTA3MjAzNjk4MWIwIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYXV0aG4tY2xpZW50Iiwic2Vzc2lvbl9zdGF0ZSI6ImM1ZDRiYmU3LTNlOTctNDMyNi05OGNmLWQ5MTg4ZjcxMGNhZSIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiYWVybyIsImRlZmF1bHQtcm9sZXMtZWNpLXByb2QtcmVhbG0iXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZWFsbS1tYW5hZ2VtZW50Ijp7InJvbGVzIjpbImltcGVyc29uYXRpb24iXX0sImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoicHJvZmlsZSBlbWFpbCIsInNpZCI6ImM1ZDRiYmU3LTNlOTctNDMyNi05OGNmLWQ5MTg4ZjcxMGNhZSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiYXV0aG9yaXplZFN0YXRlcyI6WyJTMjUiXSwicm9sZUlkIjo5LCJhdXRob3JpemVkRGlzdHJpY3RzIjpbIlMyNTIwIl0sImVtYWlsSWQiOiJrYXVzaWtpY2hhdHRlcmplZUBnbWFpbC5jb20iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhNzE3ODJlNS0wYzlmLTQyMTYtYWI4Yy02ZjEwOWFjYjg5NjUiLCJhdXRob3JpemVkQWNzIjpbMjYwXSwiZ2l2ZW5fbmFtZSI6IkFybmFiIiwibG9naW5OYW1lIjoiQUVST1MyNUEyNjBOMiIsIm5hbWUiOiJBcm5hYiBDaGF0dGVyamVlIiwicGhvbmVfbnVtYmVyIjoiNzcxOTM3MTgxNCIsImZhbWlseV9uYW1lIjoiQ2hhdHRlcmplZSIsImF1dGhvcml6ZWRQYXJ0cyI6WzIxMCwxMCwxNjYsMjEyLDksMTYxLDcsMTIyLDE2NCw4OCw4OSwyMDgsOCwxNjUsMTIxLDExOSw4NiwyMTEsMjA5LDE2MywyMDYsMjA3LDIxNCwxMjMsMTYwLDIxMywxNjcsODcsMjE1LDIxNiwyMTcsMTYyLDE2OCwxMjBdfQ.XYRboWwWgb9uwOpO0Bkc4WZQqo43noPzyUn0rPBcELFwgDKVu8FMh61k8aCOjYGleguBanek_wnMLiXgUTWTv_9UAPS5u1o2Y16Pz6Ig4kg667nom0nk1zp9tiO0L3_Zk0dzkZqnwFpwBvTq3Dhk8DuVpubWJb5WpL1YhdgwJTMk1o_Pc2hS4CPsvKOsR94EmktWKRFhZOY000KdFGskYmNpuLQ53rcqVMFzNA39Qr0aDkQfldFZIRR1K8HCQlt93V_5ADyCMwUTj782_rcCgMLMvwK8somrNDrjikVJWOFxhSiEdawkyskjmr1UeHRPtN_c8nTdYhVE1StXmD72VA",  # Replace with actual token
        "currentRole": "aero",
        "state": "S25"
    }
    
    print("=" * 60)
    print("ECI API to CSV Converter")
    print("=" * 60)
    
    # Initialize converter
    converter = ECIToCSV()
    
    # Fetch data from API
    print("\nFetching data from ECI API...")
    api_data = converter.fetch_data(params, headers)
    
    if api_data:
        print(f"API Response Status: {api_data.get('statusCode', 'N/A')}")
        print(f"API Message: {api_data.get('message', 'No message')}")
        
        # Create CSV from API data
        success = converter.create_csv(api_data)
        
        if not success:
            print("\nCreating sample CSV file...")
            converter.generate_sample_csv()
    
    # Display CSV preview
    converter.display_csv_preview(converter.csv_file)
    
    print("\n" + "=" * 60)
    print("Script execution completed")
    print("=" * 60)

if __name__ == "__main__":
    main()

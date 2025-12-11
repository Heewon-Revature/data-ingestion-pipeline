"""
API Reader - Fetches data from REST endpoints.
"""

import time
import requests
import pandas as pd

'''
Fetches data from a REST API endpoint. Makes HTTP GET requests to the
specified URL, handling multiple pages of results. It includes error handling
to continue in case a request fails. It also normalizes column names to lower case
with underscores.

Args:
    url: API endpoint URL
    pages: number of pages to fetch
    delay: seconds to wait between requests

Returns: 
    A DataFrame containing the fetched records with normalized column names.
    If the requests to all pages fails then it will return an empty DataFrame.
'''
def fetch_data(url, pages=1, delay=1.0):
    all_data = []
    headers = {"User-Agent": "DataIngestionPipeline/1.0"}
    
    for page in range(1, pages + 1):
        page_url = f"{url}&page={page}"
        print(f"  Fetching page {page}/{pages}...")
        
        try:
            response = requests.get(page_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if "docs" in data:
                all_data.extend(data["docs"])
                
        except requests.exceptions.RequestException as e:
            print(f"  Warning: Failed to fetch page {page}: {e}")
            continue
        
        if page < pages:
            time.sleep(delay)
    
    df = pd.DataFrame(all_data)
    
    if df.empty:
        return df
    
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    
    return df

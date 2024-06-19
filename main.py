import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()
import os
from supabase import create_client
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import urllib.request
from bs4 import BeautifulSoup

try:
    try:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        supabase = create_client(url, key)
        
        proxy = os.environ.get("proxy")

        proxy_support = urllib.request.ProxyHandler({'http': proxy,'https': proxy})
        opener = urllib.request.build_opener(proxy_support)
    except Exception as e:
        raise Exception("Error occurred while building opener")
        
    page = opener.open("https://idx.co.id/en/listed-companies/newly-listed-stock-performance/").read()

    soup = BeautifulSoup(page, 'html.parser')

    table = soup.find('table')
    rows = table.select('tr:nth-of-type(n+3)')

    results = []
    for row in rows:
        # Perform operations on each row
        row_details = row.select('td:nth-of-type(n+2)')
        row_details = [row_detail.text.strip() for row_detail in row_details]
        row_details = [row_detail.replace('%', '').replace(',', '') for row_detail in row_details]
        row_details[3] = int(row_details[3]) * 1000
        idx_ipo_data = {
            "symbol": row_details[0]+".JK",
            "underwriter": row_details[10],
            'updated_on': pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        }
        
        try:
            response = supabase.table('idx_ipo_perf').upsert(idx_ipo_data).execute()
            if response.data:
                print(f"Upserted data for symbol: {row_details[0]}")
            else:
                print(f"Updated ESG Score still None: {row_details[0]}")
        except:
            print(f"Error upserting data for symbol: {row_details[0]}")
        results.append(row_details)
except Exception as e:
    print(f"An exception occured: {str(e)}")
    results = []

try:
    df = pd.DataFrame(results, columns=['Symbol', 'Company Name', 'Listing Date', 'ipo_raised_fund', 'ipo_price', '1d_price_movement_after_listed','7d_price_movement_after_listed','30d_price_movement_after_listed','180d_price_movement_after_listed','365d_price_movement_after_listed', 'underwriter'])
    df.to_csv('idx_newly_listed_stock_performance.csv', index=False)
except Exception as e:
    print(f"An exception occured when saving to csv: {str(e)}")

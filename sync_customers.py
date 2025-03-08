import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import gspread
from datetime import datetime, timedelta
from dateutil import parser

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
BATCH_SIZE = 500 

supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

def fetch_google_sheet():
    try:
        csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRCe57F8CKLdvpbA959NXTt7I85CIWGE50p8rHwnVk-hH4x1wrGyPvtDe0qrx8pUoPYiVzIF4weF3bK/pub?gid=25398211&single=true&output=csv"
        df = pd.read_csv(csv_url)
        df = df.where(pd.notna(df), None)
        print(f"Fetched {len(df)} records from Google Sheet")
        
        return df
    except Exception as e:
        print(f"Error fetching Google Sheet: {str(e)}")
        return pd.DataFrame()


def normalize_phone_number(phone_number):
    if pd.isna(phone_number) or phone_number is None:
        return None
    try:
        return str(int(float(phone_number))).strip()
    except (ValueError, TypeError):
        return None

def parse_timestamp(timestamp_str):
    if pd.isna(timestamp_str) or timestamp_str is None:
        return None
    try:
        parsed_date = parser.parse(timestamp_str.replace('\u00e2\u20ac\u0178', '').strip())
        return parsed_date.isoformat()
    except Exception as e:
        print(f"Error parsing timestamp '{timestamp_str}': {str(e)}")
        return None
def get_dynamic_threshold_timestamp():
    try:
        
        response = (
            supabase.table("sync_customer_logs")
            .select("timestamp")
            .gt("records_inserted", 0)
            .order("timestamp", desc=True)
            .limit(1)
            .execute()
        )
        
        if not response.data:
            print("No previous successful sync found, using default threshold")
        
        last_sync_time = datetime.fromisoformat(response.data[0]['timestamp'].replace('Z', ''))
        window_start = last_sync_time - timedelta(minutes=5)
        
        
        customer_response = (
            supabase.table("customers")
            .select("customer_created_date")
            .gte("created_at", window_start.isoformat())
            .lte("created_at", last_sync_time.isoformat())
            .order("customer_created_date", desc=False)  
            .limit(1)
            .execute()
        )
        
        if not customer_response.data:
            print(f"No customers found in the time window, using last sync time: {last_sync_time}")
            return last_sync_time
        
        threshold = datetime.fromisoformat(customer_response.data[0]['customer_created_date'].replace('Z', ''))
        print(f"Using dynamic threshold timestamp: {threshold}")
        return threshold
        
    except Exception as e:
        print(f"Error determining threshold timestamp: {str(e)}")
        return 


def sync_customers():
    df = fetch_google_sheet()
    if df.empty:
        print("No data to process")
        return 0
    
   
    processed_df = df.copy()
    
    
    processed_df['normalized_phone'] = processed_df['PHONE NUMBER '].apply(normalize_phone_number)
    
    
    if 'Timestamp' in processed_df.columns:
        processed_df['parsed_timestamp'] = processed_df['Timestamp'].apply(parse_timestamp)

    
    threshold_timestamp = get_dynamic_threshold_timestamp()

    
    if 'parsed_timestamp' in processed_df.columns:
        processed_df['parsed_timestamp'] = pd.to_datetime(processed_df['parsed_timestamp'])
        processed_df = processed_df[processed_df['parsed_timestamp'] > threshold_timestamp]
    
    processed_df = processed_df[processed_df['normalized_phone'].notna()]
    
    
    processed_df = processed_df.drop_duplicates(subset=['normalized_phone'])
    
    print(f"After removing duplicates: {len(processed_df)} unique customers")
    
    
    status_columns = [col for col in processed_df.columns if 'STATUS' in col]
    if len(status_columns) >= 3:
        second_status_column = status_columns[2]
        print(f"Using second status column: '{second_status_column}'")
    else:
        second_status_column = "STATUS " if "STATUS " in processed_df.columns else "STATUS"
        print(f"Only found one status column: '{second_status_column}'")
    
    
    total_records = len(processed_df)
    total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE  
    total_inserted = 0
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, total_records)
        
        batch_df = processed_df.iloc[start_idx:end_idx]
        phone_numbers = batch_df['normalized_phone'].tolist()
        
        print(f"\nProcessing batch {batch_num+1}/{total_batches} ({len(phone_numbers)} customers)")
        
        
        try:
            response = (
                supabase.table("customers")
                .select("phone")
                .in_("phone", phone_numbers)
                .execute()
            )
            
            existing_phones = {record['phone'] for record in response.data}
        except Exception as e:
            print(f"Error checking existing phone numbers: {str(e)}")
            continue
        
        
        records_to_insert = []
        for _, row in batch_df.iterrows():
            phone_number = row['normalized_phone']
            
            if phone_number in existing_phones:
                continue
            
            record = {
                "name": row.get("NAME "),  
                "phone": phone_number,
                "customer_type": row.get("TYPE") if "TYPE" in processed_df.columns else None,
                "category": row.get("REQUIRED ITEM /CATEGORY") if "REQUIRED ITEM /CATEGORY" in processed_df.columns else None,
                "status": row.get(second_status_column),  
                "customer_created_date":  row.get("parsed_timestamp").isoformat() if pd.notna(row.get("parsed_timestamp")) else None 
            }
            
            
            record = {k: None if pd.isna(v) else v for k, v in record.items()}
            
            
            for key, value in record.items():
                if isinstance(value, float) and value.is_integer():
                    record[key] = int(value)
            
            records_to_insert.append(record)
        
        
        if records_to_insert:
            try:
                supabase.table("customers").insert(records_to_insert).execute()
                total_inserted += len(records_to_insert)
                print(f"Inserted {len(records_to_insert)} new records")
            except Exception as e:
                print(f"Error inserting records: {str(e)}")
    
    
    try:
        supabase.table("sync_customer_logs").insert({"records_inserted": total_inserted}).execute()
        supabase.table("controls").update({"config_value": datetime.utcnow().isoformat()}).eq("config_key", "last_customer_synced_on").execute()
    except Exception as e:
        print(f"Error logging sync: {str(e)}")
    
    print(f"\nSync completed. Total customers inserted: {total_inserted}")
    return total_inserted

if __name__ == "__main__":
    sync_customers()
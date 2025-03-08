import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from dateutil import parser

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
BATCH_SIZE = 500 

supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

def fetch_google_sheet():
    csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQD947riEFWqrV9JGJrzTo8p6kPv8qK66g0obZb4n16U1T6HwmNRXWgfq-urz10VnigT2eM9Wcr83hF/pub?output=csv"
    df = pd.read_csv(csv_url)
    df = df.where(pd.notna(df), None)
    return df

def normalize_contact_number(contact_number):
    if pd.isna(contact_number) or contact_number is None:
        return None
    try:
        return str(int(float(contact_number))).strip()
    except (ValueError, TypeError):
        return None

def parse_timestamp(timestamp_str):
    if pd.isna(timestamp_str) or timestamp_str is None:
        return None
    
    try:
        cleaned_timestamp_str = timestamp_str.replace('\u00e2\u20ac\u0178', '').strip()
        parsed_date = parser.parse(cleaned_timestamp_str)
        return parsed_date.isoformat()
    except Exception as e:
        print(f"Error parsing timestamp '{timestamp_str}': {str(e)}")
        return None

def sync_leads():
    df = fetch_google_sheet()
    print(f"Fetched {len(df)} records from Google Sheet")
    processed_df = df.copy()
    
    processed_df['normalized_contact'] = processed_df['CONTACT NUMBER '].apply(normalize_contact_number)
    if 'Timestamp' in processed_df.columns:
        processed_df['parsed_timestamp'] = processed_df['Timestamp'].apply(parse_timestamp)
    
    processed_df = processed_df[processed_df['normalized_contact'].notna()]
    
    processed_df = processed_df.drop_duplicates(subset=['normalized_contact'])
    
    print(f"After removing duplicates: {len(processed_df)} unique contacts")
    total_records = len(processed_df)
    total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE 
    total_inserted = 0
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, total_records)
        
        batch_df = processed_df.iloc[start_idx:end_idx]
        contact_numbers = batch_df['normalized_contact'].tolist()
        
        print(f"\nProcessing batch {batch_num+1}/{total_batches} ({len(contact_numbers)} contacts)")
        
        try:
            response = (
                supabase.table("leads")
                .select("contact_number")
                .in_("contact_number", contact_numbers)
                .execute()
            )
            
            existing_contacts = {record['contact_number'] for record in response.data}
        except Exception as e:
            print(f"Error checking existing contacts: {str(e)}")
            continue
        
        records_to_insert = []
        for _, row in batch_df.iterrows():
            contact = row['normalized_contact']
            if contact not in existing_contacts:
                record = {
                    "email_address": row.get("Email Address"),
                    "name": row.get("Name"),
                    "contact_number": contact,
                    "item_category": row.get("ITEM CATEGORY "),
                    "enquiry_through": row.get("ENQUIRY THROUGH"),
                    "enquiry_status": row.get("ENQUIRY STATUS"),
                    "enquiry_passed_to": row.get("ENQUIRY PASSED TO "),
                    "reason_remarks_item_detail": row.get("REASON / REMARKS /ITEM DETAIL IF ANY"),
                    "amount_value": row.get("AMOUNT /VALUE "),
                    "customer_email": row.get("CUSTOMER EMAIL"),
                    "organization": row.get("ORGANIZATION"),
                    "status_update": row.get("STATUS UPDATE"),
                    "notes": row.get("NOTES "),
                    "follow_up_date": row.get("FOLLOW UP DATE "),
                    "reason_for_cancellation": row.get("REASON FOR CANCELLATION"),
                    "quote_number": row.get("QUOTE NUMBER"),
                    "enquiry_response": row.get("ENQ RESPONSE "),
                    "follow_up_message": row.get("FOLLOW UP MESSAGE "),
                    "feedback": row.get("FEED BACK"),
                    "enquiry_per_day": row.get("ENQUIRY PER DAY "),
                    "timestamp": row.get("parsed_timestamp"),  
                }
                
                record = {k: None if pd.isna(v) else v for k, v in record.items()}
                
                
                for key, value in record.items():
                    if isinstance(value, float) and value.is_integer():
                        record[key] = int(value)
                
                records_to_insert.append(record)
        
        if records_to_insert:
            try:
                supabase.table("leads").insert(records_to_insert).execute()
                total_inserted += len(records_to_insert)
                print(f"Inserted {len(records_to_insert)} new records")
            except Exception as e:
                print(f"Error inserting records: {str(e)}")
    
    try:
        supabase.table("sync_lead_logs").insert({"records_inserted": total_inserted}).execute()
        supabase.table("controls").update({"config_value": datetime.utcnow().isoformat()}).eq("config_key", "last_lead_synced_on").execute()
    except Exception as e:
        print(f"Error logging sync: {str(e)}")
    
    print(f"\nSync completed. Total leads inserted: {total_inserted}")
    return total_inserted

if __name__ == "__main__":
    sync_leads()
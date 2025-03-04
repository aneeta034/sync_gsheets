import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import gspread
from datetime import datetime

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")  

supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

def fetch_google_sheet():
    gc = gspread.service_account(filename="credentials.json")
    sh = gc.open_by_url(GOOGLE_SHEET_URL) 
    worksheet = sh.get_worksheet(0) 
    data = worksheet.get_all_records()  
    df = pd.DataFrame(data) 

    if "is_complete" in df.columns:
        df = df[df["is_complete"] == 1]
        
    return df

def get_created_by_id(name):
    if not name:
        return None 

    response = supabase.table("profiles").select("id").ilike("full_name", name).execute()
    
    return response.data[0]["id"] if response.data else None  
def insert_new_records(df):
    new_count = 0

    for _, row in df.iterrows():
        name = row.get("name")
        email = row.get("email")
        phone_number = row.get("phone")
        address = row.get("address")
        customer_type = row.get("customer_type")
        category = row.get("category")
        status = row.get("status")
        created_by_name = row.get("created_by")
        created_by_id = get_created_by_id(created_by_name) if created_by_name else None
        response = supabase.table("customers").select("phone").eq("phone", phone_number).execute()
        
        if not response.data: 
            supabase.table("customers").insert({
                "name": name,
                "email": email,
                "phone": phone_number,
                "address": address,
                "created_by": created_by_id,
                "customer_type": customer_type,
                "category": category,
                "status": status
            }).execute()
            new_count += 1


    supabase.table("sync_customer_logs").insert({"records_inserted": new_count}).execute()
    supabase.table("controls").update({"config_value": datetime.utcnow().isoformat()}).eq("config_key", "last_customer_synced_on").execute()

    print(f"Inserted {new_count} new records.")

if __name__ == "__main__":
    df = fetch_google_sheet()
    insert_new_records(df)

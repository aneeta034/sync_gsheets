import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import gspread
from datetime import datetime

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
GOOGLE_SHEET_URL = os.getenv("LEADS_GSHEETS_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

def fetch_google_sheet():
    gc = gspread.service_account(filename="leads_credentials.json")
    sh = gc.open_by_url(GOOGLE_SHEET_URL)
    worksheet = sh.get_worksheet(0)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    if "is_complete" in df.columns:
        df = df[df["is_complete"] == 1]

    return df

def contact_exists(contact_number):
    response = (
        supabase.table("leads")
        .select("contact_number")
        .eq("contact_number", contact_number)
        .execute()
    )
    return bool(response.data) 

def insert_new_records(df):
    new_records = []

    for _, row in df.iterrows():
        contact_number = row.get("CONTACT NUMBER")

        if contact_number and not contact_exists(contact_number):  
            new_records.append({
                "email_address": row.get("Email Address"),
                "name": row.get("Name"),
                "contact_number": contact_number,
                "item_category": row.get("ITEM CATEGORY"),
                "enquiry_through": row.get("ENQUIRY THROUGH"),
                "enquiry_status": row.get("ENQUIRY STATUS"),
                "enquiry_passed_to": row.get("ENQUIRY PASSED TO"),
                "reason_remarks_item_detail": row.get("REASON / REMARKS /ITEM DETAIL IF ANY"),
                "amount_value": row.get("AMOUNT /VALUE"),
                "customer_email": row.get("CUSTOMER EMAIL"),
                "organization": row.get("ORGANIZATION"),
                "status_update": row.get("STATUS UPDATE"),
                "notes": row.get("NOTES"),
                "follow_up_date": row.get("FOLLOW UP DATE"),
                "reason_for_cancellation": row.get("REASON FOR CANCELLATION"),
                "quote_number": row.get("QUOTE NUMBER"),
                "enquiry_response": row.get("ENQ RESPONSE"),
                "follow_up_message": row.get("FOLLOW UP MESSAGE"),
                "feedback": row.get("FEED BACK"),
                "enquiry_per_day": row.get("ENQUIRY PER DAY"),
            })

    records_inserted = len(new_records)

    if records_inserted > 0:
        supabase.table("leads").insert(new_records).execute()
        
    supabase.table("sync_lead_logs").insert({"records_inserted": records_inserted}).execute()
    
    supabase.table("controls").update({"config_value": datetime.utcnow().isoformat()}).eq("config_key", "last_lead_synced_on").execute()

    print(f"Inserted {records_inserted} new leads.")

if __name__ == "__main__":
    df = fetch_google_sheet()
    insert_new_records(df)
